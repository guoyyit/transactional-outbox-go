package outbox

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Dispatcher is the independent delivery relay: it repeatedly claims committed
// messages and publishes them. It provides:
//
//   - crash recovery: claims are leases; a crashed worker's messages are
//     reclaimed after LeaseDuration and redelivered,
//   - per-stream ordering: only the head message of a stream is ever claimed,
//   - at-least-once delivery with duplicate suppression via the stable Message.ID
//     and lease tokens,
//   - exponential-backoff retries and a DLQ after MaxAttempts,
//   - bounded concurrency and a global MaxInFlight cap on memory use.
type Dispatcher struct {
	store     Store
	publisher Publisher
	cfg       Config
	clock     Clock

	wg      sync.WaitGroup
	stop    chan struct{}
	stopped chan struct{}
	once    sync.Once

	// Semaphore limiting parallel Publish calls (Concurrency).
	sem chan struct{}

	mu       sync.Mutex
	inflight int // currently leased messages, capped at MaxInFlight
	cond     *sync.Cond
}

// NewDispatcher builds a relay from a store and publisher.
func NewDispatcher(store Store, publisher Publisher, cfg Config, clock Clock) *Dispatcher {
	if clock == nil {
		clock = SystemClock{}
	}
	cfg = cfg.withDefaults()
	d := &Dispatcher{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
		clock:     clock,
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
		sem:       make(chan struct{}, cfg.Concurrency),
	}
	d.cond = sync.NewCond(&d.mu)
	return d
}

// Start launches the poll loop.
func (d *Dispatcher) Start(ctx context.Context) {
	d.once = sync.Once{}
	d.stop = make(chan struct{})
	d.stopped = make(chan struct{})
	d.wg.Add(1)
	go d.loop(ctx)
}

func (d *Dispatcher) loop(ctx context.Context) {
	defer d.wg.Done()
	defer close(d.stopped)
	t := d.clock.Now()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case <-timer.C:
			d.tick(ctx, t)
			t = t.Add(d.cfg.PollInterval)
			// Avoid timer drift: if a tick overran, run again promptly.
			if d.clock.Now().After(t) {
				t = d.clock.Now()
			}
			timer.Reset(d.cfg.PollInterval)
		}
	}
}

// Stop signals shutdown and waits for in-flight deliveries to finish.
func (d *Dispatcher) Stop() {
	d.once.Do(func() { close(d.stop) })
	d.wg.Wait()
}

// InFlight returns the number of currently leased messages.
func (d *Dispatcher) InFlight() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inflight
}

// RunOnce performs a single claim+deliver cycle at the clock's current time.
// It is mainly useful for deterministic tests; production code uses Start.
func (d *Dispatcher) RunOnce(ctx context.Context) {
	d.tick(ctx, d.clock.Now())
}

// WaitIdle blocks until every leased delivery has recorded its outcome.
func (d *Dispatcher) WaitIdle() {
	d.mu.Lock()
	for d.inflight != 0 {
		d.cond.Wait()
	}
	d.mu.Unlock()
}

// tick claims due messages up to the remaining in-flight budget and launches a
// bounded delivery goroutine for each. It never blocks the loop: when the
// publisher concurrency pool is full, workers simply queue on the semaphore.
func (d *Dispatcher) tick(ctx context.Context, now time.Time) {
	d.mu.Lock()
	free := d.cfg.MaxInFlight - d.inflight
	d.mu.Unlock()
	if free <= 0 {
		return
	}
	batch := d.cfg.BatchSize
	if batch > free {
		batch = free
	}
	msgs, err := d.store.ClaimDue(ctx, now, d.cfg.LeaseDuration, batch)
	if err != nil || len(msgs) == 0 {
		return
	}
	d.mu.Lock()
	d.inflight += len(msgs)
	d.mu.Unlock()
	for _, m := range msgs {
		m := m
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			select {
			case d.sem <- struct{}{}:
			case <-ctx.Done():
				d.releaseOne()
				return
			case <-d.stop:
				d.releaseOne()
				return
			}
			d.deliver(ctx, m)
			<-d.sem
		}()
	}
}

func (d *Dispatcher) releaseOne() {
	d.mu.Lock()
	d.inflight--
	if d.inflight == 0 {
		d.cond.Broadcast()
	}
	d.mu.Unlock()
}

// deliver publishes one leased message and applies exactly one terminal
// outcome (delivered / retry / dead), guarded by the lease token.
func (d *Dispatcher) deliver(ctx context.Context, msg *Message) {
	defer d.releaseOne()
	leaseExpiry := msg.LeaseUntil
	now := d.clock.Now()

	pctx, cancel := context.WithTimeout(ctx, d.cfg.LeaseDuration)
	defer cancel()
	err := d.publisher.Publish(pctx, msg)

	now = d.clock.Now()
	if err == nil {
		// If the lease was lost while publishing (crash recovery already handed
		// this row to another worker), our success must not overwrite their
		// outcome. The downstream call was idempotent, so dropping the ack here
		// is safe and prevents duplicate terminal transitions.
		if mErr := d.store.MarkDelivered(ctx, msg.ID, leaseExpiry, now); mErr != nil {
			if errors.Is(mErr, ErrLeaseLost) {
				return
			}
			// Treat persistence failure like a failed attempt.
			_ = d.store.MarkRetry(ctx, msg.ID, leaseExpiry, now, d.nextTime(now, msg.Attempt), mErr.Error())
		}
		return
	}
	if errors.Is(err, context.Canceled) {
		// Worker shutdown, not a downstream failure: keep attempts unchanged and
		// let lease expiry recover the row.
		return
	}
	// Attempt on the message is the count after this claim.
	if msg.Attempt >= d.cfg.MaxAttempts {
		if dErr := d.store.MarkDead(ctx, msg.ID, leaseExpiry, now, err.Error()); dErr != nil && !errors.Is(dErr, ErrLeaseLost) {
			_ = d.store.MarkRetry(ctx, msg.ID, leaseExpiry, now, d.nextTime(now, msg.Attempt), dErr.Error())
		}
		return
	}
	avail := d.nextTime(now, msg.Attempt)
	if rErr := d.store.MarkRetry(ctx, msg.ID, leaseExpiry, now, avail, err.Error()); rErr != nil {
		// Lease lost: another worker owns the row; nothing to do.
		_ = rErr
	}
}

func (d *Dispatcher) nextTime(now time.Time, attempt int) time.Time {
	return now.Add(d.cfg.Backoff.Next(attempt))
}
