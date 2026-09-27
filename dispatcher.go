package outbox

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// dispatcher polls the store and drives delivery, retry, reclaim and
// cleanup loops. Delivery is at-least-once: a message is only marked
// delivered after Publish succeeds, and claimed-but-unresolved messages are
// recovered via lease expiry or startup reset.
type dispatcher struct {
	store Store
	pub   Publisher
	cfg   Config

	sem       chan struct{} // in-flight semaphore, capacity MaxInFlight
	workerSem chan struct{} // delivery-worker semaphore, capacity Concurrency

	claimSeq atomic.Uint64

	mu      sync.Mutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// nextClaimToken returns a unique owner token for one claim operation, so
// that finalizing a claim only succeeds for the worker that made it — even
// after the message was reclaimed and re-claimed by the same process.
func (d *dispatcher) nextClaimToken() string {
	return fmt.Sprintf("%s-%d", d.cfg.InstanceID, d.claimSeq.Add(1))
}

func newDispatcher(store Store, pub Publisher, cfg Config) *dispatcher {
	return &dispatcher{
		store:     store,
		pub:       pub,
		cfg:       cfg,
		sem:       make(chan struct{}, cfg.MaxInFlight),
		workerSem: make(chan struct{}, cfg.Concurrency),
	}
}

// Start launches background loops; it returns immediately and is
// idempotent.
func (d *dispatcher) Start(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started {
		return
	}
	d.started = true
	ctx, d.cancel = context.WithCancel(ctx)

	d.wg.Add(1)
	go d.pollLoop(ctx)
	d.wg.Add(1)
	go d.reclaimLoop(ctx)
	if d.cfg.Retention > 0 {
		d.wg.Add(1)
		go d.cleanupLoop(ctx)
	}
}

// Stop signals all loops to exit and waits for in-flight deliveries,
// bounded by ctx.
func (d *dispatcher) Stop(ctx context.Context) error {
	d.mu.Lock()
	cancel := d.cancel
	d.started = false
	d.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pollLoop claims due stream heads and dispatches delivery workers.
func (d *dispatcher) pollLoop(ctx context.Context) {
	defer d.wg.Done()
	d.pollOnce(ctx)
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.pollOnce(ctx)
		}
	}
}

func (d *dispatcher) pollOnce(ctx context.Context) {
	available := cap(d.sem) - len(d.sem)
	if workers := cap(d.workerSem) - len(d.workerSem); workers < available {
		available = workers
	}
	if available <= 0 {
		return
	}
	limit := d.cfg.BatchSize
	if available < limit {
		limit = available
	}
	msgs, err := d.store.ClaimDue(ctx, time.Now(), limit, d.nextClaimToken(), d.cfg.LeaseDuration)
	if err != nil {
		return // transient; next tick retries
	}
	for _, msg := range msgs {
		// Guaranteed non-blocking: claims were capped by availability.
		d.sem <- struct{}{}
		d.workerSem <- struct{}{}
		d.wg.Add(1)
		go d.deliverChain(ctx, msg)
	}
}

// deliverChain publishes msg and, on success, immediately chains to the
// next head of the same stream so per-stream throughput is not limited by
// the poll interval. Ordering is preserved because only the head message of
// a stream is ever claimable.
func (d *dispatcher) deliverChain(ctx context.Context, msg *Message) {
	defer d.wg.Done()
	defer func() { <-d.sem; <-d.workerSem }()
	for msg != nil {
		if ctx.Err() != nil {
			return // shutting down; claim expires or is reset at next start
		}
		if !d.deliverOne(ctx, msg) {
			return
		}
		next, err := d.store.ClaimStreamHead(ctx, msg.Stream, d.nextClaimToken(), d.cfg.LeaseDuration, time.Now())
		if err != nil {
			return // transient; poll loop picks the stream up later
		}
		msg = next
	}
}

// deliverOne attempts a single publish and resolves the claim accordingly.
// It reports whether the message was delivered and the chain may continue.
func (d *dispatcher) deliverOne(ctx context.Context, msg *Message) bool {
	pctx, cancel := context.WithTimeout(ctx, d.cfg.PublishTimeout)
	err := d.pub.Publish(pctx, msg)
	cancel()

	if err == nil {
		ok, merr := d.store.MarkDelivered(ctx, msg.ID, msg.ClaimOwner, time.Now())
		if merr != nil || !ok {
			// Lost the lease or the store failed: the message may be
			// redelivered; downstream deduplicates by Message.ID.
			return false
		}
		return true
	}
	if ctx.Err() != nil {
		return false // shutting down; leave the claim to expire
	}
	if msg.Attempts >= d.cfg.MaxAttempts {
		_ = d.store.MarkDead(context.Background(), msg.ID, msg.ClaimOwner, err.Error())
		return false
	}
	delay := nextBackoff(d.cfg.BaseBackoff, d.cfg.MaxBackoff, msg.Attempts, d.cfg.BackoffJitter)
	_ = d.store.Reschedule(context.Background(), msg.ID, msg.ClaimOwner, time.Now().Add(delay), err.Error())
	return false
}

// reclaimLoop returns messages with expired leases to pending, covering
// workers that died mid-delivery.
func (d *dispatcher) reclaimLoop(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = d.store.ReclaimExpired(ctx, time.Now())
		}
	}
}

// cleanupLoop deletes delivered messages past the retention window in
// bounded batches. It never touches pending, claimed or dead messages.
func (d *dispatcher) cleanupLoop(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.cleanupOnce(ctx)
		}
	}
}

func (d *dispatcher) cleanupOnce(ctx context.Context) {
	cutoff := time.Now().Add(-d.cfg.Retention)
	for range 10 { // bound work per tick
		n, err := d.store.DeleteDelivered(ctx, cutoff, d.cfg.BatchSize)
		if err != nil || n < d.cfg.BatchSize {
			return
		}
	}
}
