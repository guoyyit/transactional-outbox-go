package outbox

import (
	"context"
	"sync"
	"time"
)

// Cleaner periodically purges delivered messages older than the retention
// window. Safety properties:
//
//   - only StatusDelivered rows are eligible; pending, in-flight and dead rows
//     are always skipped, including expired-lease rows awaiting recovery,
//   - deletion is a store-level atomic predicate, so a row claimed between the
//     scan and the delete is not removed (no dangling delivery, no duplicate),
//   - purge batches are bounded to avoid locking/transaction blowups.
type Cleaner struct {
	store     Store
	retention time.Duration
	interval  time.Duration
	batch     int
	clock     Clock
	stop      chan struct{}
	stopped   chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
}

// NewCleaner builds a retention cleaner.
func NewCleaner(store Store, retention, interval time.Duration, clock Clock) *Cleaner {
	if clock == nil {
		clock = SystemClock{}
	}
	if retention <= 0 {
		retention = DefaultConfig().Retention
	}
	if interval <= 0 {
		interval = DefaultConfig().CleanupInterval
	}
	return &Cleaner{
		store:     store,
		retention: retention,
		interval:  interval,
		batch:     500,
		clock:     clock,
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

// Start launches the periodic purge loop.
func (c *Cleaner) Start(ctx context.Context) {
	c.once = sync.Once{}
	c.stop = make(chan struct{})
	c.stopped = make(chan struct{})
	c.wg.Add(1)
	go c.loop(ctx)
}

func (c *Cleaner) loop(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.stopped)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stop:
			return
		case <-ticker.C:
			// Drain the whole age-out window in bounded batches.
			for {
				n, err := c.PurgeOnce(ctx)
				if err != nil || n < c.batch {
					break
				}
			}
		}
	}
}

// Stop waits for the current purge to finish and exits.
func (c *Cleaner) Stop() {
	c.once.Do(func() { close(c.stop) })
	c.wg.Wait()
}

// PurgeOnce removes one batch of expired delivered rows; returns the count.
func (c *Cleaner) PurgeOnce(ctx context.Context) (int, error) {
	cutoff := c.clock.Now().Add(-c.retention)
	return c.store.DeleteDelivered(ctx, cutoff, c.batch)
}
