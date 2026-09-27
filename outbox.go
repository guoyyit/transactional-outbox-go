package outbox

import (
	"context"
	"errors"
	"time"
)

// Outbox is the public facade combining a Store, a Publisher and the
// background dispatcher.
type Outbox struct {
	store Store
	cfg   Config
	disp  *dispatcher
}

// New wires an Outbox. Call Start to begin delivery.
func New(store Store, pub Publisher, cfg Config) (*Outbox, error) {
	if store == nil {
		return nil, errors.New("outbox: store is required")
	}
	if pub == nil {
		return nil, errors.New("outbox: publisher is required")
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Outbox{store: store, cfg: cfg, disp: newDispatcher(store, pub, cfg)}, nil
}

// Write runs fn inside a local transaction: business writes via tx.Raw()
// and message inserts via tx.Enqueue commit or roll back atomically.
func (o *Outbox) Write(ctx context.Context, fn func(Tx) error) error {
	return o.store.RunInTx(ctx, fn)
}

// Start recovers messages left claimed by a previous (crashed) process and
// begins background delivery, retry, reclaim and cleanup loops.
func (o *Outbox) Start(ctx context.Context) error {
	if _, err := o.store.ResetClaimed(ctx); err != nil {
		return err
	}
	o.disp.Start(ctx)
	return nil
}

// Stop gracefully shuts the dispatcher down. Messages still claimed when
// the context expires are recovered by lease expiry or the next Start.
func (o *Outbox) Stop(ctx context.Context) error {
	return o.disp.Stop(ctx)
}

// DeadLetters lists dead-lettered messages, optionally filtered by stream
// (empty stream lists all), ordered by creation time.
func (o *Outbox) DeadLetters(ctx context.Context, stream string, limit, offset int) ([]*Message, error) {
	return o.store.DeadLetters(ctx, stream, limit, offset)
}

// Replay moves a dead message back to pending with a fresh attempt budget.
// The message keeps its original ID and stream position, so redelivery is
// deduplicated downstream by ID and preserves per-stream ordering.
func (o *Outbox) Replay(ctx context.Context, id string) error {
	return o.store.ReplayDead(ctx, id, time.Now())
}

// Cleanup deletes delivered messages older than the configured retention.
// Pending, claimed and dead messages are never touched.
func (o *Outbox) Cleanup(ctx context.Context) (int, error) {
	if o.cfg.Retention <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-o.cfg.Retention)
	total := 0
	for {
		n, err := o.store.DeleteDelivered(ctx, cutoff, o.cfg.BatchSize)
		total += n
		if err != nil || n < o.cfg.BatchSize {
			return total, err
		}
	}
}

// Stats returns store-wide counts.
func (o *Outbox) Stats(ctx context.Context) (Stats, error) {
	return o.store.Stats(ctx)
}

// StreamProgress returns per-stream delivery progress for tracing
// retry/cleanup state of individual business flows.
func (o *Outbox) StreamProgress(ctx context.Context, stream string) (StreamProgress, error) {
	return o.store.StreamProgress(ctx, stream)
}

// Config returns the effective configuration (with defaults applied).
func (o *Outbox) Config() Config { return o.cfg }
