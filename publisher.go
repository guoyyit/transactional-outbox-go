package outbox

import (
	"context"
	"sync"
)

// Publisher delivers a message to the downstream system.
//
// Delivery is at-least-once: implementations must be prepared to see the
// same Message.ID more than once (retries, crash recovery, lease expiry).
// Message.ID is the idempotency key; Message.Stream and Message.Seq give
// the per-stream commit order.
type Publisher interface {
	Publish(ctx context.Context, msg *Message) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, msg *Message) error

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}

// DedupPublisher suppresses repeat publishes of the same message ID within
// this process, making an idempotent publisher out of a non-idempotent one.
// Downstream systems must still deduplicate by Message.ID across process
// restarts.
type DedupPublisher struct {
	inner Publisher

	mu   sync.Mutex
	seen map[string]struct{}
}

// NewDedupPublisher wraps inner with in-process idempotency.
func NewDedupPublisher(inner Publisher) *DedupPublisher {
	return &DedupPublisher{inner: inner, seen: map[string]struct{}{}}
}

// Publish implements Publisher.
func (d *DedupPublisher) Publish(ctx context.Context, msg *Message) error {
	d.mu.Lock()
	_, dup := d.seen[msg.ID]
	if !dup {
		d.seen[msg.ID] = struct{}{}
	}
	d.mu.Unlock()
	if dup {
		return nil
	}
	if err := d.inner.Publish(ctx, msg); err != nil {
		// Allow a later retry to attempt the publish again.
		d.mu.Lock()
		delete(d.seen, msg.ID)
		d.mu.Unlock()
		return err
	}
	return nil
}

// Forget drops the dedup record for id, e.g. after the message was cleaned
// up and will never be republished by this process.
func (d *DedupPublisher) Forget(id string) {
	d.mu.Lock()
	delete(d.seen, id)
	d.mu.Unlock()
}
