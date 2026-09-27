package outbox

import "context"

// DLQ manages the independent dead-letter set: messages whose retries are
// exhausted. Dead messages are queryable and can be replayed individually or
// per stream. Replay only reschedules the original outbound message; business
// side effects stay committed once and are never re-executed.
type DLQ struct {
	store Store
	clock Clock
}

// NewDLQ builds a dead-letter manager.
func NewDLQ(store Store, clock Clock) *DLQ {
	if clock == nil {
		clock = SystemClock{}
	}
	return &DLQ{store: store, clock: clock}
}

// List returns dead messages for a stream (empty means every stream), newest
// first. Pagination is offset based; limit <= 0 means a sensible default.
func (q *DLQ) List(ctx context.Context, stream string, limit, offset int) ([]*Message, error) {
	if limit <= 0 {
		limit = 100
	}
	return q.store.ListDead(ctx, stream, limit, offset)
}

// Replay requeues one dead message by ID. Its attempt budget is reset and the
// relay will redeliver it in stream order behind any earlier messages.
func (q *DLQ) Replay(ctx context.Context, id string) error {
	return q.store.Replay(ctx, id, q.clock.Now())
}

// ReplayAll requeues all dead messages (or one stream's) and returns the count.
func (q *DLQ) ReplayAll(ctx context.Context, stream string) (int, error) {
	return q.store.ReplayAll(ctx, stream, q.clock.Now())
}
