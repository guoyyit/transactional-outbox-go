package outbox

import (
	"context"
	"time"
)

// Store is the persistence abstraction. All multi-statement operations happen
// inside Transaction so business data and outbox rows share one atomic commit.
type Store interface {
	// Transaction runs fn in a local transaction. Commit iff fn returns nil.
	Transaction(ctx context.Context, fn func(tx StoreTx) error) error

	// ClaimDue atomically leases up to batch due messages.
	//
	// Ordering rule: for each stream, messages are taken strictly in Seq order;
	// a stream is blocked at its oldest not-delivered message (pending, in-flight
	// whose lease is still valid, or dead). Expired leases are recovered and may
	// be re-claimed. Returns the claimed messages with status StatusInFlight and
	// LeaseUntil = now+lease.
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, batch int) ([]*Message, error)

	// MarkDelivered marks a leased message as delivered, but only if the caller
	// still owns the exact lease it was granted (lease token = LeaseUntil).
	// A stale worker after crash recovery gets ErrLeaseLost and its send is
	// discarded, preventing duplicate "delivered" transitions.
	MarkDelivered(ctx context.Context, id string, leaseExpiry, now time.Time) error

	// MarkRetry releases a leased message back to pending with the next attempt,
	// backoff deadline and error text; lease token must still match.
	MarkRetry(ctx context.Context, id string, leaseExpiry, now, availableAt time.Time, lastErr string) error

	// MarkDead moves a leased message to the dead-letter set; token must match.
	MarkDead(ctx context.Context, id string, leaseExpiry, now time.Time, lastErr string) error

	// Replay moves a dead message back to pending so it can be redelivered.
	Replay(ctx context.Context, id string, now time.Time) error

	// ReplayAll requeues every dead message for a stream (empty = all streams).
	ReplayAll(ctx context.Context, stream string, now time.Time) (int, error)

	// DeleteDelivered removes delivered messages older than before, excluding
	// messages that are pending/in-flight/dead. Returns rows deleted.
	DeleteDelivered(ctx context.Context, before time.Time, limit int) (int, error)

	// ListDead returns dead messages for a stream (empty = all), newest first.
	ListDead(ctx context.Context, stream string, limit, offset int) ([]*Message, error)

	// Get fetches a single message by ID.
	Get(ctx context.Context, id string) (*Message, error)

	// StreamStats returns per-stream counters.
	StreamStats(ctx context.Context) ([]StreamStat, error)

	// Size returns the total number of non-delivered (active) messages. Used to
	// enforce MaxOutboxSize backpressure.
	Size(ctx context.Context) (int, error)
}

// StoreTx is the transaction-scoped persistence API.
type StoreTx interface {
	// PutKV / DeleteKV mutate business data.
	PutKV(ctx context.Context, key string, value []byte) error
	DeleteKV(ctx context.Context, key string) error
	// InsertMessage stores a new outbox message with a stream-local monotonic Seq
	// assigned at commit time, preserving insertion order.
	InsertMessage(ctx context.Context, msg *Message) error
	// ActiveCount returns the current number of active messages (for admission).
	ActiveCount(ctx context.Context) (int, error)
}

// StreamStat reports per-stream delivery progress for observability.
type StreamStat struct {
	Stream    string
	Pending   int
	InFlight  int
	Delivered int
	Dead      int
	LastSeq   int64
}
