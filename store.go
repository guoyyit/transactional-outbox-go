package outbox

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrOutboxFull is returned by Tx.Enqueue when the store's pending cap is reached.
	ErrOutboxFull = errors.New("outbox: pending message limit reached")
	// ErrNotFound is returned when a message ID does not exist.
	ErrNotFound = errors.New("outbox: message not found")
	// ErrNotDead is returned when replaying a message that is not dead.
	ErrNotDead = errors.New("outbox: message is not in dead state")
)

// Tx is a local transaction spanning business writes and outbox inserts.
type Tx interface {
	// Enqueue assigns the per-stream sequence number and stages the message.
	// On success msg.ID and msg.Seq are populated.
	Enqueue(ctx context.Context, msg *Message) error
	// Raw exposes the underlying transaction handle (e.g. *sql.Tx) so the
	// business can write its own tables in the same transaction.
	Raw() any
}

// Store persists outbox messages and mediates dispatcher access.
type Store interface {
	// RunInTx executes fn inside a local transaction, committing business
	// writes and enqueued messages atomically.
	RunInTx(ctx context.Context, fn func(Tx) error) error

	// ClaimDue atomically claims up to limit due stream-head messages for
	// owner, incrementing each message's attempt counter.
	ClaimDue(ctx context.Context, now time.Time, limit int, owner string, lease time.Duration) ([]*Message, error)
	// ClaimStreamHead atomically claims the next due head message of one
	// stream; returns (nil, nil) when the stream has nothing deliverable.
	ClaimStreamHead(ctx context.Context, stream, owner string, lease time.Duration, now time.Time) (*Message, error)
	// MarkDelivered finalizes a claimed message; reports whether the claim
	// was still owned by owner.
	MarkDelivered(ctx context.Context, id, owner string, at time.Time) (bool, error)
	// Reschedule returns a claimed message to pending for a later retry.
	Reschedule(ctx context.Context, id, owner string, nextRetryAt time.Time, lastErr string) error
	// MarkDead moves a claimed message to the dead-letter set.
	MarkDead(ctx context.Context, id, owner string, lastErr string) error

	// ReclaimExpired returns claims whose lease expired back to pending.
	ReclaimExpired(ctx context.Context, now time.Time) (int, error)
	// ResetClaimed returns every claimed message to pending; used once at
	// startup to recover messages owned by a dead process.
	ResetClaimed(ctx context.Context) (int, error)
	// DeleteDelivered removes up to limit delivered messages older than before.
	DeleteDelivered(ctx context.Context, before time.Time, limit int) (int, error)

	PendingCount(ctx context.Context) (int, error)
	DeadLetters(ctx context.Context, stream string, limit, offset int) ([]*Message, error)
	ReplayDead(ctx context.Context, id string, now time.Time) error
	StreamProgress(ctx context.Context, stream string) (StreamProgress, error)
	Stats(ctx context.Context) (Stats, error)

	Close() error
}
