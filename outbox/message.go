// Package outbox provides a transactional outbox: business data changes and
// outbox messages are committed atomically in a single local transaction,
// while a separate relay delivers committed messages to downstream systems.
package outbox

import (
	"context"
	"time"
)

// Status is the delivery state of an outbox message.
type Status int

const (
	// StatusPending means the message is committed and waiting for delivery.
	StatusPending Status = iota
	// StatusInFlight means the message has been leased by a dispatcher.
	StatusInFlight
	// StatusDelivered means the downstream acknowledged the message.
	StatusDelivered
	// StatusDead means the message exhausted all retries and entered the DLQ.
	StatusDead
)

// Message is a single outbox record.
type Message struct {
	// ID is the globally unique, idempotency key of the message.
	// It is generated on Enqueue and must never change.
	ID string
	// Stream identifies a business stream. Ordering is guaranteed per stream.
	Stream string
	// Type is the downstream message type / topic.
	Type string
	// Payload is the opaque message body.
	Payload []byte
	// Metadata holds optional trace/tenant information.
	Metadata map[string]string
	// Seq is the monotonically increasing commit order within a stream.
	Seq int64
	// Status is the current delivery status.
	Status Status
	// Attempt is the number of delivery attempts already made.
	Attempt int
	// AvailableAt is when the message becomes eligible for (re)delivery.
	AvailableAt time.Time
	// LeaseUntil is the current delivery lease expiry (0 if not leased).
	LeaseUntil time.Time
	// LastError is the error from the most recent failed attempt.
	LastError string
	// CreatedAt is the transaction commit time.
	CreatedAt time.Time
	// UpdatedAt is the last status transition time.
	UpdatedAt time.Time
}

// Outbox is the entry point for business writers. Messages enqueued inside a
// transaction are committed and rolled back atomically with the business
// writes made in the same transaction.
type Outbox interface {
	// Run executes fn inside a local transaction. Business writes (Put/Delete)
	// and Enqueue calls are all part of the same atomic unit.
	Run(ctx context.Context, fn func(tx Tx) error) error
}

// Tx is a local transaction.
type Tx interface {
	// Enqueue stages a message into the outbox inside this transaction.
	// Returns the assigned message ID.
	Enqueue(ctx context.Context, stream, msgType string, payload []byte, opts ...EnqueueOption) (string, error)
	// Put upserts a business key/value pair in the same transaction.
	Put(ctx context.Context, key string, value []byte) error
	// Delete removes a business key/value pair in the same transaction.
	Delete(ctx context.Context, key string) error
}

// EnqueueOption customizes an enqueued message.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	id       string
	metadata map[string]string
}
