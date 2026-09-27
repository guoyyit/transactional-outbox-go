package outbox

import (
	"context"
	"time"
)

// Publisher is the downstream transport. Implementations MUST treat calls with
// the same Message.ID as duplicates. A returned error triggers a retry (with
// backoff); a nil result means the message is durably accepted downstream.
type Publisher interface {
	Publish(ctx context.Context, msg *Message) error
}

// PublisherFunc adapts a function to the Publisher interface.
type PublisherFunc func(ctx context.Context, msg *Message) error

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}

// Config holds all tunables for the delivery relay and cleaner. Zero values
// are replaced by DefaultConfig in New; any field explicitly set is respected.
type Config struct {
	// PollInterval is how often the dispatcher scans for due messages.
	PollInterval time.Duration
	// Concurrency is the maximum number of messages delivered in parallel.
	Concurrency int
	// BatchSize is the maximum number of messages claimed per scan.
	BatchSize int
	// MaxInFlight is the global cap on leased/unfinished messages. It bounds the
	// amount of memory tied up by the relay regardless of backlog size.
	MaxInFlight int
	// LeaseDuration is how long a claim lasts before crash recovery reclaims it.
	LeaseDuration time.Duration
	// MaxAttempts is the attempt cap after which a message goes to the DLQ.
	MaxAttempts int
	// Backoff computes retry delays.
	Backoff Backoff
	// Retention is how long delivered messages are kept before cleanup.
	Retention time.Duration
	// CleanupInterval is how often delivered messages are purged.
	CleanupInterval time.Duration
	// MaxOutboxSize bounds the number of active (non-delivered) outbox rows.
	// When a transaction would exceed it, Run returns ErrOutboxFull and the
	// whole transaction (including business writes) is rejected, preventing
	// unbounded growth during prolonged downstream outages.
	MaxOutboxSize int
}

// DefaultConfig returns a Config with production-safe defaults.
func DefaultConfig() Config {
	return Config{
		PollInterval:    200 * time.Millisecond,
		Concurrency:     4,
		BatchSize:       32,
		MaxInFlight:     64,
		LeaseDuration:   30 * time.Second,
		MaxAttempts:     8,
		Backoff:         ExponentialBackoff{Base: 100 * time.Millisecond, Max: 30 * time.Second, Factor: 2},
		Retention:       24 * time.Hour,
		CleanupInterval: time.Minute,
		MaxOutboxSize:   100_000,
	}
}

// withDefaults fills zero-valued fields from DefaultConfig.
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.PollInterval <= 0 {
		c.PollInterval = d.PollInterval
	}
	if c.Concurrency <= 0 {
		c.Concurrency = d.Concurrency
	}
	if c.BatchSize <= 0 {
		c.BatchSize = d.BatchSize
	}
	if c.MaxInFlight <= 0 {
		c.MaxInFlight = d.MaxInFlight
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = d.LeaseDuration
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.Backoff == nil {
		c.Backoff = d.Backoff
	}
	if c.Retention <= 0 {
		c.Retention = d.Retention
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = d.CleanupInterval
	}
	if c.MaxOutboxSize <= 0 {
		c.MaxOutboxSize = d.MaxOutboxSize
	}
	return c
}
