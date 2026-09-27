package outbox

import (
	"errors"
	"fmt"
	"time"
)

// Config tunes dispatcher behaviour. See README for the full semantics.
type Config struct {
	// InstanceID identifies the dispatcher instance claiming messages.
	// Defaults to "<hostname>-<pid>-<rand>".
	InstanceID string
	// PollInterval is how often the dispatcher looks for due messages. Default 100ms.
	PollInterval time.Duration
	// BatchSize caps messages claimed per poll cycle. Default 64.
	BatchSize int
	// Concurrency is the number of delivery workers. Default 8.
	Concurrency int
	// MaxInFlight caps claimed-but-unresolved messages process-wide,
	// bounding memory while the downstream is unavailable. Default 256.
	MaxInFlight int
	// LeaseDuration is how long a claimed message is owned before it may be
	// reclaimed. Must exceed PublishTimeout. Default 30s.
	LeaseDuration time.Duration
	// PublishTimeout bounds a single Publish call. Default 10s.
	PublishTimeout time.Duration
	// MaxAttempts is the number of delivery attempts before a message is
	// moved to the dead-letter set. Default 8.
	MaxAttempts int
	// BaseBackoff is the delay after the first failure. Default 200ms.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential retry delay. Default 30s.
	MaxBackoff time.Duration
	// BackoffJitter is the randomization factor in [0,1). Default 0.2.
	BackoffJitter float64
	// ReclaimInterval is how often expired claims are returned to pending. Default 5s.
	ReclaimInterval time.Duration
	// CleanupInterval is how often delivered messages are pruned. Default 1m.
	CleanupInterval time.Duration
	// Retention is how long delivered messages are kept. <=0 disables cleanup. Default 24h.
	Retention time.Duration
}

func (c Config) withDefaults() Config {
	if c.InstanceID == "" {
		c.InstanceID = defaultInstanceID()
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 100 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 64
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.MaxInFlight <= 0 {
		c.MaxInFlight = 256
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 200 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.BackoffJitter < 0 || c.BackoffJitter >= 1 {
		c.BackoffJitter = 0.2
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 5 * time.Second
	}
	if c.CleanupInterval <= 0 {
		c.CleanupInterval = time.Minute
	}
	if c.Retention == 0 {
		c.Retention = 24 * time.Hour
	}
	return c
}

func (c Config) validate() error {
	var errs []error
	if c.BatchSize < 1 {
		errs = append(errs, fmt.Errorf("BatchSize must be >= 1, got %d", c.BatchSize))
	}
	if c.Concurrency < 1 {
		errs = append(errs, fmt.Errorf("Concurrency must be >= 1, got %d", c.Concurrency))
	}
	if c.MaxInFlight < 1 {
		errs = append(errs, fmt.Errorf("MaxInFlight must be >= 1, got %d", c.MaxInFlight))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("MaxAttempts must be >= 1, got %d", c.MaxAttempts))
	}
	if c.LeaseDuration <= c.PublishTimeout {
		errs = append(errs, fmt.Errorf("LeaseDuration (%s) must exceed PublishTimeout (%s)", c.LeaseDuration, c.PublishTimeout))
	}
	return errors.Join(errs...)
}
