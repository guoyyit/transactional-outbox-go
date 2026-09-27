package outbox

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"sync/atomic"
	"time"
)

// Clock allows injecting time in tests.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the wall clock.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }

// IDGenerator generates globally unique message IDs.
type IDGenerator interface {
	NewID() (string, error)
}

// RandomIDGenerator generates 16 random bytes hex-encoded (128 bits, UUID-like).
type RandomIDGenerator struct{}

// NewID implements IDGenerator.
func (RandomIDGenerator) NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Backoff computes the delay before the next attempt. attempt is 1-based.
type Backoff interface {
	Next(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay each attempt up to Max, with jitter
// added by callers/tests as desired.
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
}

// Next implements Backoff.
func (b ExponentialBackoff) Next(attempt int) time.Duration {
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}
	d := float64(b.Base) * math.Pow(factor, float64(attempt-1))
	if d > float64(b.Max) || math.IsInf(d, 1) || d <= 0 {
		d = float64(b.Max)
	}
	return time.Duration(d)
}

// SequenceIDGenerator is a deterministic monotonic generator for tests.
type SequenceIDGenerator struct {
	n uint64
}

// NewID returns ids "msg-1", "msg-2", ...
func (g *SequenceIDGenerator) NewID() (string, error) {
	n := atomic.AddUint64(&g.n, 1)
	return "msg-" + uintToString(n), nil
}

func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
