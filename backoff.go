package outbox

import (
	"math/rand/v2"
	"time"
)

// nextBackoff computes the delay before the given (1-based) retry attempt:
// base * 2^(attempt-1), capped at max, then randomized by +/- jitter.
func nextBackoff(base, max time.Duration, attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max || d <= 0 { // cap and overflow guard
			d = max
			break
		}
	}
	if d > max {
		d = max
	}
	if jitter > 0 {
		spread := int64(float64(d) * jitter)
		if spread > 0 {
			d += time.Duration(rand.Int64N(2*spread+1) - spread)
		}
	}
	if d < 0 {
		d = 0
	}
	return d
}
