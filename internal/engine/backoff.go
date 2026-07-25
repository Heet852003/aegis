package engine

import (
	"math/rand"
	"time"
)

// BackoffPolicy computes retry delays for failed jobs using exponential
// backoff with full jitter (per the AWS Architecture Blog's "Exponential
// Backoff And Jitter" guidance): delay = random(0, min(cap, base*2^attempt)).
// Full jitter avoids retry storms where every failed job in a batch retries
// at the exact same instant.
type BackoffPolicy struct {
	Base time.Duration
	Cap  time.Duration
}

func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{Base: 2 * time.Second, Cap: 5 * time.Minute}
}

// Next returns the delay before attempt number `attempt` (1-indexed) should run.
func (p BackoffPolicy) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := float64(p.Base) * float64(uint64(1)<<uint(min(attempt-1, 20)))
	if exp > float64(p.Cap) {
		exp = float64(p.Cap)
	}
	return time.Duration(rand.Float64() * exp)
}
