package engine

import (
	"testing"
	"time"
)

func TestBackoffPolicy_Next(t *testing.T) {
	p := BackoffPolicy{Base: 1 * time.Second, Cap: 10 * time.Second}

	cases := []struct {
		attempt int
		maxWant time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 10 * time.Second},  // would be 16s uncapped; must clamp to Cap
		{20, 10 * time.Second}, // large attempts must never exceed Cap
	}

	for _, c := range cases {
		for i := 0; i < 50; i++ { // jitter is random; sample repeatedly to catch bound violations
			got := p.Next(c.attempt)
			if got < 0 {
				t.Fatalf("attempt %d: negative delay %v", c.attempt, got)
			}
			if got > c.maxWant {
				t.Fatalf("attempt %d: delay %v exceeds max %v", c.attempt, got, c.maxWant)
			}
		}
	}
}

func TestBackoffPolicy_ZeroOrNegativeAttemptTreatedAsOne(t *testing.T) {
	p := DefaultBackoffPolicy()
	for _, a := range []int{0, -1, -100} {
		got := p.Next(a)
		if got > p.Base {
			t.Fatalf("attempt %d: expected delay bounded by Base=%v, got %v", a, p.Base, got)
		}
	}
}
