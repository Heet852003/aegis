package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/Heet852003/aegis/internal/store"
)

// LeaderElector gates which process runs the claim loop, lease-reclaim loop,
// and cron ticker in a multi-node deployment. Exactly one node should be
// "leading" scheduling at a time; the rest keep serving reads (API/dashboard)
// so a crash fails over without a restart.
type LeaderElector interface {
	// Campaign blocks until ctx is cancelled, calling onElected each time
	// this process becomes leader and onDemoted if leadership is lost.
	Campaign(ctx context.Context, onElected, onDemoted func())
}

// SingleNodeElector always considers the current process leader. This is
// the correct choice for the embedded SQLite backend, which is single-writer
// and single-process by construction.
type SingleNodeElector struct{}

func (SingleNodeElector) Campaign(ctx context.Context, onElected, onDemoted func()) {
	onElected()
	<-ctx.Done()
	onDemoted()
}

// PostgresElector implements leader election with a Postgres session-level
// advisory lock (pg_try_advisory_lock). Advisory locks are cheap, need no
// extra schema, and are automatically released if the holding connection
// dies — which is exactly the failover behavior we want.
type PostgresElector struct {
	Store        *store.PostgresStore
	LockKey      int64
	PollInterval time.Duration
}

func (p PostgresElector) Campaign(ctx context.Context, onElected, onDemoted func()) {
	interval := p.PollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	leading := false
	for {
		acquired, err := p.Store.TryAcquireLeaderLock(ctx, p.LockKey)
		if err != nil {
			slog.Error("leader election check failed", "error", err)
		} else if acquired && !leading {
			leading = true
			slog.Info("acquired scheduler leadership")
			onElected()
		} else if !acquired && leading {
			leading = false
			slog.Warn("lost scheduler leadership")
			onDemoted()
		}

		select {
		case <-ctx.Done():
			if leading {
				p.Store.ReleaseLeaderLock(context.Background(), p.LockKey)
				onDemoted()
			}
			return
		case <-ticker.C:
		}
	}
}
