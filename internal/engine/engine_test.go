package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Heet852003/aegis/internal/models"
	"github.com/Heet852003/aegis/internal/store"
)

func newTestEngine(t *testing.T, cfg Config) (*Engine, store.Store) {
	t.Helper()
	s, err := store.NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return NewEngine(s, NewBus(), SingleNodeElector{}, cfg), s
}

// waitFor polls cond until it returns true or the timeout elapses, failing
// the test on timeout. Used instead of fixed sleeps so the suite stays fast
// and non-flaky regardless of machine speed.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %v", timeout)
	}
}

func TestEngine_EnqueueClaimComplete(t *testing.T) {
	eng, _ := newTestEngine(t, Config{})
	ctx := context.Background()

	job, err := eng.Enqueue(ctx, SubmitJobInput{Type: "noop", MaxAttempts: 3})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed, err := eng.Claim(ctx, []string{"default"}, "worker-1", 5)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != job.ID {
		t.Fatalf("expected to claim the enqueued job, got %+v", claimed)
	}

	if err := eng.Complete(ctx, job.ID, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got, err := eng.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != models.JobSucceeded {
		t.Fatalf("expected succeeded, got %s", got.Status)
	}
}

func TestEngine_FailRetriesThenDeadLetters(t *testing.T) {
	eng, _ := newTestEngine(t, Config{Backoff: BackoffPolicy{Base: time.Millisecond, Cap: 5 * time.Millisecond}})
	ctx := context.Background()

	job, err := eng.Enqueue(ctx, SubmitJobInput{Type: "flaky", MaxAttempts: 2})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Attempt 1: claim, fail — should be retried, not dead-lettered yet.
	claimed, err := eng.Claim(ctx, []string{"default"}, "worker-1", 5)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim (1st): jobs=%v err=%v", claimed, err)
	}
	if err := eng.Fail(ctx, job.ID, "boom"); err != nil {
		t.Fatalf("Fail (1st): %v", err)
	}
	got, _ := eng.GetJob(ctx, job.ID)
	if got.Status != models.JobPending {
		t.Fatalf("expected pending (scheduled for retry) after 1st failure, got %s", got.Status)
	}

	// Wait for the retry backoff to elapse, then claim attempt 2.
	waitFor(t, time.Second, func() bool {
		claimed, err = eng.Claim(ctx, []string{"default"}, "worker-1", 5)
		return err == nil && len(claimed) == 1
	})

	if err := eng.Fail(ctx, job.ID, "boom again"); err != nil {
		t.Fatalf("Fail (2nd): %v", err)
	}
	got, err = eng.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != models.JobDeadLetter {
		t.Fatalf("expected dead_letter after exhausting max_attempts=2, got %s (attempts=%d)", got.Status, got.Attempts)
	}
	if got.LastError != "boom again" {
		t.Fatalf("expected last_error to be preserved, got %q", got.LastError)
	}

	if err := eng.RequeueDeadLetter(ctx, job.ID); err != nil {
		t.Fatalf("RequeueDeadLetter: %v", err)
	}
	got, _ = eng.GetJob(ctx, job.ID)
	if got.Status != models.JobPending || got.Attempts != 0 {
		t.Fatalf("expected requeue to reset to pending/0 attempts, got status=%s attempts=%d", got.Status, got.Attempts)
	}
}

func TestEngine_CancelPreventsClaim(t *testing.T) {
	eng, _ := newTestEngine(t, Config{})
	ctx := context.Background()

	job, err := eng.Enqueue(ctx, SubmitJobInput{Type: "noop", MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := eng.Cancel(ctx, job.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	claimed, err := eng.Claim(ctx, []string{"default"}, "worker-1", 5)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected a cancelled job to never be claimed, got %+v", claimed)
	}
}

func TestEngine_UpsertCronComputesNextRun(t *testing.T) {
	eng, _ := newTestEngine(t, Config{})
	ctx := context.Background()

	cs := &models.CronSchedule{Name: "every-minute", Expression: "* * * * *", JobType: "tick", Enabled: true}
	if err := eng.UpsertCron(ctx, cs); err != nil {
		t.Fatalf("UpsertCron: %v", err)
	}
	if cs.ID == "" {
		t.Fatal("expected UpsertCron to assign an ID")
	}
	if !cs.NextRun.After(time.Now().UTC().Add(-time.Minute)) {
		t.Fatalf("expected NextRun to be computed close to now, got %v", cs.NextRun)
	}

	if err := eng.UpsertCron(ctx, &models.CronSchedule{Expression: "not a cron expr", JobType: "x"}); err == nil {
		t.Fatal("expected an error for an invalid cron expression")
	}
}
