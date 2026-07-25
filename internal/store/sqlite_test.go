package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Heet852003/aegis/internal/models"
)

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Regression tests for a real bug found while wiring up the dashboard: Go's
// encoding/json marshals a nil slice as `null`, not `[]`. Every List* method
// used to declare its result with `var out []*T`, which is nil until
// something is appended — so an empty result crashed the frontend's
// `.map()` calls. These tests pin each List method to always return a
// non-nil (possibly empty) slice.

func TestSQLiteStore_ListJobs_EmptyIsNotNil(t *testing.T) {
	s := newTestStore(t)
	out, err := s.ListJobs(context.Background(), models.JobFilter{})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if out == nil {
		t.Fatal("ListJobs returned nil slice for an empty result; must return an empty non-nil slice")
	}
}

func TestSQLiteStore_ListWorkflows_EmptyIsNotNil(t *testing.T) {
	s := newTestStore(t)
	out, err := s.ListWorkflows(context.Background(), 50, 0)
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if out == nil {
		t.Fatal("ListWorkflows returned nil slice for an empty result; must return an empty non-nil slice")
	}
}

func TestSQLiteStore_ListWorkers_EmptyIsNotNil(t *testing.T) {
	s := newTestStore(t)
	out, err := s.ListWorkers(context.Background())
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if out == nil {
		t.Fatal("ListWorkers returned nil slice for an empty result; must return an empty non-nil slice")
	}
}

func TestSQLiteStore_ListCronSchedules_EmptyIsNotNil(t *testing.T) {
	s := newTestStore(t)
	out, err := s.ListCronSchedules(context.Background())
	if err != nil {
		t.Fatalf("ListCronSchedules: %v", err)
	}
	if out == nil {
		t.Fatal("ListCronSchedules returned nil slice for an empty result; must return an empty non-nil slice")
	}
}

func TestSQLiteStore_GetWorkflow_StepsEmptyIsNotNil(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	wf := &models.Workflow{ID: uuid.NewString(), Name: "empty-wf", Status: models.WorkflowPending}
	if err := s.CreateWorkflow(ctx, wf, nil); err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	_, steps, err := s.GetWorkflow(ctx, wf.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if steps == nil {
		t.Fatal("GetWorkflow returned nil steps slice for a workflow with no steps")
	}
}

func TestSQLiteStore_EnqueueClaimComplete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job := &models.Job{ID: uuid.NewString(), Type: "noop", Queue: "default", MaxAttempts: 3}
	if err := s.EnqueueJob(ctx, job); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}

	claimed, err := s.ClaimJobs(ctx, []string{"default"}, "worker-1", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != job.ID {
		t.Fatalf("expected to claim exactly the enqueued job, got %+v", claimed)
	}
	if claimed[0].Status != models.JobRunning {
		t.Fatalf("expected status running after claim, got %s", claimed[0].Status)
	}
	if claimed[0].Attempts != 1 {
		t.Fatalf("expected attempts=1 after first claim, got %d", claimed[0].Attempts)
	}

	// A second claim attempt must not re-claim the same (now running) job.
	again, err := s.ClaimJobs(ctx, []string{"default"}, "worker-2", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimJobs (second): %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("expected no jobs available for a second claimant, got %d", len(again))
	}

	if err := s.CompleteJob(ctx, job.ID, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	got, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != models.JobSucceeded {
		t.Fatalf("expected succeeded, got %s", got.Status)
	}
}

func TestSQLiteStore_ReclaimExpiredLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job := &models.Job{ID: uuid.NewString(), Type: "slow", Queue: "default", MaxAttempts: 3}
	if err := s.EnqueueJob(ctx, job); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	if _, err := s.ClaimJobs(ctx, []string{"default"}, "worker-1", 10*time.Millisecond, 10); err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}

	time.Sleep(30 * time.Millisecond) // let the lease expire

	reclaimed, err := s.ReclaimExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReclaimExpiredLeases: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != job.ID {
		t.Fatalf("expected the expired job to be reclaimed, got %+v", reclaimed)
	}
	if reclaimed[0].Status != models.JobPending {
		t.Fatalf("expected reclaimed job back to pending, got %s", reclaimed[0].Status)
	}

	// Now it must be claimable again.
	claimed, err := s.ClaimJobs(ctx, []string{"default"}, "worker-2", time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimJobs after reclaim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected the reclaimed job to be claimable again, got %d jobs", len(claimed))
	}
}

func TestSQLiteStore_ClaimRespectsPriorityThenFIFO(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	low := &models.Job{ID: uuid.NewString(), Type: "t", Queue: "default", Priority: 0, MaxAttempts: 1}
	high := &models.Job{ID: uuid.NewString(), Type: "t", Queue: "default", Priority: 10, MaxAttempts: 1}
	if err := s.EnqueueJob(ctx, low); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, high); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimJobs(ctx, []string{"default"}, "worker-1", time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != high.ID {
		t.Fatalf("expected the higher-priority job to be claimed first, got %+v", claimed)
	}
}
