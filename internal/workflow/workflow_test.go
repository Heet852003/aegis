package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/models"
	"github.com/Heet852003/aegis/internal/store"
)

func newTestCoordinator(t *testing.T) (*Coordinator, context.CancelFunc) {
	t.Helper()
	s, err := store.NewSQLite(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	eng := engine.NewEngine(s, engine.NewBus(), engine.SingleNodeElector{}, engine.Config{})
	c := NewCoordinator(eng)
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)
	return c, cancel
}

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

func TestValidateDAG_RejectsUnknownDependency(t *testing.T) {
	spec := models.WorkflowSpec{
		Name: "bad",
		Steps: []models.WorkflowStepSpec{
			{Name: "a", Type: "t", DependsOn: []string{"does-not-exist"}},
		},
	}
	if err := validateDAG(spec); err == nil {
		t.Fatal("expected an error for a dependency on an unknown step")
	}
}

func TestValidateDAG_RejectsCycle(t *testing.T) {
	spec := models.WorkflowSpec{
		Name: "cycle",
		Steps: []models.WorkflowStepSpec{
			{Name: "a", Type: "t", DependsOn: []string{"b"}},
			{Name: "b", Type: "t", DependsOn: []string{"a"}},
		},
	}
	if err := validateDAG(spec); err == nil {
		t.Fatal("expected an error for a dependency cycle")
	}
}

func TestValidateDAG_RejectsDuplicateStepNames(t *testing.T) {
	spec := models.WorkflowSpec{
		Name: "dup",
		Steps: []models.WorkflowStepSpec{
			{Name: "a", Type: "t"},
			{Name: "a", Type: "t"},
		},
	}
	if err := validateDAG(spec); err == nil {
		t.Fatal("expected an error for duplicate step names")
	}
}

func TestValidateDAG_AcceptsValidFanOutFanIn(t *testing.T) {
	spec := models.WorkflowSpec{
		Name: "ok",
		Steps: []models.WorkflowStepSpec{
			{Name: "extract", Type: "t"},
			{Name: "transform", Type: "t", DependsOn: []string{"extract"}},
			{Name: "validate", Type: "t", DependsOn: []string{"extract"}},
			{Name: "load", Type: "t", DependsOn: []string{"transform", "validate"}},
		},
	}
	if err := validateDAG(spec); err != nil {
		t.Fatalf("expected a valid fan-out/fan-in DAG to pass validation, got: %v", err)
	}
}

// TestSubmit_ZeroDependencyStepsGetEnqueued is a direct regression test for a
// bug where root steps (no dependencies) were created in a "ready" status
// that advance() never scanned — since advance() only promotes steps
// currently "pending" — so they were created but never actually turned into
// a job. This asserts the root step gets a JobID immediately on submit.
func TestSubmit_ZeroDependencyStepsGetEnqueued(t *testing.T) {
	c, cancel := newTestCoordinator(t)
	defer cancel()
	ctx := context.Background()

	wf, err := c.Submit(ctx, models.WorkflowSpec{
		Name:  "root-only",
		Steps: []models.WorkflowStepSpec{{Name: "solo", Type: "noop"}},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	_, steps, err := c.Get(ctx, wf.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].Status != models.StepQueued || steps[0].JobID == "" {
		t.Fatalf("expected the zero-dependency step to be queued with a job immediately, got status=%s job_id=%q",
			steps[0].Status, steps[0].JobID)
	}
}

// TestFanOutFanIn_LoadWaitsForBothBranches drives a diamond-shaped DAG
// (extract -> {transform, validate} -> load) by hand, claiming and
// completing jobs one at a time, to prove `load` never becomes claimable
// until *both* of its dependencies have succeeded — not just one.
func TestFanOutFanIn_LoadWaitsForBothBranches(t *testing.T) {
	c, cancel := newTestCoordinator(t)
	defer cancel()
	ctx := context.Background()

	wf, err := c.Submit(ctx, models.WorkflowSpec{
		Name: "diamond",
		Steps: []models.WorkflowStepSpec{
			{Name: "extract", Type: "extract"},
			{Name: "transform", Type: "transform", DependsOn: []string{"extract"}},
			{Name: "validate", Type: "validate", DependsOn: []string{"extract"}},
			{Name: "load", Type: "load", DependsOn: []string{"transform", "validate"}},
		},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// claimOne claims exactly one job (max=1), so fan-out branches can be
	// claimed and completed independently instead of arriving as a batch.
	claimOne := func() *models.Job {
		t.Helper()
		var job *models.Job
		waitFor(t, time.Second, func() bool {
			jobs, err := c.Engine.Claim(ctx, []string{"default"}, "test-worker", 1)
			if err != nil || len(jobs) == 0 {
				return false
			}
			job = jobs[0]
			return true
		})
		return job
	}
	claimAndComplete := func(expectType string) {
		t.Helper()
		job := claimOne()
		if job.Type != expectType {
			t.Fatalf("expected to claim a %q job, got %q", expectType, job.Type)
		}
		if err := c.Engine.Complete(ctx, job.ID, nil); err != nil {
			t.Fatalf("Complete(%s): %v", expectType, err)
		}
	}

	claimAndComplete("extract")

	// Both branches should now be independently claimable (fan-out): two
	// separate max=1 claims should surface transform and validate, in
	// whichever order the scheduler happens to return them — never load.
	first := claimOne()
	if first.Type != "transform" && first.Type != "validate" {
		t.Fatalf("expected transform or validate to be claimable after extract, got %q", first.Type)
	}
	second := claimOne()
	if second.Type == "load" {
		t.Fatal("load became claimable before both transform and validate succeeded")
	}
	if second.Type != "transform" && second.Type != "validate" {
		t.Fatalf("expected the other of transform/validate, got %q", second.Type)
	}

	if err := c.Engine.Complete(ctx, first.ID, nil); err != nil {
		t.Fatalf("Complete(%s): %v", first.Type, err)
	}

	// Only one of the two branches has succeeded so far — load must still
	// not be claimable.
	if jobs, err := c.Engine.Claim(ctx, []string{"default"}, "test-worker", 10); err != nil {
		t.Fatalf("Claim: %v", err)
	} else {
		for _, j := range jobs {
			if j.Type == "load" {
				t.Fatal("load became claimable before both transform and validate succeeded")
			}
		}
	}

	if err := c.Engine.Complete(ctx, second.ID, nil); err != nil {
		t.Fatalf("Complete(%s): %v", second.Type, err)
	}

	// Now that both branches are done, load must become claimable.
	claimAndComplete("load")

	waitFor(t, time.Second, func() bool {
		got, _, err := c.Get(ctx, wf.ID)
		return err == nil && got.Status == models.WorkflowSucceeded
	})
}
