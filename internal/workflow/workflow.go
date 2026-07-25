// Package workflow implements Aegis's DAG orchestration on top of the job
// engine: a Workflow is a set of named steps with dependencies between them.
// The Coordinator submits a step as a Job only once every step it depends on
// has succeeded, propagates failure by skipping downstream steps, and rolls
// per-step state up into an overall Workflow status — all driven reactively
// off the engine's event bus, so no polling is needed to advance a DAG.
package workflow

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/models"
)

type Coordinator struct {
	Engine *engine.Engine
	unsub  func()
}

func NewCoordinator(e *engine.Engine) *Coordinator {
	return &Coordinator{Engine: e}
}

// Start subscribes to job-update events and advances any workflow a
// completed/failed job belongs to. Runs until ctx is cancelled.
func (c *Coordinator) Start(ctx context.Context) {
	events, unsub := c.Engine.Bus.Subscribe()
	c.unsub = unsub
	go func() {
		defer unsub()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				if ev.Type != engine.EventJobUpdated {
					continue
				}
				job, ok := ev.Payload.(*models.Job)
				if !ok || job.WorkflowID == "" {
					continue
				}
				if job.Status != models.JobSucceeded && job.Status != models.JobDeadLetter {
					continue // still running / transient retry — nothing to advance yet
				}
				if err := c.onStepJobFinished(ctx, job); err != nil {
					slog.Error("workflow advance failed", "workflow_id", job.WorkflowID, "error", err)
				}
			}
		}
	}()
}

// Submit validates the DAG (unknown dependency names, cycles), persists the
// workflow and its steps, and immediately enqueues every step with no
// dependencies.
func (c *Coordinator) Submit(ctx context.Context, spec models.WorkflowSpec) (*models.Workflow, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	if len(spec.Steps) == 0 {
		return nil, fmt.Errorf("workflow must have at least one step")
	}
	if err := validateDAG(spec); err != nil {
		return nil, err
	}

	wf := &models.Workflow{ID: uuid.NewString(), Name: spec.Name, Status: models.WorkflowPending}
	steps := make([]*models.WorkflowStep, 0, len(spec.Steps))
	for _, s := range spec.Steps {
		// Every step starts Pending, including ones with no dependencies —
		// advance() is what promotes a step once its dependencies (trivially,
		// zero of them) are satisfied. Assigning StepReady here directly would
		// skip advance()'s promotion loop, which only scans Pending steps, and
		// the step would never actually be enqueued.
		maxAttempts := s.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 3
		}
		steps = append(steps, &models.WorkflowStep{
			ID:          uuid.NewString(),
			WorkflowID:  wf.ID,
			Name:        s.Name,
			Type:        s.Type,
			Payload:     s.Payload,
			DependsOn:   s.DependsOn,
			Status:      models.StepPending,
			MaxAttempts: maxAttempts,
		})
	}

	if err := c.Engine.Store.CreateWorkflow(ctx, wf, steps); err != nil {
		return nil, fmt.Errorf("create workflow: %w", err)
	}
	wf.Status = models.WorkflowRunning
	if err := c.Engine.Store.UpdateWorkflowStatus(ctx, wf.ID, models.WorkflowRunning); err != nil {
		return nil, err
	}

	if err := c.advance(ctx, wf.ID); err != nil {
		return nil, err
	}
	return wf, nil
}

func validateDAG(spec models.WorkflowSpec) error {
	names := make(map[string]bool, len(spec.Steps))
	for _, s := range spec.Steps {
		if names[s.Name] {
			return fmt.Errorf("duplicate step name %q", s.Name)
		}
		names[s.Name] = true
	}
	for _, s := range spec.Steps {
		for _, dep := range s.DependsOn {
			if !names[dep] {
				return fmt.Errorf("step %q depends on unknown step %q", s.Name, dep)
			}
			if dep == s.Name {
				return fmt.Errorf("step %q cannot depend on itself", s.Name)
			}
		}
	}

	// Kahn's algorithm for cycle detection.
	indegree := make(map[string]int, len(spec.Steps))
	adj := make(map[string][]string, len(spec.Steps))
	for _, s := range spec.Steps {
		indegree[s.Name] = len(s.DependsOn)
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.Name)
		}
	}
	var queue []string
	for name, deg := range indegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[n] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(spec.Steps) {
		return fmt.Errorf("workflow contains a dependency cycle")
	}
	return nil
}

// onStepJobFinished updates the step matching the finished job and
// re-advances the owning workflow.
func (c *Coordinator) onStepJobFinished(ctx context.Context, job *models.Job) error {
	wf, steps, err := c.Engine.Store.GetWorkflow(ctx, job.WorkflowID)
	if err != nil {
		return err
	}
	for _, st := range steps {
		if st.JobID != job.ID {
			continue
		}
		newStatus := models.StepSucceeded
		if job.Status == models.JobDeadLetter {
			newStatus = models.StepFailed
		}
		if err := c.Engine.Store.UpdateStepStatus(ctx, st.ID, newStatus, job.ID); err != nil {
			return err
		}
		c.Engine.Bus.Publish(engine.Event{Type: engine.EventWorkflowStep, Payload: st})
		break
	}
	return c.advance(ctx, wf.ID)
}

// advance recomputes step readiness: promotes Pending steps whose
// dependencies all succeeded to Queued (enqueuing a job for them), skips
// steps downstream of a failure, and rolls the aggregate status up onto the
// Workflow once every step reaches a terminal state.
func (c *Coordinator) advance(ctx context.Context, workflowID string) error {
	wf, steps, err := c.Engine.Store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}

	byName := make(map[string]*models.WorkflowStep, len(steps))
	for _, st := range steps {
		byName[st.Name] = st
	}

	changed := true
	for changed {
		changed = false
		for _, st := range steps {
			if st.Status != models.StepPending {
				continue
			}
			allSucceeded, anyBad := true, false
			for _, dep := range st.DependsOn {
				d := byName[dep]
				switch d.Status {
				case models.StepSucceeded:
					// satisfied
				case models.StepFailed, models.StepSkipped:
					anyBad = true
					allSucceeded = false
				default:
					allSucceeded = false
				}
			}
			if anyBad {
				st.Status = models.StepSkipped
				if err := c.Engine.Store.UpdateStepStatus(ctx, st.ID, models.StepSkipped, ""); err != nil {
					return err
				}
				c.Engine.Bus.Publish(engine.Event{Type: engine.EventWorkflowStep, Payload: st})
				changed = true
				continue
			}
			if allSucceeded {
				job, err := c.Engine.Enqueue(ctx, engine.SubmitJobInput{
					Type:         st.Type,
					Payload:      st.Payload,
					Queue:        "default",
					MaxAttempts:  st.MaxAttempts,
					WorkflowID:   workflowID,
					WorkflowStep: st.Name,
				})
				if err != nil {
					return err
				}
				st.Status = models.StepQueued
				st.JobID = job.ID
				if err := c.Engine.Store.UpdateStepStatus(ctx, st.ID, models.StepQueued, job.ID); err != nil {
					return err
				}
				c.Engine.Bus.Publish(engine.Event{Type: engine.EventWorkflowStep, Payload: st})
				changed = true
			}
		}
	}

	allTerminal, anyFailed := true, false
	for _, st := range steps {
		switch st.Status {
		case models.StepSucceeded:
		case models.StepFailed, models.StepSkipped:
			anyFailed = true
		default:
			allTerminal = false
		}
	}
	if !allTerminal {
		return nil
	}
	final := models.WorkflowSucceeded
	if anyFailed {
		final = models.WorkflowFailed
	}
	if wf.Status == final {
		return nil
	}
	return c.Engine.Store.UpdateWorkflowStatus(ctx, workflowID, final)
}

func (c *Coordinator) Get(ctx context.Context, id string) (*models.Workflow, []*models.WorkflowStep, error) {
	return c.Engine.Store.GetWorkflow(ctx, id)
}

func (c *Coordinator) List(ctx context.Context, limit, offset int) ([]*models.Workflow, error) {
	return c.Engine.Store.ListWorkflows(ctx, limit, offset)
}
