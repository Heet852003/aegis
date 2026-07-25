// Package engine implements Aegis's scheduling core: enqueueing, atomic
// claiming with leases, retry with exponential backoff, dead-lettering,
// stale-lease reclamation, and cron-driven recurring jobs. It is storage-
// backend agnostic (works against any store.Store) and transport agnostic
// (the HTTP/WebSocket layer in internal/api drives it).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/Heet852003/aegis/internal/models"
	"github.com/Heet852003/aegis/internal/store"
)

// Config tunes engine behavior; zero values fall back to sane defaults via
// NewEngine.
type Config struct {
	LeaseDuration    time.Duration // how long a worker has to finish a claimed job before it's reclaimed
	ReclaimInterval  time.Duration // how often to scan for expired leases
	CronPollInterval time.Duration // how often to check cron schedules for due runs
	Backoff          BackoffPolicy
}

// Engine ties a Store to scheduling policy and an event Bus. All mutation
// methods (Enqueue, Claim, Complete, Fail) go through here rather than
// directly against the Store so that retry/backoff/dead-letter/eventing
// policy lives in one place regardless of backend.
type Engine struct {
	Store   store.Store
	Bus     *Bus
	cfg     Config
	elector LeaderElector

	cronRunning bool
	stopCron    context.CancelFunc
}

func NewEngine(s store.Store, bus *Bus, elector LeaderElector, cfg Config) *Engine {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.ReclaimInterval <= 0 {
		cfg.ReclaimInterval = 5 * time.Second
	}
	if cfg.CronPollInterval <= 0 {
		cfg.CronPollInterval = 1 * time.Second
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = DefaultBackoffPolicy()
	}
	return &Engine{Store: s, Bus: bus, cfg: cfg, elector: elector}
}

// Start launches the leader campaign, which in turn starts/stops the
// reclaim loop and cron loop as leadership is gained/lost. It blocks until
// ctx is cancelled, so callers should run it in a goroutine.
func (e *Engine) Start(ctx context.Context) {
	var loopCancel context.CancelFunc
	onElected := func() {
		var loopCtx context.Context
		loopCtx, loopCancel = context.WithCancel(ctx)
		go e.reclaimLoop(loopCtx)
		go e.cronLoop(loopCtx)
	}
	onDemoted := func() {
		if loopCancel != nil {
			loopCancel()
		}
	}
	e.elector.Campaign(ctx, onElected, onDemoted)
}

// --- Jobs ---

type SubmitJobInput struct {
	Type         string
	Payload      json.RawMessage
	Queue        string
	Priority     int
	MaxAttempts  int
	ScheduledAt  time.Time
	WorkflowID   string
	WorkflowStep string
}

func (e *Engine) Enqueue(ctx context.Context, in SubmitJobInput) (*models.Job, error) {
	j := &models.Job{
		ID:           uuid.NewString(),
		Type:         in.Type,
		Payload:      in.Payload,
		Status:       models.JobPending,
		Priority:     in.Priority,
		Queue:        in.Queue,
		MaxAttempts:  in.MaxAttempts,
		ScheduledAt:  in.ScheduledAt,
		WorkflowID:   in.WorkflowID,
		WorkflowStep: in.WorkflowStep,
	}
	if err := e.Store.EnqueueJob(ctx, j); err != nil {
		return nil, fmt.Errorf("enqueue job: %w", err)
	}
	e.Bus.Publish(Event{Type: EventJobEnqueued, Payload: j})
	e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
	return j, nil
}

// Claim hands up to `max` due jobs from the given queues to leaseOwner
// (typically a worker connection ID). Returns an empty slice, not an error,
// when there is simply no work available.
func (e *Engine) Claim(ctx context.Context, queues []string, leaseOwner string, max int) ([]*models.Job, error) {
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	jobs, err := e.Store.ClaimJobs(ctx, queues, leaseOwner, e.cfg.LeaseDuration, max)
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
	}
	return jobs, nil
}

func (e *Engine) Heartbeat(ctx context.Context, jobID, leaseOwner string) error {
	return e.Store.Heartbeat(ctx, jobID, leaseOwner, e.cfg.LeaseDuration)
}

func (e *Engine) Complete(ctx context.Context, jobID string, result json.RawMessage) error {
	if err := e.Store.CompleteJob(ctx, jobID, result); err != nil {
		return err
	}
	j, err := e.Store.GetJob(ctx, jobID)
	if err == nil {
		e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
	}
	return nil
}

// Fail records a job execution failure. If the job has attempts remaining
// it is rescheduled with exponential backoff; otherwise it moves to the
// dead-letter queue for manual inspection/requeue.
func (e *Engine) Fail(ctx context.Context, jobID string, errMsg string) error {
	j, err := e.Store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	deadLetter := j.Attempts >= j.MaxAttempts
	var retryAt *time.Time
	if !deadLetter {
		t := time.Now().UTC().Add(e.cfg.Backoff.Next(j.Attempts))
		retryAt = &t
	}
	if err := e.Store.FailJob(ctx, jobID, errMsg, retryAt, deadLetter); err != nil {
		return err
	}
	updated, err := e.Store.GetJob(ctx, jobID)
	if err == nil {
		e.Bus.Publish(Event{Type: EventJobUpdated, Payload: updated})
		if deadLetter {
			slog.Warn("job moved to dead letter", "job_id", jobID, "type", updated.Type, "error", errMsg)
		}
	}
	return nil
}

func (e *Engine) Cancel(ctx context.Context, jobID string) error {
	if err := e.Store.CancelJob(ctx, jobID); err != nil {
		return err
	}
	j, err := e.Store.GetJob(ctx, jobID)
	if err == nil {
		e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
	}
	return nil
}

func (e *Engine) RequeueDeadLetter(ctx context.Context, jobID string) error {
	if err := e.Store.RequeueJob(ctx, jobID); err != nil {
		return err
	}
	j, err := e.Store.GetJob(ctx, jobID)
	if err == nil {
		e.Bus.Publish(Event{Type: EventJobEnqueued, Payload: j})
		e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
	}
	return nil
}

func (e *Engine) GetJob(ctx context.Context, id string) (*models.Job, error) {
	return e.Store.GetJob(ctx, id)
}

func (e *Engine) ListJobs(ctx context.Context, f models.JobFilter) ([]*models.Job, error) {
	return e.Store.ListJobs(ctx, f)
}

// reclaimLoop periodically returns jobs whose worker lease expired (the
// worker crashed, was killed, or lost connectivity mid-job) back to pending
// so another worker can pick them up. This is what makes Aegis resilient to
// worker failure without any special crash-detection logic on the worker
// side — the server just notices silence.
func (e *Engine) reclaimLoop(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobs, err := e.Store.ReclaimExpiredLeases(ctx)
			if err != nil {
				slog.Error("reclaim expired leases failed", "error", err)
				continue
			}
			for _, j := range jobs {
				slog.Info("reclaimed job with expired lease", "job_id", j.ID, "type", j.Type)
				e.Bus.Publish(Event{Type: EventJobEnqueued, Payload: j})
				e.Bus.Publish(Event{Type: EventJobUpdated, Payload: j})
			}
		}
	}
}

// cronLoop scans enabled cron schedules once per CronPollInterval and
// enqueues a job for any schedule whose NextRun has passed, advancing
// NextRun via the standard cron parser.
func (e *Engine) cronLoop(ctx context.Context) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	ticker := time.NewTicker(e.cfg.CronPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			schedules, err := e.Store.ListCronSchedules(ctx)
			if err != nil {
				slog.Error("list cron schedules failed", "error", err)
				continue
			}
			now := time.Now().UTC()
			for _, cs := range schedules {
				if !cs.Enabled || now.Before(cs.NextRun) {
					continue
				}
				sched, err := parser.Parse(cs.Expression)
				if err != nil {
					slog.Error("invalid cron expression", "schedule", cs.Name, "expr", cs.Expression, "error", err)
					continue
				}
				if _, err := e.Enqueue(ctx, SubmitJobInput{
					Type:        cs.JobType,
					Payload:     cs.Payload,
					Queue:       cs.Queue,
					MaxAttempts: cs.MaxAttempts,
					ScheduledAt: now,
				}); err != nil {
					slog.Error("cron enqueue failed", "schedule", cs.Name, "error", err)
					continue
				}
				next := sched.Next(now)
				if err := e.Store.MarkCronFired(ctx, cs.ID, now, next); err != nil {
					slog.Error("mark cron fired failed", "schedule", cs.Name, "error", err)
				}
			}
		}
	}
}

// UpsertCron registers or updates a recurring job schedule, computing the
// initial NextRun from the cron expression if this is a new schedule.
func (e *Engine) UpsertCron(ctx context.Context, cs *models.CronSchedule) error {
	if cs.ID == "" {
		cs.ID = uuid.NewString()
	}
	if cs.NextRun.IsZero() {
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		sched, err := parser.Parse(cs.Expression)
		if err != nil {
			return fmt.Errorf("invalid cron expression %q: %w", cs.Expression, err)
		}
		cs.NextRun = sched.Next(time.Now().UTC())
	}
	return e.Store.UpsertCronSchedule(ctx, cs)
}

// --- Workers ---

func (e *Engine) RegisterWorker(ctx context.Context, w *models.Worker) error {
	if err := e.Store.UpsertWorker(ctx, w); err != nil {
		return err
	}
	e.Bus.Publish(Event{Type: EventWorkerUpdate, Payload: w})
	return nil
}

func (e *Engine) TouchWorker(ctx context.Context, id string, currentJobs []string) error {
	return e.Store.TouchWorker(ctx, id, currentJobs)
}

func (e *Engine) RemoveWorker(ctx context.Context, id string) error {
	if err := e.Store.RemoveWorker(ctx, id); err != nil {
		return err
	}
	e.Bus.Publish(Event{Type: EventWorkerUpdate, Payload: map[string]string{"id": id, "status": "disconnected"}})
	return nil
}

func (e *Engine) ListWorkers(ctx context.Context) ([]*models.Worker, error) {
	return e.Store.ListWorkers(ctx)
}

func (e *Engine) Stats(ctx context.Context) (*models.Stats, error) {
	return e.Store.Stats(ctx)
}
