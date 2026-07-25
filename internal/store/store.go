// Package store defines the persistence interface Aegis's engine depends on,
// plus concrete SQLite and PostgreSQL implementations.
//
// The engine talks only to the Store interface. Job claiming is expressed
// as a single atomic operation per backend (SELECT ... FOR UPDATE SKIP LOCKED
// on Postgres; a serialized single-writer transaction on SQLite) so that two
// workers can never be handed the same job, and so the two backends can be
// swapped without touching engine logic. SQLite is the zero-dependency
// default for local dev and small deployments; Postgres is recommended for
// multi-node / HA deployments since it also backs leader election.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/Heet852003/aegis/internal/models"
)

// ErrNotFound is returned when a lookup by ID finds nothing.
var ErrNotFound = errors.New("store: not found")

// Store is the persistence contract used by the scheduling engine, the
// workflow engine, and the API layer.
type Store interface {
	Migrate(ctx context.Context) error
	Close() error

	// Jobs
	EnqueueJob(ctx context.Context, job *models.Job) error
	ClaimJobs(ctx context.Context, queues []string, leaseOwner string, leaseDuration time.Duration, max int) ([]*models.Job, error)
	Heartbeat(ctx context.Context, jobID, leaseOwner string, leaseDuration time.Duration) error
	CompleteJob(ctx context.Context, jobID string, result []byte) error
	FailJob(ctx context.Context, jobID string, errMsg string, retryAt *time.Time, deadLetter bool) error
	CancelJob(ctx context.Context, jobID string) error
	RequeueJob(ctx context.Context, jobID string) error
	ReclaimExpiredLeases(ctx context.Context) ([]*models.Job, error)
	GetJob(ctx context.Context, id string) (*models.Job, error)
	ListJobs(ctx context.Context, filter models.JobFilter) ([]*models.Job, error)

	// Workflows
	CreateWorkflow(ctx context.Context, wf *models.Workflow, steps []*models.WorkflowStep) error
	GetWorkflow(ctx context.Context, id string) (*models.Workflow, []*models.WorkflowStep, error)
	ListWorkflows(ctx context.Context, limit, offset int) ([]*models.Workflow, error)
	UpdateStepStatus(ctx context.Context, stepID string, status models.StepStatus, jobID string) error
	UpdateWorkflowStatus(ctx context.Context, id string, status models.WorkflowStatus) error

	// Cron
	UpsertCronSchedule(ctx context.Context, cs *models.CronSchedule) error
	ListCronSchedules(ctx context.Context) ([]*models.CronSchedule, error)
	MarkCronFired(ctx context.Context, id string, lastRun, nextRun time.Time) error

	// Workers
	UpsertWorker(ctx context.Context, w *models.Worker) error
	TouchWorker(ctx context.Context, id string, currentJobs []string) error
	RemoveWorker(ctx context.Context, id string) error
	ListWorkers(ctx context.Context) ([]*models.Worker, error)

	// Observability
	Stats(ctx context.Context) (*models.Stats, error)
}

// Driver identifies which backend to construct.
type Driver string

const (
	DriverSQLite   Driver = "sqlite"
	DriverPostgres Driver = "postgres"
)
