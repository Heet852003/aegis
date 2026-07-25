// Package models defines the core domain types shared across the engine,
// storage backends, and API layer.
package models

import (
	"encoding/json"
	"time"
)

// JobStatus is the lifecycle state of a job.
type JobStatus string

const (
	JobPending    JobStatus = "pending"
	JobRunning    JobStatus = "running"
	JobSucceeded  JobStatus = "succeeded"
	JobFailed     JobStatus = "failed"
	JobDeadLetter JobStatus = "dead_letter"
	JobCancelled  JobStatus = "cancelled"
)

// Job is a single unit of work enqueued for a worker to execute.
type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      JobStatus       `json:"status"`
	Priority    int             `json:"priority"` // higher runs first
	Queue       string          `json:"queue"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`

	// Scheduling.
	ScheduledAt time.Time  `json:"scheduled_at"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseExpiry *time.Time `json:"lease_expiry,omitempty"`

	// Workflow linkage (nullable — a job may be standalone).
	WorkflowID   string `json:"workflow_id,omitempty"`
	WorkflowStep string `json:"workflow_step,omitempty"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

// WorkflowStatus is the lifecycle state of a workflow (DAG run).
type WorkflowStatus string

const (
	WorkflowPending   WorkflowStatus = "pending"
	WorkflowRunning   WorkflowStatus = "running"
	WorkflowSucceeded WorkflowStatus = "succeeded"
	WorkflowFailed    WorkflowStatus = "failed"
)

// Workflow is a named DAG run composed of interdependent steps.
type Workflow struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Status    WorkflowStatus `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// StepStatus mirrors JobStatus but scoped to a workflow step definition,
// since a step may be retried as multiple underlying Job rows.
type StepStatus string

const (
	StepPending   StepStatus = "pending" // waiting on dependencies
	StepReady     StepStatus = "ready"   // dependencies satisfied, not yet enqueued
	StepQueued    StepStatus = "queued"  // job row created
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped" // ancestor failed
)

// WorkflowStep is one node in a workflow DAG.
type WorkflowStep struct {
	ID          string          `json:"id"`
	WorkflowID  string          `json:"workflow_id"`
	Name        string          `json:"name"`
	Type        string          `json:"type"` // job type/handler name
	Payload     json.RawMessage `json:"payload"`
	DependsOn   []string        `json:"depends_on"` // step names
	Status      StepStatus      `json:"status"`
	JobID       string          `json:"job_id,omitempty"`
	MaxAttempts int             `json:"max_attempts"`
}

// WorkflowSpec is the user-facing definition submitted to create a Workflow.
type WorkflowSpec struct {
	Name  string             `json:"name" yaml:"name"`
	Steps []WorkflowStepSpec `json:"steps" yaml:"steps"`
}

// WorkflowStepSpec is the user-facing definition of a single DAG node.
type WorkflowStepSpec struct {
	Name        string          `json:"name" yaml:"name"`
	Type        string          `json:"type" yaml:"type"`
	Payload     json.RawMessage `json:"payload" yaml:"payload"`
	DependsOn   []string        `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty"`
}

// CronSchedule recurs a job template on a cron expression.
type CronSchedule struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Expression  string          `json:"expression"`
	JobType     string          `json:"job_type"`
	Payload     json.RawMessage `json:"payload"`
	Queue       string          `json:"queue"`
	MaxAttempts int             `json:"max_attempts"`
	Enabled     bool            `json:"enabled"`
	NextRun     time.Time       `json:"next_run"`
	LastRun     *time.Time      `json:"last_run,omitempty"`
}

// Worker is a connected worker process registered over the dispatch socket.
type Worker struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Queues        []string  `json:"queues"`
	JobTypes      []string  `json:"job_types"`
	Concurrency   int       `json:"concurrency"`
	ConnectedAt   time.Time `json:"connected_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	CurrentJobs   []string  `json:"current_jobs"`
}

// JobFilter narrows a job listing query.
type JobFilter struct {
	Status     JobStatus
	Queue      string
	Type       string
	WorkflowID string
	Limit      int
	Offset     int
}

// Stats is an aggregate snapshot of queue health used by the dashboard.
type Stats struct {
	Pending      int64   `json:"pending"`
	Running      int64   `json:"running"`
	Succeeded24h int64   `json:"succeeded_24h"`
	Failed24h    int64   `json:"failed_24h"`
	DeadLetter   int64   `json:"dead_letter"`
	Throughput1m float64 `json:"throughput_1m"` // jobs completed per second, trailing 1 minute
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	Workers      int     `json:"workers"`
}
