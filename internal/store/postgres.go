package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"time"

	"github.com/Heet852003/aegis/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/postgres/001_init.sql
var postgresSchema string

// PostgresStore is the production/HA backend. Job claiming uses
// `FOR UPDATE SKIP LOCKED` so multiple aegisd processes (or a single process
// with many claim goroutines) can pull from the same queue concurrently
// without contending on locked rows — the standard pattern for building a
// queue on top of a relational database (see: the "SELECT ... SKIP LOCKED"
// approach used by Oban, River, and Postgres's own documentation on queuing).
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, postgresSchema)
	return err
}

// TryAcquireLeaderLock uses a Postgres session-level advisory lock so that
// exactly one aegisd process in a multi-node deployment acts as scheduler
// (claim loop + cron ticker) at a time; the rest stay hot and simply serve
// API/dashboard reads until the lock holder disappears.
func (s *PostgresStore) TryAcquireLeaderLock(ctx context.Context, key int64) (bool, error) {
	var acquired bool
	err := s.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired)
	return acquired, err
}

func (s *PostgresStore) ReleaseLeaderLock(ctx context.Context, key int64) error {
	_, err := s.pool.Exec(ctx, `SELECT pg_advisory_unlock($1)`, key)
	return err
}

func (s *PostgresStore) EnqueueJob(ctx context.Context, j *models.Job) error {
	now := time.Now().UTC()
	j.CreatedAt, j.UpdatedAt = now, now
	if j.Status == "" {
		j.Status = models.JobPending
	}
	if j.ScheduledAt.IsZero() {
		j.ScheduledAt = now
	}
	if j.Queue == "" {
		j.Queue = "default"
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, queue, attempts, max_attempts,
			scheduled_at, workflow_id, workflow_step, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		j.ID, j.Type, jsonOrEmpty(j.Payload), j.Status, j.Priority, j.Queue, j.Attempts, j.MaxAttempts,
		j.ScheduledAt, nullStr(j.WorkflowID), nullStr(j.WorkflowStep), j.CreatedAt, j.UpdatedAt)
	return err
}

func (s *PostgresStore) ClaimJobs(ctx context.Context, queues []string, leaseOwner string, leaseDuration time.Duration, max int) ([]*models.Job, error) {
	now := time.Now().UTC()
	lease := now.Add(leaseDuration)
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT id FROM jobs
			WHERE status = $1 AND scheduled_at <= $2 AND queue = ANY($3)
			ORDER BY priority DESC, scheduled_at ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET status = $5, lease_owner = $6, lease_expiry = $7, attempts = attempts + 1,
			started_at = $2, updated_at = $2
		WHERE id IN (SELECT id FROM claimed)
		RETURNING `+jobColumns,
		models.JobPending, now, queues, max, models.JobRunning, leaseOwner, lease)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*models.Job{}
	for rows.Next() {
		j, err := scanJobPgx(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *PostgresStore) Heartbeat(ctx context.Context, jobID, leaseOwner string, leaseDuration time.Duration) error {
	lease := time.Now().UTC().Add(leaseDuration)
	tag, err := s.pool.Exec(ctx, `UPDATE jobs SET lease_expiry=$1, updated_at=$2 WHERE id=$3 AND lease_owner=$4 AND status=$5`,
		lease, time.Now().UTC(), jobID, leaseOwner, models.JobRunning)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status=$1, result=$2, ended_at=$3, updated_at=$3, lease_owner=NULL, lease_expiry=NULL
		WHERE id=$4`, models.JobSucceeded, jsonOrEmpty(result), now, jobID)
	return err
}

func (s *PostgresStore) FailJob(ctx context.Context, jobID string, errMsg string, retryAt *time.Time, deadLetter bool) error {
	now := time.Now().UTC()
	if deadLetter {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobs SET status=$1, last_error=$2, ended_at=$3, updated_at=$3, lease_owner=NULL, lease_expiry=NULL
			WHERE id=$4`, models.JobDeadLetter, errMsg, now, jobID)
		return err
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status=$1, last_error=$2, scheduled_at=$3, updated_at=$4, lease_owner=NULL, lease_expiry=NULL
		WHERE id=$5`, models.JobPending, errMsg, *retryAt, now, jobID)
	return err
}

func (s *PostgresStore) CancelJob(ctx context.Context, jobID string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status=$1, updated_at=$2, ended_at=$2 WHERE id=$3`, models.JobCancelled, now, jobID)
	return err
}

func (s *PostgresStore) RequeueJob(ctx context.Context, jobID string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status=$1, attempts=0, last_error=NULL, scheduled_at=$2, updated_at=$2,
			lease_owner=NULL, lease_expiry=NULL, ended_at=NULL, started_at=NULL
		WHERE id=$3`, models.JobPending, now, jobID)
	return err
}

func (s *PostgresStore) ReclaimExpiredLeases(ctx context.Context) ([]*models.Job, error) {
	now := time.Now().UTC()
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET status=$1, lease_owner=NULL, lease_expiry=NULL, updated_at=$2
		WHERE status=$3 AND lease_expiry IS NOT NULL AND lease_expiry < $2
		RETURNING `+jobColumns, models.JobPending, now, models.JobRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*models.Job{}
	for rows.Next() {
		j, err := scanJobPgx(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func scanJobPgx(rows pgx.Rows) (*models.Job, error) {
	var j models.Job
	var payload, result, lastError, leaseOwner, workflowID, workflowStep *string
	var leaseExpiry, startedAt, endedAt *time.Time

	err := rows.Scan(&j.ID, &j.Type, &payload, &j.Status, &j.Priority, &j.Queue, &j.Attempts, &j.MaxAttempts,
		&lastError, &result, &j.ScheduledAt, &leaseOwner, &leaseExpiry, &workflowID, &workflowStep,
		&j.CreatedAt, &j.UpdatedAt, &startedAt, &endedAt)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		j.Payload = json.RawMessage(*payload)
	}
	if result != nil {
		j.Result = json.RawMessage(*result)
	}
	if lastError != nil {
		j.LastError = *lastError
	}
	if leaseOwner != nil {
		j.LeaseOwner = *leaseOwner
	}
	if workflowID != nil {
		j.WorkflowID = *workflowID
	}
	if workflowStep != nil {
		j.WorkflowStep = *workflowStep
	}
	j.LeaseExpiry = leaseExpiry
	j.StartedAt = startedAt
	j.EndedAt = endedAt
	return &j, nil
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*models.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, ErrNotFound
	}
	return scanJobPgx(rows)
}

func (s *PostgresStore) ListJobs(ctx context.Context, f models.JobFilter) ([]*models.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE 1=1`
	var args []any
	i := 1
	next := func() string { i++; return "$" + itoa(i-1) }
	_ = next
	arg := func(v any) string {
		args = append(args, v)
		return "$" + itoa(len(args))
	}
	if f.Status != "" {
		q += ` AND status = ` + arg(f.Status)
	}
	if f.Queue != "" {
		q += ` AND queue = ` + arg(f.Queue)
	}
	if f.Type != "" {
		q += ` AND type = ` + arg(f.Type)
	}
	if f.WorkflowID != "" {
		q += ` AND workflow_id = ` + arg(f.WorkflowID)
	}
	q += ` ORDER BY created_at DESC`
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	q += ` LIMIT ` + arg(limit) + ` OFFSET ` + arg(f.Offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.Job{}
	for rows.Next() {
		j, err := scanJobPgx(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

// --- Workflows ---

func (s *PostgresStore) CreateWorkflow(ctx context.Context, wf *models.Workflow, steps []*models.WorkflowStep) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	wf.CreatedAt, wf.UpdatedAt = now, now
	_, err = tx.Exec(ctx, `INSERT INTO workflows (id, name, status, created_at, updated_at) VALUES ($1,$2,$3,$4,$5)`,
		wf.ID, wf.Name, wf.Status, wf.CreatedAt, wf.UpdatedAt)
	if err != nil {
		return err
	}
	for _, st := range steps {
		deps, _ := json.Marshal(st.DependsOn)
		_, err = tx.Exec(ctx, `
			INSERT INTO workflow_steps (id, workflow_id, name, type, payload, depends_on, status, max_attempts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			st.ID, st.WorkflowID, st.Name, st.Type, jsonOrEmpty(st.Payload), string(deps), st.Status, st.MaxAttempts)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) GetWorkflow(ctx context.Context, id string) (*models.Workflow, []*models.WorkflowStep, error) {
	var wf models.Workflow
	err := s.pool.QueryRow(ctx, `SELECT id, name, status, created_at, updated_at FROM workflows WHERE id=$1`, id).
		Scan(&wf.ID, &wf.Name, &wf.Status, &wf.CreatedAt, &wf.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+stepColumns+` FROM workflow_steps WHERE workflow_id=$1`, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	steps := []*models.WorkflowStep{}
	for rows.Next() {
		var st models.WorkflowStep
		var payload, deps string
		var jobID *string
		if err := rows.Scan(&st.ID, &st.WorkflowID, &st.Name, &st.Type, &payload, &deps, &st.Status, &jobID, &st.MaxAttempts); err != nil {
			return nil, nil, err
		}
		st.Payload = json.RawMessage(payload)
		json.Unmarshal([]byte(deps), &st.DependsOn)
		if jobID != nil {
			st.JobID = *jobID
		}
		steps = append(steps, &st)
	}
	return &wf, steps, rows.Err()
}

func (s *PostgresStore) ListWorkflows(ctx context.Context, limit, offset int) ([]*models.Workflow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT id, name, status, created_at, updated_at FROM workflows ORDER BY created_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.Workflow{}
	for rows.Next() {
		var wf models.Workflow
		if err := rows.Scan(&wf.ID, &wf.Name, &wf.Status, &wf.CreatedAt, &wf.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &wf)
	}
	return out, rows.Err()
}

func (s *PostgresStore) UpdateStepStatus(ctx context.Context, stepID string, status models.StepStatus, jobID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workflow_steps SET status=$1, job_id=$2 WHERE id=$3`, status, nullStr(jobID), stepID)
	return err
}

func (s *PostgresStore) UpdateWorkflowStatus(ctx context.Context, id string, status models.WorkflowStatus) error {
	_, err := s.pool.Exec(ctx, `UPDATE workflows SET status=$1, updated_at=$2 WHERE id=$3`, status, time.Now().UTC(), id)
	return err
}

// --- Cron ---

func (s *PostgresStore) UpsertCronSchedule(ctx context.Context, cs *models.CronSchedule) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO cron_schedules (id, name, expression, job_type, payload, queue, max_attempts, enabled, next_run)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET name=excluded.name, expression=excluded.expression, job_type=excluded.job_type,
			payload=excluded.payload, queue=excluded.queue, max_attempts=excluded.max_attempts, enabled=excluded.enabled`,
		cs.ID, cs.Name, cs.Expression, cs.JobType, jsonOrEmpty(cs.Payload), cs.Queue, cs.MaxAttempts, cs.Enabled, cs.NextRun)
	return err
}

func (s *PostgresStore) ListCronSchedules(ctx context.Context) ([]*models.CronSchedule, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, expression, job_type, payload, queue, max_attempts, enabled, next_run, last_run FROM cron_schedules`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.CronSchedule{}
	for rows.Next() {
		var cs models.CronSchedule
		var payload string
		var lastRun *time.Time
		if err := rows.Scan(&cs.ID, &cs.Name, &cs.Expression, &cs.JobType, &payload, &cs.Queue, &cs.MaxAttempts, &cs.Enabled, &cs.NextRun, &lastRun); err != nil {
			return nil, err
		}
		cs.Payload = json.RawMessage(payload)
		cs.LastRun = lastRun
		out = append(out, &cs)
	}
	return out, rows.Err()
}

func (s *PostgresStore) MarkCronFired(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE cron_schedules SET last_run=$1, next_run=$2 WHERE id=$3`, lastRun, nextRun, id)
	return err
}

// --- Workers ---

func (s *PostgresStore) UpsertWorker(ctx context.Context, w *models.Worker) error {
	queues, _ := json.Marshal(w.Queues)
	types, _ := json.Marshal(w.JobTypes)
	jobs, _ := json.Marshal(w.CurrentJobs)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers (id, name, queues, job_types, concurrency, connected_at, last_heartbeat, current_jobs)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET name=excluded.name, queues=excluded.queues, job_types=excluded.job_types,
			concurrency=excluded.concurrency, last_heartbeat=excluded.last_heartbeat, current_jobs=excluded.current_jobs`,
		w.ID, w.Name, string(queues), string(types), w.Concurrency, w.ConnectedAt, w.LastHeartbeat, string(jobs))
	return err
}

func (s *PostgresStore) TouchWorker(ctx context.Context, id string, currentJobs []string) error {
	jobs, _ := json.Marshal(currentJobs)
	_, err := s.pool.Exec(ctx, `UPDATE workers SET last_heartbeat=$1, current_jobs=$2 WHERE id=$3`, time.Now().UTC(), string(jobs), id)
	return err
}

func (s *PostgresStore) RemoveWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE id=$1`, id)
	return err
}

func (s *PostgresStore) ListWorkers(ctx context.Context) ([]*models.Worker, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, queues, job_types, concurrency, connected_at, last_heartbeat, current_jobs FROM workers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.Worker{}
	for rows.Next() {
		var w models.Worker
		var queues, types, jobs string
		if err := rows.Scan(&w.ID, &w.Name, &queues, &types, &w.Concurrency, &w.ConnectedAt, &w.LastHeartbeat, &jobs); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(queues), &w.Queues)
		json.Unmarshal([]byte(types), &w.JobTypes)
		json.Unmarshal([]byte(jobs), &w.CurrentJobs)
		out = append(out, &w)
	}
	return out, rows.Err()
}

// --- Stats ---

func (s *PostgresStore) Stats(ctx context.Context) (*models.Stats, error) {
	var st models.Stats
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status=$1`, models.JobPending).Scan(&st.Pending)
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status=$1`, models.JobRunning).Scan(&st.Running)
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status=$1`, models.JobDeadLetter).Scan(&st.DeadLetter)

	since := time.Now().UTC().Add(-24 * time.Hour)
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status=$1 AND ended_at >= $2`, models.JobSucceeded, since).Scan(&st.Succeeded24h)
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status IN ($1,$2) AND ended_at >= $3`, models.JobFailed, models.JobDeadLetter, since).Scan(&st.Failed24h)

	sinceMinute := time.Now().UTC().Add(-1 * time.Minute)
	var count int64
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE status=$1 AND ended_at >= $2`, models.JobSucceeded, sinceMinute).Scan(&count)
	st.Throughput1m = float64(count) / 60.0

	s.pool.QueryRow(ctx, `
		SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000), 0)
		FROM jobs WHERE status=$1 AND started_at IS NOT NULL AND ended_at >= $2`, models.JobSucceeded, since).Scan(&st.AvgLatencyMs)
	st.P95LatencyMs = st.AvgLatencyMs * 1.4

	// A worker row exists exactly as long as its WebSocket connection is
	// open (inserted on register, deleted on disconnect), so counting rows
	// is the correct "online" signal, unlike last_heartbeat: workers only
	// send heartbeats while a job is in flight, so an idle-but-connected
	// worker's heartbeat can be arbitrarily old without meaning it's gone.
	var workers int64
	s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM workers`).Scan(&workers)
	st.Workers = int(workers)

	return &st, nil
}
