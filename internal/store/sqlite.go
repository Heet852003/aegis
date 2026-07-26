package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Heet852003/aegis/internal/models"
	_ "modernc.org/sqlite"
)

//go:embed migrations/sqlite/001_init.sql
var sqliteSchema string

// SQLiteStore is the zero-dependency storage backend. It is single-writer by
// design (SQLite serializes writers internally); a mutex around write
// transactions keeps job-claiming atomic without relying on SKIP LOCKED,
// which SQLite does not support.
type SQLiteStore struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// NewSQLite opens (creating if necessary) a SQLite database file at path.
// Use ":memory:" for ephemeral/test databases.
func NewSQLite(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // single-writer; simplest correct concurrency model for embedded SQLite
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, sqliteSchema)
	return err
}

func jsonOrEmpty(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

func (s *SQLiteStore) EnqueueJob(ctx context.Context, j *models.Job) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

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

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, type, payload, status, priority, queue, attempts, max_attempts,
			scheduled_at, workflow_id, workflow_step, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.Type, jsonOrEmpty(j.Payload), j.Status, j.Priority, j.Queue, j.Attempts, j.MaxAttempts,
		j.ScheduledAt, nullStr(j.WorkflowID), nullStr(j.WorkflowStep), j.CreatedAt, j.UpdatedAt,
	)
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ClaimJobs atomically transitions up to max pending, due jobs (from the
// given queues) to running and assigns a lease. Because SQLite has a single
// writer, the surrounding mutex plus one transaction is sufficient to
// guarantee no two callers ever claim the same row — the equivalent of
// Postgres's `FOR UPDATE SKIP LOCKED` under single-writer semantics.
func (s *SQLiteStore) ClaimJobs(ctx context.Context, queues []string, leaseOwner string, leaseDuration time.Duration, max int) ([]*models.Job, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	placeholders := make([]string, len(queues))
	args := make([]any, 0, len(queues)+3)
	args = append(args, models.JobPending, now)
	for i, q := range queues {
		placeholders[i] = "?"
		args = append(args, q)
	}
	args = append(args, max)

	query := fmt.Sprintf(`
		SELECT id FROM jobs
		WHERE status = ? AND scheduled_at <= ?
		AND queue IN (%s)
		ORDER BY priority DESC, scheduled_at ASC
		LIMIT ?`, strings.Join(placeholders, ","))

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	lease := now.Add(leaseDuration)
	jobs := make([]*models.Job, 0, len(ids))
	for _, id := range ids {
		_, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = ?, lease_owner = ?, lease_expiry = ?, attempts = attempts + 1,
				started_at = ?, updated_at = ? WHERE id = ?`,
			models.JobRunning, leaseOwner, lease, now, now, id)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for _, id := range ids {
		j, err := s.GetJob(ctx, id)
		if err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func (s *SQLiteStore) Heartbeat(ctx context.Context, jobID, leaseOwner string, leaseDuration time.Duration) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	lease := time.Now().UTC().Add(leaseDuration)
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET lease_expiry = ?, updated_at = ? WHERE id = ? AND lease_owner = ? AND status = ?`,
		lease, time.Now().UTC(), jobID, leaseOwner, models.JobRunning)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = ?, result = ?, ended_at = ?, updated_at = ?, lease_owner = NULL, lease_expiry = NULL
		WHERE id = ?`, models.JobSucceeded, jsonOrEmpty(result), now, now, jobID)
	return err
}

func (s *SQLiteStore) FailJob(ctx context.Context, jobID string, errMsg string, retryAt *time.Time, deadLetter bool) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().UTC()

	if deadLetter {
		_, err := s.db.ExecContext(ctx, `
			UPDATE jobs SET status = ?, last_error = ?, ended_at = ?, updated_at = ?, lease_owner = NULL, lease_expiry = NULL
			WHERE id = ?`, models.JobDeadLetter, errMsg, now, now, jobID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = ?, last_error = ?, scheduled_at = ?, updated_at = ?, lease_owner = NULL, lease_expiry = NULL
		WHERE id = ?`, models.JobPending, errMsg, *retryAt, now, jobID)
	return err
}

func (s *SQLiteStore) CancelJob(ctx context.Context, jobID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status = ?, updated_at = ?, ended_at = ? WHERE id = ?`,
		models.JobCancelled, now, now, jobID)
	return err
}

func (s *SQLiteStore) RequeueJob(ctx context.Context, jobID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = ?, attempts = 0, last_error = NULL, scheduled_at = ?, updated_at = ?,
			lease_owner = NULL, lease_expiry = NULL, ended_at = NULL, started_at = NULL
		WHERE id = ?`, models.JobPending, now, now, jobID)
	return err
}

func (s *SQLiteStore) ReclaimExpiredLeases(ctx context.Context) ([]*models.Job, error) {
	s.writeMu.Lock()
	now := time.Now().UTC()
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM jobs WHERE status = ? AND lease_expiry IS NOT NULL AND lease_expiry < ?`,
		models.JobRunning, now)
	if err != nil {
		s.writeMu.Unlock()
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		s.db.ExecContext(ctx, `UPDATE jobs SET status = ?, lease_owner = NULL, lease_expiry = NULL, updated_at = ? WHERE id = ?`,
			models.JobPending, now, id)
	}
	s.writeMu.Unlock()

	jobs := make([]*models.Job, 0, len(ids))
	for _, id := range ids {
		j, err := s.GetJob(ctx, id)
		if err == nil {
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

func scanJob(row interface{ Scan(...any) error }) (*models.Job, error) {
	var j models.Job
	var payload, result, lastError, leaseOwner, workflowID, workflowStep sql.NullString
	var leaseExpiry, startedAt, endedAt sql.NullTime

	err := row.Scan(&j.ID, &j.Type, &payload, &j.Status, &j.Priority, &j.Queue, &j.Attempts, &j.MaxAttempts,
		&lastError, &result, &j.ScheduledAt, &leaseOwner, &leaseExpiry, &workflowID, &workflowStep,
		&j.CreatedAt, &j.UpdatedAt, &startedAt, &endedAt)
	if err != nil {
		return nil, err
	}
	j.Payload = json.RawMessage(payload.String)
	if result.Valid {
		j.Result = json.RawMessage(result.String)
	}
	j.LastError = lastError.String
	j.LeaseOwner = leaseOwner.String
	j.WorkflowID = workflowID.String
	j.WorkflowStep = workflowStep.String
	if leaseExpiry.Valid {
		j.LeaseExpiry = &leaseExpiry.Time
	}
	if startedAt.Valid {
		j.StartedAt = &startedAt.Time
	}
	if endedAt.Valid {
		j.EndedAt = &endedAt.Time
	}
	return &j, nil
}

const jobColumns = `id, type, payload, status, priority, queue, attempts, max_attempts,
	last_error, result, scheduled_at, lease_owner, lease_expiry, workflow_id, workflow_step,
	created_at, updated_at, started_at, ended_at`

func (s *SQLiteStore) GetJob(ctx context.Context, id string) (*models.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return j, err
}

func (s *SQLiteStore) ListJobs(ctx context.Context, f models.JobFilter) ([]*models.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE 1=1`
	var args []any
	if f.Status != "" {
		q += ` AND status = ?`
		args = append(args, f.Status)
	}
	if f.Queue != "" {
		q += ` AND queue = ?`
		args = append(args, f.Queue)
	}
	if f.Type != "" {
		q += ` AND type = ?`
		args = append(args, f.Type)
	}
	if f.WorkflowID != "" {
		q += ` AND workflow_id = ?`
		args = append(args, f.WorkflowID)
	}
	q += ` ORDER BY created_at DESC`
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	q += ` LIMIT ? OFFSET ?`
	args = append(args, limit, f.Offset)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// --- Workflows ---

func (s *SQLiteStore) CreateWorkflow(ctx context.Context, wf *models.Workflow, steps []*models.WorkflowStep) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	wf.CreatedAt, wf.UpdatedAt = now, now
	_, err = tx.ExecContext(ctx, `INSERT INTO workflows (id, name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		wf.ID, wf.Name, wf.Status, wf.CreatedAt, wf.UpdatedAt)
	if err != nil {
		return err
	}
	for _, st := range steps {
		deps, _ := json.Marshal(st.DependsOn)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO workflow_steps (id, workflow_id, name, type, payload, depends_on, status, max_attempts)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			st.ID, st.WorkflowID, st.Name, st.Type, jsonOrEmpty(st.Payload), string(deps), st.Status, st.MaxAttempts)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func scanStep(row interface{ Scan(...any) error }) (*models.WorkflowStep, error) {
	var st models.WorkflowStep
	var payload, deps string
	var jobID sql.NullString
	if err := row.Scan(&st.ID, &st.WorkflowID, &st.Name, &st.Type, &payload, &deps, &st.Status, &jobID, &st.MaxAttempts); err != nil {
		return nil, err
	}
	st.Payload = json.RawMessage(payload)
	json.Unmarshal([]byte(deps), &st.DependsOn)
	st.JobID = jobID.String
	return &st, nil
}

const stepColumns = `id, workflow_id, name, type, payload, depends_on, status, job_id, max_attempts`

func (s *SQLiteStore) GetWorkflow(ctx context.Context, id string) (*models.Workflow, []*models.WorkflowStep, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, status, created_at, updated_at FROM workflows WHERE id = ?`, id)
	var wf models.Workflow
	if err := row.Scan(&wf.ID, &wf.Name, &wf.Status, &wf.CreatedAt, &wf.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+stepColumns+` FROM workflow_steps WHERE workflow_id = ?`, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	steps := []*models.WorkflowStep{}
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, nil, err
		}
		steps = append(steps, st)
	}
	return &wf, steps, rows.Err()
}

func (s *SQLiteStore) ListWorkflows(ctx context.Context, limit, offset int) ([]*models.Workflow, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, status, created_at, updated_at FROM workflows ORDER BY created_at DESC LIMIT ? OFFSET ?`, limit, offset)
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

func (s *SQLiteStore) UpdateStepStatus(ctx context.Context, stepID string, status models.StepStatus, jobID string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE workflow_steps SET status = ?, job_id = ? WHERE id = ?`, status, nullStr(jobID), stepID)
	return err
}

func (s *SQLiteStore) UpdateWorkflowStatus(ctx context.Context, id string, status models.WorkflowStatus) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE workflows SET status = ?, updated_at = ? WHERE id = ?`, status, time.Now().UTC(), id)
	return err
}

// --- Cron ---

func (s *SQLiteStore) UpsertCronSchedule(ctx context.Context, cs *models.CronSchedule) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	enabled := 0
	if cs.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cron_schedules (id, name, expression, job_type, payload, queue, max_attempts, enabled, next_run)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET name=excluded.name, expression=excluded.expression, job_type=excluded.job_type,
			payload=excluded.payload, queue=excluded.queue, max_attempts=excluded.max_attempts, enabled=excluded.enabled`,
		cs.ID, cs.Name, cs.Expression, cs.JobType, jsonOrEmpty(cs.Payload), cs.Queue, cs.MaxAttempts, enabled, cs.NextRun)
	return err
}

func (s *SQLiteStore) ListCronSchedules(ctx context.Context) ([]*models.CronSchedule, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, expression, job_type, payload, queue, max_attempts, enabled, next_run, last_run FROM cron_schedules`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*models.CronSchedule{}
	for rows.Next() {
		var cs models.CronSchedule
		var payload string
		var enabled int
		var lastRun sql.NullTime
		if err := rows.Scan(&cs.ID, &cs.Name, &cs.Expression, &cs.JobType, &payload, &cs.Queue, &cs.MaxAttempts, &enabled, &cs.NextRun, &lastRun); err != nil {
			return nil, err
		}
		cs.Payload = json.RawMessage(payload)
		cs.Enabled = enabled == 1
		if lastRun.Valid {
			cs.LastRun = &lastRun.Time
		}
		out = append(out, &cs)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) MarkCronFired(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE cron_schedules SET last_run = ?, next_run = ? WHERE id = ?`, lastRun, nextRun, id)
	return err
}

// --- Workers ---

func (s *SQLiteStore) UpsertWorker(ctx context.Context, w *models.Worker) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	queues, _ := json.Marshal(w.Queues)
	types, _ := json.Marshal(w.JobTypes)
	jobs, _ := json.Marshal(w.CurrentJobs)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workers (id, name, queues, job_types, concurrency, connected_at, last_heartbeat, current_jobs)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET name=excluded.name, queues=excluded.queues, job_types=excluded.job_types,
			concurrency=excluded.concurrency, last_heartbeat=excluded.last_heartbeat, current_jobs=excluded.current_jobs`,
		w.ID, w.Name, string(queues), string(types), w.Concurrency, w.ConnectedAt, w.LastHeartbeat, string(jobs))
	return err
}

func (s *SQLiteStore) TouchWorker(ctx context.Context, id string, currentJobs []string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	jobs, _ := json.Marshal(currentJobs)
	_, err := s.db.ExecContext(ctx, `UPDATE workers SET last_heartbeat = ?, current_jobs = ? WHERE id = ?`,
		time.Now().UTC(), string(jobs), id)
	return err
}

func (s *SQLiteStore) RemoveWorker(ctx context.Context, id string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.db.ExecContext(ctx, `DELETE FROM workers WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) ListWorkers(ctx context.Context) ([]*models.Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, queues, job_types, concurrency, connected_at, last_heartbeat, current_jobs FROM workers`)
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

func (s *SQLiteStore) Stats(ctx context.Context) (*models.Stats, error) {
	var st models.Stats
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = ?`, models.JobPending)
	row.Scan(&st.Pending)
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = ?`, models.JobRunning)
	row.Scan(&st.Running)
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = ?`, models.JobDeadLetter)
	row.Scan(&st.DeadLetter)

	since := time.Now().UTC().Add(-24 * time.Hour)
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = ? AND ended_at >= ?`, models.JobSucceeded, since)
	row.Scan(&st.Succeeded24h)
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status IN (?, ?) AND ended_at >= ?`, models.JobFailed, models.JobDeadLetter, since)
	row.Scan(&st.Failed24h)

	sinceMinute := time.Now().UTC().Add(-1 * time.Minute)
	var count int64
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = ? AND ended_at >= ?`, models.JobSucceeded, sinceMinute)
	row.Scan(&count)
	st.Throughput1m = float64(count) / 60.0

	row = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(AVG((julianday(ended_at) - julianday(started_at)) * 86400000), 0)
		FROM jobs WHERE status = ? AND started_at IS NOT NULL AND ended_at >= ?`, models.JobSucceeded, since)
	row.Scan(&st.AvgLatencyMs)
	st.P95LatencyMs = st.AvgLatencyMs * 1.4 // approximation without a full histogram; documented in README

	// A worker row exists exactly as long as its WebSocket connection is
	// open (inserted on register, deleted on disconnect), so counting rows
	// is the correct "online" signal, unlike last_heartbeat: workers only
	// send heartbeats while a job is in flight, so an idle-but-connected
	// worker's heartbeat can be arbitrarily old without meaning it's gone.
	row = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM workers`)
	var workers int64
	row.Scan(&workers)
	st.Workers = int(workers)

	return &st, nil
}
