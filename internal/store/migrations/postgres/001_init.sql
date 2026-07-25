CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    type          TEXT NOT NULL,
    payload       TEXT NOT NULL DEFAULT '{}',
    status        TEXT NOT NULL,
    priority      INTEGER NOT NULL DEFAULT 0,
    queue         TEXT NOT NULL DEFAULT 'default',
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL DEFAULT 3,
    last_error    TEXT,
    result        TEXT,
    scheduled_at  TIMESTAMPTZ NOT NULL,
    lease_owner   TEXT,
    lease_expiry  TIMESTAMPTZ,
    workflow_id   TEXT,
    workflow_step TEXT,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, queue, priority DESC, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_jobs_workflow ON jobs (workflow_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry ON jobs (lease_expiry);

CREATE TABLE IF NOT EXISTS workflows (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_steps (
    id           TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL,
    name         TEXT NOT NULL,
    type         TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '{}',
    depends_on   TEXT NOT NULL DEFAULT '[]',
    status       TEXT NOT NULL,
    job_id       TEXT,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    UNIQUE(workflow_id, name)
);

CREATE INDEX IF NOT EXISTS idx_steps_workflow ON workflow_steps (workflow_id);

CREATE TABLE IF NOT EXISTS cron_schedules (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    expression   TEXT NOT NULL,
    job_type     TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '{}',
    queue        TEXT NOT NULL DEFAULT 'default',
    max_attempts INTEGER NOT NULL DEFAULT 3,
    enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    next_run     TIMESTAMPTZ NOT NULL,
    last_run     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    queues         TEXT NOT NULL DEFAULT '[]',
    job_types      TEXT NOT NULL DEFAULT '[]',
    concurrency    INTEGER NOT NULL DEFAULT 1,
    connected_at   TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    current_jobs   TEXT NOT NULL DEFAULT '[]'
);
