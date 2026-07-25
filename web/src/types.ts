export type JobStatus =
  | 'pending'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'dead_letter'
  | 'cancelled'

export interface Job {
  id: string
  type: string
  payload: unknown
  status: JobStatus
  priority: number
  queue: string
  attempts: number
  max_attempts: number
  last_error?: string
  result?: unknown
  scheduled_at: string
  lease_owner?: string
  lease_expiry?: string
  workflow_id?: string
  workflow_step?: string
  created_at: string
  updated_at: string
  started_at?: string
  ended_at?: string
}

export type WorkflowStatus = 'pending' | 'running' | 'succeeded' | 'failed'

export interface Workflow {
  id: string
  name: string
  status: WorkflowStatus
  created_at: string
  updated_at: string
}

export type StepStatus = 'pending' | 'ready' | 'queued' | 'succeeded' | 'failed' | 'skipped'

export interface WorkflowStep {
  id: string
  workflow_id: string
  name: string
  type: string
  payload: unknown
  depends_on: string[] | null
  status: StepStatus
  job_id?: string
  max_attempts: number
}

export interface WorkflowDetail {
  workflow: Workflow
  steps: WorkflowStep[]
}

export interface CronSchedule {
  id: string
  name: string
  expression: string
  job_type: string
  payload: unknown
  queue: string
  max_attempts: number
  enabled: boolean
  next_run: string
  last_run?: string
}

export interface Worker {
  id: string
  name: string
  queues: string[]
  job_types: string[]
  concurrency: number
  connected_at: string
  last_heartbeat: string
  current_jobs: string[]
}

export interface Stats {
  pending: number
  running: number
  succeeded_24h: number
  failed_24h: number
  dead_letter: number
  throughput_1m: number
  avg_latency_ms: number
  p95_latency_ms: number
  workers: number
}

export interface DashboardEvent {
  type: string
  data: unknown
}
