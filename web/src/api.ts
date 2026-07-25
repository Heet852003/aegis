import type { CronSchedule, Job, Stats, Worker, Workflow, WorkflowDetail } from './types'

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  const text = await res.text()
  const data = text ? JSON.parse(text) : undefined
  if (!res.ok) {
    throw new Error((data && data.error) || `request failed: ${res.status}`)
  }
  return data as T
}

export const api = {
  listJobs: (params: Record<string, string> = {}) =>
    req<Job[]>('GET', `/api/v1/jobs?${new URLSearchParams(params)}`),
  getJob: (id: string) => req<Job>('GET', `/api/v1/jobs/${id}`),
  submitJob: (input: {
    type: string
    payload?: unknown
    queue?: string
    priority?: number
    max_attempts?: number
    delay_seconds?: number
  }) => req<Job>('POST', '/api/v1/jobs', input),
  cancelJob: (id: string) => req<void>('POST', `/api/v1/jobs/${id}/cancel`),
  requeueJob: (id: string) => req<void>('POST', `/api/v1/jobs/${id}/requeue`),

  listWorkflows: (params: Record<string, string> = {}) =>
    req<Workflow[]>('GET', `/api/v1/workflows?${new URLSearchParams(params)}`),
  getWorkflow: (id: string) => req<WorkflowDetail>('GET', `/api/v1/workflows/${id}`),
  submitWorkflow: (spec: unknown) => req<Workflow>('POST', '/api/v1/workflows', spec),

  listCron: () => req<CronSchedule[]>('GET', '/api/v1/cron'),
  createCron: (input: unknown) => req<CronSchedule>('POST', '/api/v1/cron', input),

  listWorkers: () => req<Worker[]>('GET', '/api/v1/workers'),
  stats: () => req<Stats>('GET', '/api/v1/stats'),
}
