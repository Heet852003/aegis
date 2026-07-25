import { useState } from 'react'
import { Plus, RotateCcw, X } from 'lucide-react'
import { api } from '../api'
import type { Job, JobStatus } from '../types'
import StatusBadge from '../components/StatusBadge'
import { useLiveRefetch } from '../hooks/useLive'
import { timeAgo } from '../lib/time'

const STATUSES: JobStatus[] = ['pending', 'running', 'succeeded', 'failed', 'dead_letter', 'cancelled']

export default function JobsView() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [status, setStatus] = useState('')
  const [queue, setQueue] = useState('')
  const [error, setError] = useState('')
  const [showForm, setShowForm] = useState(false)

  const refetch = () => {
    const params: Record<string, string> = { limit: '100' }
    if (status) params.status = status
    if (queue) params.queue = queue
    api.listJobs(params).then(setJobs).catch((e) => setError(String(e)))
  }

  useLiveRefetch(['job.enqueued', 'job.updated'], refetch)

  async function act(fn: () => Promise<unknown>) {
    try {
      await fn()
      refetch()
    } catch (e) {
      setError(String(e))
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-slate-100">Jobs</h1>
          <p className="text-sm text-slate-500">Every job the scheduler knows about, updated in real time.</p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium px-3 py-2"
        >
          <Plus size={15} /> Submit job
        </button>
      </div>

      {showForm && <SubmitJobForm onSubmitted={() => { setShowForm(false); refetch() }} onError={setError} />}

      <div className="flex items-center gap-2">
        <select
          value={status}
          onChange={(e) => { setStatus(e.target.value); }}
          onBlur={refetch}
          className="bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200"
        >
          <option value="">All statuses</option>
          {STATUSES.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <input
          value={queue}
          onChange={(e) => setQueue(e.target.value)}
          placeholder="filter by queue..."
          className="bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 placeholder:text-slate-600"
        />
        <button onClick={refetch} className="text-sm text-violet-300 hover:text-violet-200 px-2">Apply</button>
      </div>

      {error && <div className="text-sm text-red-400">{error}</div>}

      <div className="rounded-xl border border-white/10 bg-white/[0.03] overflow-hidden">
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-500 border-b border-white/10">
            <tr>
              <th className="text-left px-4 py-2 font-medium">ID</th>
              <th className="text-left px-4 py-2 font-medium">Type</th>
              <th className="text-left px-4 py-2 font-medium">Queue</th>
              <th className="text-left px-4 py-2 font-medium">Status</th>
              <th className="text-left px-4 py-2 font-medium">Attempts</th>
              <th className="text-left px-4 py-2 font-medium">Updated</th>
              <th className="text-right px-4 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {jobs.map((j) => (
              <tr key={j.id} className="hover:bg-white/[0.02]">
                <td className="px-4 py-2 font-mono text-xs text-slate-500">{j.id.slice(0, 8)}</td>
                <td className="px-4 py-2 text-slate-200">{j.type}</td>
                <td className="px-4 py-2 text-slate-400">{j.queue}</td>
                <td className="px-4 py-2"><StatusBadge status={j.status} /></td>
                <td className="px-4 py-2 text-slate-400 tabular-nums">{j.attempts}/{j.max_attempts}</td>
                <td className="px-4 py-2 text-slate-500 text-xs">{timeAgo(j.updated_at)}</td>
                <td className="px-4 py-2 text-right space-x-2">
                  {j.status === 'pending' && (
                    <button title="Cancel" onClick={() => act(() => api.cancelJob(j.id))} className="text-slate-400 hover:text-red-400">
                      <X size={15} />
                    </button>
                  )}
                  {j.status === 'dead_letter' && (
                    <button title="Requeue" onClick={() => act(() => api.requeueJob(j.id))} className="text-slate-400 hover:text-violet-300">
                      <RotateCcw size={15} />
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {jobs.length === 0 && (
              <tr><td className="px-4 py-8 text-center text-slate-500" colSpan={7}>No jobs match this filter.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function SubmitJobForm({ onSubmitted, onError }: { onSubmitted: () => void; onError: (e: string) => void }) {
  const [type, setType] = useState('')
  const [payload, setPayload] = useState('{}')
  const [queue, setQueue] = useState('default')
  const [priority, setPriority] = useState(0)
  const [maxAttempts, setMaxAttempts] = useState(3)
  const [submitting, setSubmitting] = useState(false)

  async function submit() {
    if (!type.trim()) return onError('job type is required')
    let parsed: unknown
    try {
      parsed = JSON.parse(payload || '{}')
    } catch {
      return onError('payload must be valid JSON')
    }
    setSubmitting(true)
    try {
      await api.submitJob({ type, payload: parsed, queue, priority, max_attempts: maxAttempts })
      onSubmitted()
    } catch (e) {
      onError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 grid grid-cols-1 md:grid-cols-2 gap-3">
      <div>
        <label className="text-xs text-slate-400">Job type</label>
        <input value={type} onChange={(e) => setType(e.target.value)} placeholder="send_email"
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Queue</label>
        <input value={queue} onChange={(e) => setQueue(e.target.value)}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div className="md:col-span-2">
        <label className="text-xs text-slate-400">Payload (JSON)</label>
        <textarea value={payload} onChange={(e) => setPayload(e.target.value)} rows={3}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 font-mono" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Priority</label>
        <input type="number" value={priority} onChange={(e) => setPriority(Number(e.target.value))}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Max attempts</label>
        <input type="number" value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div className="md:col-span-2 flex justify-end">
        <button disabled={submitting} onClick={submit}
          className="rounded-lg bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white text-sm font-medium px-4 py-2">
          {submitting ? 'Submitting…' : 'Submit job'}
        </button>
      </div>
    </div>
  )
}
