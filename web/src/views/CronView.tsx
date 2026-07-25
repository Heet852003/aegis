import { useEffect, useState } from 'react'
import { Plus } from 'lucide-react'
import { api } from '../api'
import type { CronSchedule } from '../types'
import { formatTime, timeAgo } from '../lib/time'

export default function CronView() {
  const [schedules, setSchedules] = useState<CronSchedule[]>([])
  const [showForm, setShowForm] = useState(false)
  const [error, setError] = useState('')

  const refetch = () => api.listCron().then(setSchedules).catch((e) => setError(String(e)))
  useEffect(() => {
    refetch()
    const id = setInterval(refetch, 5000)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-slate-100">Cron schedules</h1>
          <p className="text-sm text-slate-500">Recurring jobs, fired by the leader node on their cron expression.</p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium px-3 py-2"
        >
          <Plus size={15} /> New schedule
        </button>
      </div>

      {showForm && <CronForm onCreated={() => { setShowForm(false); refetch() }} onError={setError} />}
      {error && <div className="text-sm text-red-400">{error}</div>}

      <div className="rounded-xl border border-white/10 bg-white/[0.03] overflow-hidden">
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-500 border-b border-white/10">
            <tr>
              <th className="text-left px-4 py-2 font-medium">Name</th>
              <th className="text-left px-4 py-2 font-medium">Expression</th>
              <th className="text-left px-4 py-2 font-medium">Job type</th>
              <th className="text-left px-4 py-2 font-medium">Next run</th>
              <th className="text-left px-4 py-2 font-medium">Last run</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {schedules.map((c) => (
              <tr key={c.id}>
                <td className="px-4 py-2 text-slate-200">{c.name}</td>
                <td className="px-4 py-2 font-mono text-xs text-slate-400">{c.expression}</td>
                <td className="px-4 py-2 text-slate-400">{c.job_type}</td>
                <td className="px-4 py-2 text-slate-400 text-xs">{formatTime(c.next_run)}</td>
                <td className="px-4 py-2 text-slate-500 text-xs">{c.last_run ? timeAgo(c.last_run) : 'never'}</td>
              </tr>
            ))}
            {schedules.length === 0 && (
              <tr><td className="px-4 py-8 text-center text-slate-500" colSpan={5}>No cron schedules yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function CronForm({ onCreated, onError }: { onCreated: () => void; onError: (e: string) => void }) {
  const [name, setName] = useState('')
  const [expression, setExpression] = useState('*/5 * * * *')
  const [jobType, setJobType] = useState('')
  const [payload, setPayload] = useState('{}')
  const [submitting, setSubmitting] = useState(false)

  async function submit() {
    if (!name.trim() || !jobType.trim()) return onError('name and job type are required')
    let parsed: unknown
    try {
      parsed = JSON.parse(payload || '{}')
    } catch {
      return onError('payload must be valid JSON')
    }
    setSubmitting(true)
    try {
      await api.createCron({ name, expression, job_type: jobType, payload: parsed, queue: 'default', max_attempts: 3 })
      onCreated()
    } catch (e) {
      onError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 grid grid-cols-1 md:grid-cols-2 gap-3">
      <div>
        <label className="text-xs text-slate-400">Name</label>
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="nightly-report"
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Cron expression</label>
        <input value={expression} onChange={(e) => setExpression(e.target.value)}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 font-mono" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Job type</label>
        <input value={jobType} onChange={(e) => setJobType(e.target.value)} placeholder="generate_report"
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200" />
      </div>
      <div>
        <label className="text-xs text-slate-400">Payload (JSON)</label>
        <input value={payload} onChange={(e) => setPayload(e.target.value)}
          className="mt-1 w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-1.5 text-sm text-slate-200 font-mono" />
      </div>
      <div className="md:col-span-2 flex justify-end">
        <button disabled={submitting} onClick={submit}
          className="rounded-lg bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white text-sm font-medium px-4 py-2">
          {submitting ? 'Creating…' : 'Create schedule'}
        </button>
      </div>
    </div>
  )
}
