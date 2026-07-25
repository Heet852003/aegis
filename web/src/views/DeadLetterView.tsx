import { useState } from 'react'
import { RotateCcw } from 'lucide-react'
import { api } from '../api'
import type { Job } from '../types'
import { useLiveRefetch } from '../hooks/useLive'
import { timeAgo } from '../lib/time'

export default function DeadLetterView() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [error, setError] = useState('')

  const refetch = () => api.listJobs({ status: 'dead_letter', limit: '100' }).then(setJobs).catch((e) => setError(String(e)))
  useLiveRefetch(['job.updated'], refetch)

  async function requeue(id: string) {
    try {
      await api.requeueJob(id)
      refetch()
    } catch (e) {
      setError(String(e))
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-100">Dead letter queue</h1>
        <p className="text-sm text-slate-500">Jobs that exhausted every retry. Inspect the error, fix the root cause, then requeue.</p>
      </div>
      {error && <div className="text-sm text-red-400">{error}</div>}

      <div className="space-y-3">
        {jobs.map((j) => (
          <div key={j.id} className="rounded-xl border border-red-500/20 bg-red-500/[0.04] p-4">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-slate-100 font-medium">{j.type}</span>
                <span className="text-xs text-slate-500 ml-2 font-mono">{j.id.slice(0, 8)}</span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-slate-500">{timeAgo(j.ended_at)} · {j.attempts}/{j.max_attempts} attempts</span>
                <button
                  onClick={() => requeue(j.id)}
                  className="flex items-center gap-1 text-xs rounded-lg bg-violet-600 hover:bg-violet-500 text-white px-2.5 py-1.5"
                >
                  <RotateCcw size={13} /> Requeue
                </button>
              </div>
            </div>
            {j.last_error && (
              <pre className="mt-2 text-xs text-red-300 bg-black/30 rounded-lg p-2 overflow-auto">{j.last_error}</pre>
            )}
          </div>
        ))}
        {jobs.length === 0 && (
          <div className="rounded-xl border border-white/10 bg-white/[0.03] p-10 text-center text-slate-500 text-sm">
            Nothing dead-lettered. Everything's healthy.
          </div>
        )}
      </div>
    </div>
  )
}
