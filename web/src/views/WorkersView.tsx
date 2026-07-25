import { useState } from 'react'
import { api } from '../api'
import type { Worker } from '../types'
import { useLiveRefetch } from '../hooks/useLive'
import { timeAgo } from '../lib/time'

export default function WorkersView() {
  const [workers, setWorkers] = useState<Worker[]>([])

  useLiveRefetch(['worker.updated'], () => {
    api.listWorkers().then(setWorkers).catch(() => {})
  })

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-100">Workers</h1>
        <p className="text-sm text-slate-500">Every worker process currently connected over the dispatch socket.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {workers.map((w) => (
          <div key={w.id} className="rounded-xl border border-white/10 bg-white/[0.03] p-4 space-y-2">
            <div className="flex items-center justify-between">
              <span className="font-medium text-slate-100">{w.name}</span>
              <span className="text-xs text-slate-500">{timeAgo(w.last_heartbeat)}</span>
            </div>
            <div className="text-xs text-slate-500 font-mono">{w.id.slice(0, 8)}</div>
            <div className="flex flex-wrap gap-1">
              {w.queues.map((q) => (
                <span key={q} className="text-[11px] rounded-full px-2 py-0.5 bg-sky-500/10 text-sky-300 ring-1 ring-sky-500/20">{q}</span>
              ))}
            </div>
            <div className="flex flex-wrap gap-1">
              {w.job_types.map((t) => (
                <span key={t} className="text-[11px] rounded-full px-2 py-0.5 bg-violet-500/10 text-violet-300 ring-1 ring-violet-500/20">{t}</span>
              ))}
            </div>
            <div className="flex items-center justify-between text-sm pt-1">
              <span className="text-slate-400">In flight</span>
              <span className="tabular-nums text-slate-200">{w.current_jobs?.length ?? 0} / {w.concurrency}</span>
            </div>
          </div>
        ))}
        {workers.length === 0 && (
          <div className="col-span-full rounded-xl border border-white/10 bg-white/[0.03] p-10 text-center text-slate-500 text-sm">
            No workers connected. Start one with the Go or Python SDK, or run an example worker from <code>examples/</code>.
          </div>
        )}
      </div>
    </div>
  )
}
