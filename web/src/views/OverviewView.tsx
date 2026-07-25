import { useEffect, useState } from 'react'
import { Activity, Clock3, Skull, Users } from 'lucide-react'
import { useLive } from '../hooks/useLive'
import StatCard from '../components/StatCard'
import ThroughputChart, { type Point } from '../components/ThroughputChart'
import StatusBadge from '../components/StatusBadge'
import { api } from '../api'
import type { Job } from '../types'
import { timeAgo } from '../lib/time'

const HISTORY_LEN = 60

export default function OverviewView() {
  const { stats } = useLive()
  const [history, setHistory] = useState<Point[]>([])
  const [recent, setRecent] = useState<Job[]>([])

  useEffect(() => {
    if (!stats) return
    setHistory((h) => [...h, { t: Date.now(), throughput: stats.throughput_1m, latency: stats.avg_latency_ms }].slice(-HISTORY_LEN))
  }, [stats])

  useEffect(() => {
    const load = () => api.listJobs({ limit: '8' }).then(setRecent).catch(() => {})
    load()
    const id = setInterval(load, 4000)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-slate-100">Overview</h1>
        <p className="text-sm text-slate-500">Live queue health across every worker connected to this server.</p>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Pending" value={stats?.pending ?? '—'} icon={<Clock3 size={14} />} />
        <StatCard label="Running" value={stats?.running ?? '—'} tone="good" icon={<Activity size={14} />} />
        <StatCard label="Dead letter" value={stats?.dead_letter ?? '—'} tone={stats && stats.dead_letter > 0 ? 'bad' : 'default'} icon={<Skull size={14} />} />
        <StatCard label="Workers online" value={stats?.workers ?? '—'} icon={<Users size={14} />} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 rounded-xl border border-white/10 bg-white/[0.03] p-4">
          <div className="flex items-baseline justify-between mb-2">
            <h2 className="text-sm font-medium text-slate-300">Throughput (jobs/sec, trailing 1m)</h2>
            <span className="text-xs text-slate-500">{stats ? `${stats.throughput_1m.toFixed(2)} jobs/s` : ''}</span>
          </div>
          <ThroughputChart data={history} />
        </div>
        <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 space-y-3">
          <h2 className="text-sm font-medium text-slate-300">Last 24h</h2>
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">Succeeded</span>
            <span className="text-emerald-400 font-medium tabular-nums">{stats?.succeeded_24h ?? '—'}</span>
          </div>
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">Failed</span>
            <span className="text-amber-400 font-medium tabular-nums">{stats?.failed_24h ?? '—'}</span>
          </div>
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">Avg latency</span>
            <span className="font-medium tabular-nums">{stats ? `${stats.avg_latency_ms.toFixed(0)} ms` : '—'}</span>
          </div>
          <div className="flex items-center justify-between text-sm">
            <span className="text-slate-400">p95 latency</span>
            <span className="font-medium tabular-nums">{stats ? `${stats.p95_latency_ms.toFixed(0)} ms` : '—'}</span>
          </div>
        </div>
      </div>

      <div className="rounded-xl border border-white/10 bg-white/[0.03] overflow-hidden">
        <div className="px-4 py-3 border-b border-white/10 text-sm font-medium text-slate-300">Recent jobs</div>
        <table className="w-full text-sm">
          <tbody className="divide-y divide-white/5">
            {recent.map((j) => (
              <tr key={j.id} className="hover:bg-white/[0.02]">
                <td className="px-4 py-2 font-mono text-xs text-slate-500">{j.id.slice(0, 8)}</td>
                <td className="px-4 py-2 text-slate-200">{j.type}</td>
                <td className="px-4 py-2"><StatusBadge status={j.status} /></td>
                <td className="px-4 py-2 text-slate-500 text-xs">{timeAgo(j.updated_at)}</td>
              </tr>
            ))}
            {recent.length === 0 && (
              <tr><td className="px-4 py-6 text-center text-slate-500" colSpan={4}>No jobs yet — submit one from the Jobs tab.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
