const STYLES: Record<string, string> = {
  pending: 'bg-slate-500/15 text-slate-300 ring-slate-500/30',
  ready: 'bg-slate-500/15 text-slate-300 ring-slate-500/30',
  queued: 'bg-sky-500/15 text-sky-300 ring-sky-500/30',
  running: 'bg-blue-500/15 text-blue-300 ring-blue-500/30',
  succeeded: 'bg-emerald-500/15 text-emerald-300 ring-emerald-500/30',
  failed: 'bg-amber-500/15 text-amber-300 ring-amber-500/30',
  dead_letter: 'bg-red-500/15 text-red-300 ring-red-500/30',
  cancelled: 'bg-zinc-500/15 text-zinc-400 ring-zinc-500/30',
  skipped: 'bg-zinc-500/15 text-zinc-400 ring-zinc-500/30',
}

export default function StatusBadge({ status }: { status: string }) {
  const cls = STYLES[status] ?? 'bg-slate-500/15 text-slate-300 ring-slate-500/30'
  const pulse = status === 'running'
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ring-1 ring-inset ${cls}`}>
      {pulse && <span className="h-1.5 w-1.5 rounded-full bg-blue-400 animate-pulse" />}
      {status.replace('_', ' ')}
    </span>
  )
}
