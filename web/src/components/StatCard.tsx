import type { ReactNode } from 'react'

export default function StatCard({
  label,
  value,
  icon,
  tone = 'default',
  sub,
}: {
  label: string
  value: ReactNode
  icon?: ReactNode
  tone?: 'default' | 'good' | 'warn' | 'bad'
  sub?: string
}) {
  const toneCls = {
    default: 'text-slate-100',
    good: 'text-emerald-400',
    warn: 'text-amber-400',
    bad: 'text-red-400',
  }[tone]

  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 flex flex-col gap-1">
      <div className="flex items-center justify-between text-xs uppercase tracking-wide text-slate-400">
        <span>{label}</span>
        {icon}
      </div>
      <div className={`text-2xl font-semibold tabular-nums ${toneCls}`}>{value}</div>
      {sub && <div className="text-xs text-slate-500">{sub}</div>}
    </div>
  )
}
