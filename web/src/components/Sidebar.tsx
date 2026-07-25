import {
  LayoutDashboard,
  ListTodo,
  GitBranch,
  Users,
  Skull,
  Clock,
} from 'lucide-react'

export type View = 'overview' | 'jobs' | 'workflows' | 'workers' | 'dead-letter' | 'cron'

const ITEMS: { id: View; label: string; icon: React.ElementType }[] = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'jobs', label: 'Jobs', icon: ListTodo },
  { id: 'workflows', label: 'Workflows', icon: GitBranch },
  { id: 'workers', label: 'Workers', icon: Users },
  { id: 'dead-letter', label: 'Dead Letter', icon: Skull },
  { id: 'cron', label: 'Cron', icon: Clock },
]

export default function Sidebar({
  view,
  onChange,
  connected,
}: {
  view: View
  onChange: (v: View) => void
  connected: boolean
}) {
  return (
    <aside className="w-60 shrink-0 border-r border-white/10 bg-[#0d0f16] flex flex-col">
      <div className="px-5 py-5 flex items-center gap-2 border-b border-white/10">
        <img src="/logo.svg" alt="" width={28} height={28} />
        <div>
          <div className="font-semibold text-slate-100 leading-tight">Aegis</div>
          <div className="text-[11px] text-slate-500 leading-tight">job orchestration</div>
        </div>
      </div>
      <nav className="flex-1 py-3 px-2 space-y-0.5">
        {ITEMS.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => onChange(id)}
            className={`w-full flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors ${
              view === id
                ? 'bg-violet-500/15 text-violet-200'
                : 'text-slate-400 hover:bg-white/5 hover:text-slate-200'
            }`}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </nav>
      <div className="px-4 py-3 border-t border-white/10 flex items-center gap-2 text-xs text-slate-500">
        <span className={`h-2 w-2 rounded-full ${connected ? 'bg-emerald-400' : 'bg-red-500'}`} />
        {connected ? 'live' : 'disconnected'}
      </div>
    </aside>
  )
}
