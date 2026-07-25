import { useState } from 'react'
import ErrorBoundary from './components/ErrorBoundary'
import Sidebar, { type View } from './components/Sidebar'
import { LiveProvider, useLive } from './hooks/useLive'
import OverviewView from './views/OverviewView'
import JobsView from './views/JobsView'
import WorkflowsView from './views/WorkflowsView'
import WorkersView from './views/WorkersView'
import DeadLetterView from './views/DeadLetterView'
import CronView from './views/CronView'

function Shell() {
  const [view, setView] = useState<View>('overview')
  const { connected } = useLive()

  return (
    <div className="flex h-screen bg-[#0b0d12] text-slate-200">
      <Sidebar view={view} onChange={setView} connected={connected} />
      <main className="flex-1 overflow-auto p-6">
        <ErrorBoundary>
          {view === 'overview' && <OverviewView />}
          {view === 'jobs' && <JobsView />}
          {view === 'workflows' && <WorkflowsView />}
          {view === 'workers' && <WorkersView />}
          {view === 'dead-letter' && <DeadLetterView />}
          {view === 'cron' && <CronView />}
        </ErrorBoundary>
      </main>
    </div>
  )
}

export default function App() {
  return (
    <LiveProvider>
      <Shell />
    </LiveProvider>
  )
}
