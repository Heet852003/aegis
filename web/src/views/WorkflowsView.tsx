import { useEffect, useState } from 'react'
import { Plus } from 'lucide-react'
import { api } from '../api'
import type { Workflow, WorkflowDetail } from '../types'
import StatusBadge from '../components/StatusBadge'
import WorkflowGraph from '../components/WorkflowGraph'
import { useLiveRefetch, useLive } from '../hooks/useLive'
import { timeAgo } from '../lib/time'

const SAMPLE_SPEC = `{
  "name": "etl-pipeline",
  "steps": [
    { "name": "extract", "type": "extract_data", "payload": {} },
    { "name": "transform", "type": "transform_data", "payload": {}, "depends_on": ["extract"] },
    { "name": "load", "type": "load_data", "payload": {}, "depends_on": ["transform"] }
  ]
}`

export default function WorkflowsView() {
  const [workflows, setWorkflows] = useState<Workflow[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [detail, setDetail] = useState<WorkflowDetail | null>(null)
  const [showForm, setShowForm] = useState(false)
  const [error, setError] = useState('')
  const { subscribe } = useLive()

  useLiveRefetch(['workflow.step_updated'], () => {
    api.listWorkflows({ limit: '100' }).then(setWorkflows).catch(() => {})
  })

  useEffect(() => {
    if (!selected) return
    const load = () => api.getWorkflow(selected).then(setDetail).catch((e) => setError(String(e)))
    load()
    return subscribe((ev) => {
      if (ev.type === 'workflow.step_updated') load()
    })
  }, [selected, subscribe])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-slate-100">Workflows</h1>
          <p className="text-sm text-slate-500">DAG runs: steps advance automatically as their dependencies succeed.</p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-1.5 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-medium px-3 py-2"
        >
          <Plus size={15} /> Submit workflow
        </button>
      </div>

      {showForm && (
        <SubmitWorkflowForm
          onSubmitted={(wf) => { setShowForm(false); setSelected(wf.id); api.listWorkflows({ limit: '100' }).then(setWorkflows) }}
          onError={setError}
        />
      )}
      {error && <div className="text-sm text-red-400">{error}</div>}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="rounded-xl border border-white/10 bg-white/[0.03] overflow-hidden lg:col-span-1">
          <div className="px-4 py-3 border-b border-white/10 text-sm font-medium text-slate-300">All workflows</div>
          <ul className="divide-y divide-white/5 max-h-[560px] overflow-auto">
            {workflows.map((w) => (
              <li key={w.id}>
                <button
                  onClick={() => setSelected(w.id)}
                  className={`w-full text-left px-4 py-3 hover:bg-white/[0.03] ${selected === w.id ? 'bg-white/[0.05]' : ''}`}
                >
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-slate-200">{w.name}</span>
                    <StatusBadge status={w.status} />
                  </div>
                  <div className="text-xs text-slate-500 mt-1">{timeAgo(w.updated_at)}</div>
                </button>
              </li>
            ))}
            {workflows.length === 0 && <li className="px-4 py-8 text-center text-slate-500 text-sm">No workflows submitted yet.</li>}
          </ul>
        </div>

        <div className="lg:col-span-2 space-y-3">
          {detail ? (
            <>
              <div className="flex items-center justify-between">
                <h2 className="text-slate-200 font-medium">{detail.workflow.name}</h2>
                <StatusBadge status={detail.workflow.status} />
              </div>
              <WorkflowGraph steps={detail.steps} />
              <div className="rounded-xl border border-white/10 bg-white/[0.03] overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="text-xs uppercase tracking-wide text-slate-500 border-b border-white/10">
                    <tr>
                      <th className="text-left px-4 py-2 font-medium">Step</th>
                      <th className="text-left px-4 py-2 font-medium">Type</th>
                      <th className="text-left px-4 py-2 font-medium">Depends on</th>
                      <th className="text-left px-4 py-2 font-medium">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/5">
                    {detail.steps.map((s) => (
                      <tr key={s.id}>
                        <td className="px-4 py-2 text-slate-200">{s.name}</td>
                        <td className="px-4 py-2 text-slate-400">{s.type}</td>
                        <td className="px-4 py-2 text-slate-500 text-xs">{(s.depends_on ?? []).join(', ') || '—'}</td>
                        <td className="px-4 py-2"><StatusBadge status={s.status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <div className="rounded-xl border border-white/10 bg-white/[0.03] p-10 text-center text-slate-500 text-sm">
              Select a workflow to see its DAG.
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function SubmitWorkflowForm({ onSubmitted, onError }: { onSubmitted: (wf: Workflow) => void; onError: (e: string) => void }) {
  const [spec, setSpec] = useState(SAMPLE_SPEC)
  const [submitting, setSubmitting] = useState(false)

  async function submit() {
    let parsed: unknown
    try {
      parsed = JSON.parse(spec)
    } catch {
      return onError('spec must be valid JSON')
    }
    setSubmitting(true)
    try {
      const wf = await api.submitWorkflow(parsed)
      onSubmitted(wf)
    } catch (e) {
      onError(String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 space-y-2">
      <label className="text-xs text-slate-400">
        Workflow spec (JSON) — or submit YAML from the CLI with <code className="text-slate-300">aegis workflow submit file.yaml</code>
      </label>
      <textarea value={spec} onChange={(e) => setSpec(e.target.value)} rows={10}
        className="w-full bg-white/5 border border-white/10 rounded-lg px-2.5 py-2 text-sm text-slate-200 font-mono" />
      <div className="flex justify-end">
        <button disabled={submitting} onClick={submit}
          className="rounded-lg bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white text-sm font-medium px-4 py-2">
          {submitting ? 'Submitting…' : 'Submit workflow'}
        </button>
      </div>
    </div>
  )
}
