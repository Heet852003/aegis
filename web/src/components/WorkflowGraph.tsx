import { useMemo } from 'react'
import type { WorkflowStep } from '../types'

const NODE_W = 168
const NODE_H = 56
const COL_GAP = 96
const ROW_GAP = 24

const STATUS_COLORS: Record<string, { fill: string; stroke: string; text: string }> = {
  pending: { fill: '#1e2230', stroke: '#3a3f52', text: '#94a3b8' },
  ready: { fill: '#1e2230', stroke: '#3a3f52', text: '#94a3b8' },
  queued: { fill: '#0c2b45', stroke: '#1d68a6', text: '#7dd3fc' },
  succeeded: { fill: '#0b2e22', stroke: '#1d8a5f', text: '#6ee7b7' },
  failed: { fill: '#3a1f13', stroke: '#b4640f', text: '#fcd34d' },
  skipped: { fill: '#221f28', stroke: '#3a3648', text: '#a1a1aa' },
}

// Layered layout: each step's column = 1 + max(column of its dependencies),
// independent steps sharing a column stack vertically. This mirrors the
// same topological structure the server validates on submit, just rendered.
function layout(steps: WorkflowStep[]) {
  const byName = new Map(steps.map((s) => [s.name, s]))
  const colOf = new Map<string, number>()

  function colFor(name: string, seen: Set<string>): number {
    if (colOf.has(name)) return colOf.get(name)!
    if (seen.has(name)) return 0 // guard against unexpected cycles in malformed data
    seen.add(name)
    const step = byName.get(name)
    const deps = step?.depends_on ?? []
    const col = deps.length === 0 ? 0 : 1 + Math.max(...deps.map((d) => colFor(d, seen)))
    colOf.set(name, col)
    return col
  }
  steps.forEach((s) => colFor(s.name, new Set()))

  const columns = new Map<number, WorkflowStep[]>()
  steps.forEach((s) => {
    const c = colOf.get(s.name)!
    if (!columns.has(c)) columns.set(c, [])
    columns.get(c)!.push(s)
  })

  const positions = new Map<string, { x: number; y: number }>()
  columns.forEach((colSteps, col) => {
    const totalHeight = colSteps.length * NODE_H + (colSteps.length - 1) * ROW_GAP
    colSteps.forEach((s, i) => {
      positions.set(s.name, {
        x: col * (NODE_W + COL_GAP),
        y: i * (NODE_H + ROW_GAP) - totalHeight / 2,
      })
    })
  })

  const maxCol = Math.max(0, ...Array.from(columns.keys()))
  const width = (maxCol + 1) * NODE_W + maxCol * COL_GAP
  const maxRows = Math.max(1, ...Array.from(columns.values()).map((c) => c.length))
  const height = maxRows * NODE_H + (maxRows - 1) * ROW_GAP + 40

  return { positions, width, height }
}

export default function WorkflowGraph({ steps }: { steps: WorkflowStep[] }) {
  const { positions, width, height } = useMemo(() => layout(steps), [steps])

  return (
    <div className="overflow-auto rounded-xl border border-white/10 bg-white/[0.02] p-6">
      <svg width={width} height={height} style={{ overflow: 'visible' }}>
        <g transform={`translate(0, ${height / 2})`}>
          {steps.map((s) =>
            (s.depends_on ?? []).map((dep) => {
              const from = positions.get(dep)
              const to = positions.get(s.name)
              if (!from || !to) return null
              const x1 = from.x + NODE_W
              const y1 = from.y + NODE_H / 2
              const x2 = to.x
              const y2 = to.y + NODE_H / 2
              const midX = (x1 + x2) / 2
              return (
                <path
                  key={`${dep}->${s.name}`}
                  d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
                  fill="none"
                  stroke="#3a3f52"
                  strokeWidth={1.5}
                />
              )
            }),
          )}
          {steps.map((s) => {
            const pos = positions.get(s.name)!
            const colors = STATUS_COLORS[s.status] ?? STATUS_COLORS.pending
            return (
              <g key={s.id} transform={`translate(${pos.x}, ${pos.y})`}>
                <rect
                  width={NODE_W}
                  height={NODE_H}
                  rx={10}
                  fill={colors.fill}
                  stroke={colors.stroke}
                  strokeWidth={1.5}
                />
                <text x={12} y={22} fill="#e5e7eb" fontSize={13} fontWeight={600}>
                  {s.name.length > 18 ? s.name.slice(0, 17) + '…' : s.name}
                </text>
                <text x={12} y={40} fill={colors.text} fontSize={11}>
                  {s.status}
                </text>
              </g>
            )
          })}
        </g>
      </svg>
    </div>
  )
}
