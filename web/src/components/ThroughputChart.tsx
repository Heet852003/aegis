import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

export interface Point {
  t: number
  throughput: number
  latency: number
}

export default function ThroughputChart({ data }: { data: Point[] }) {
  return (
    <div className="h-56 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
          <defs>
            <linearGradient id="throughput" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#a78bfa" stopOpacity={0.5} />
              <stop offset="95%" stopColor="#a78bfa" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis dataKey="t" hide />
          <YAxis stroke="#64748b" fontSize={11} width={40} />
          <Tooltip
            contentStyle={{ background: '#0d0f16', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 8, fontSize: 12 }}
            labelFormatter={() => ''}
            formatter={(value, name) => {
              const num = typeof value === 'number' ? value : Number(value)
              return [
                name === 'throughput' ? `${num.toFixed(2)} jobs/s` : `${num.toFixed(0)} ms`,
                name === 'throughput' ? 'throughput' : 'avg latency',
              ]
            }}
          />
          <Area type="monotone" dataKey="throughput" stroke="#a78bfa" strokeWidth={2} fill="url(#throughput)" isAnimationActive={false} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
