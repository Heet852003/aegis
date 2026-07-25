import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from 'react'
import type { DashboardEvent, Stats } from '../types'

interface LiveContextValue {
  stats: Stats | null
  connected: boolean
  subscribe: (fn: (ev: DashboardEvent) => void) => () => void
}

const LiveContext = createContext<LiveContextValue | null>(null)

// LiveProvider opens a single shared WebSocket to /ws/events and fans
// incoming events out to subscribers, so the whole dashboard reacts to
// server-side state changes in real time without any component polling.
export function LiveProvider({ children }: { children: ReactNode }) {
  const [stats, setStats] = useState<Stats | null>(null)
  const [connected, setConnected] = useState(false)
  const listeners = useRef(new Set<(ev: DashboardEvent) => void>())

  useEffect(() => {
    let ws: WebSocket
    let closed = false
    let retryDelay = 1000

    function connect() {
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
      ws = new WebSocket(`${proto}://${window.location.host}/ws/events`)

      ws.onopen = () => {
        setConnected(true)
        retryDelay = 1000
      }
      ws.onclose = () => {
        setConnected(false)
        if (!closed) {
          setTimeout(connect, retryDelay)
          retryDelay = Math.min(retryDelay * 2, 15000)
        }
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (msg) => {
        try {
          const ev: DashboardEvent = JSON.parse(msg.data)
          if (ev.type === 'stats.tick') {
            setStats(ev.data as Stats)
          }
          listeners.current.forEach((fn) => fn(ev))
        } catch {
          // ignore malformed frames
        }
      }
    }
    connect()
    return () => {
      closed = true
      ws?.close()
    }
  }, [])

  function subscribe(fn: (ev: DashboardEvent) => void) {
    listeners.current.add(fn)
    return () => listeners.current.delete(fn)
  }

  return <LiveContext.Provider value={{ stats, connected, subscribe }}>{children}</LiveContext.Provider>
}

export function useLive() {
  const ctx = useContext(LiveContext)
  if (!ctx) throw new Error('useLive must be used within LiveProvider')
  return ctx
}

// useLiveRefetch re-runs `fn` whenever an event of one of `types` arrives,
// plus once on mount. Handy for tables that should refresh the moment the
// server tells us something relevant changed.
export function useLiveRefetch(types: string[], fn: () => void) {
  const { subscribe } = useLive()
  const fnRef = useRef(fn)
  fnRef.current = fn

  useEffect(() => {
    fnRef.current()
    return subscribe((ev) => {
      if (types.includes(ev.type)) fnRef.current()
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [types.join(',')])
}
