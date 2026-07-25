package engine

import "sync"

// EventType names the kind of domain event published on the Bus.
type EventType string

const (
	EventJobEnqueued  EventType = "job.enqueued"
	EventJobUpdated   EventType = "job.updated"
	EventWorkflowStep EventType = "workflow.step_updated"
	EventWorkerUpdate EventType = "worker.updated"
	EventStatsTick    EventType = "stats.tick"
)

// Event is a lightweight notification broadcast to subscribers. Payload is
// left as `any` (concrete domain structs from internal/models) so the bus
// stays decoupled from any one consumer's serialization needs.
type Event struct {
	Type    EventType
	Payload any
}

// Bus is a minimal in-process pub/sub used to fan engine state changes out
// to the WebSocket layer (both the worker-dispatch side, which wants to know
// the instant new work lands so it can push to idle workers, and the
// dashboard side, which wants a live feed of job/workflow/worker changes).
//
// It intentionally is not backed by Postgres LISTEN/NOTIFY or Redis pub/sub:
// Aegis runs one active scheduler per cluster (see leader.go), so all state
// mutations already happen in a single process, and an in-memory bus is
// simpler, faster, and has no extra infra dependency for the common case.
type Bus struct {
	mu   sync.RWMutex
	subs map[int]chan Event
	next int
}

func NewBus() *Bus {
	return &Bus{subs: make(map[int]chan Event)}
}

// Subscribe returns a channel of events and an unsubscribe function. The
// channel is buffered; slow consumers drop events rather than blocking
// publishers.
func (b *Bus) Subscribe() (<-chan Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	ch := make(chan Event, 64)
	b.subs[id] = ch
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if c, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(c)
		}
	}
}

func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- e:
		default: // drop for slow/disconnected subscribers instead of blocking the engine
		}
	}
}
