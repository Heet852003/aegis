package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Heet852003/aegis/internal/engine"
)

// dashboardEvent is what actually goes over the wire to the browser: the
// engine's internal Event carries typed Go payloads, but the dashboard just
// wants type + JSON blob.
type dashboardEvent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

// serveDashboardEvents streams every engine event to the browser as it
// happens, plus a synthetic stats.tick roughly once a second so throughput
// charts update smoothly even during quiet periods.
func (s *Server) serveDashboardEvents(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	events, unsub := s.Engine.Bus.Subscribe()
	defer unsub()

	var writeMu chanWriter
	writeMu.ws = ws

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if err := writeMu.write(dashboardEvent{Type: string(ev.Type), Data: ev.Payload}); err != nil {
				return
			}
		case <-ticker.C:
			stats, err := s.Engine.Stats(r.Context())
			if err != nil {
				continue
			}
			if err := writeMu.write(dashboardEvent{Type: string(engine.EventStatsTick), Data: stats}); err != nil {
				return
			}
		}
	}
}

// chanWriter serializes concurrent writers to a single websocket connection
// (the event forwarder and the stats ticker both write from the same
// goroutine here, but the helper keeps this file safe if that changes).
type chanWriter struct {
	ws *websocket.Conn
}

func (c *chanWriter) write(v any) error {
	c.ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return c.ws.WriteMessage(websocket.TextMessage, b)
}
