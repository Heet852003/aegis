package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/models"
)

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = (wsPongWait * 9) / 10
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true }, // dashboard/workers may run on a different origin in dev
}

// wireMsg is the envelope for every worker<->server WebSocket message. Only
// the fields relevant to Type are populated.
type wireMsg struct {
	Type        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	Queues      []string        `json:"queues,omitempty"`
	JobTypes    []string        `json:"job_types,omitempty"`
	Concurrency int             `json:"concurrency,omitempty"`
	Count       int             `json:"count,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
	Job         *models.Job     `json:"job,omitempty"`
	JobID       string          `json:"job_id,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	Message     string          `json:"message,omitempty"`
}

// workerConn tracks one connected worker's socket plus the demand/lease
// bookkeeping needed to push new jobs to it the instant they're enqueued.
type workerConn struct {
	id      string
	ws      *websocket.Conn
	writeMu sync.Mutex
	mu      sync.Mutex
	queues  []string
	wanted  int
	current map[string]bool
}

func (c *workerConn) send(msg wireMsg) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return c.ws.WriteJSON(msg)
}

func (c *workerConn) currentJobIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.current))
	for id := range c.current {
		ids = append(ids, id)
	}
	return ids
}

// workerHub tracks all connected workers and reactively pushes newly
// enqueued jobs to any connection with outstanding demand, so a worker that
// says "I have 3 free slots" gets work the instant it's available instead of
// waiting for its next poll.
type workerHub struct {
	engine *engine.Engine
	mu     sync.Mutex
	conns  map[string]*workerConn
}

func newWorkerHub(e *engine.Engine) *workerHub {
	h := &workerHub{engine: e, conns: make(map[string]*workerConn)}
	go h.dispatchLoop(context.Background())
	return h
}

// dispatchLoop wakes on every job-enqueued event (freshly submitted jobs,
// cron firings, reclaimed leases, workflow steps becoming ready) and tries
// to satisfy any worker still waiting on a claim request.
func (h *workerHub) dispatchLoop(ctx context.Context) {
	events, unsub := h.engine.Bus.Subscribe()
	defer unsub()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if ev.Type != engine.EventJobEnqueued {
				continue
			}
			h.fillDemand()
		}
	}
}

func (h *workerHub) fillDemand() {
	h.mu.Lock()
	conns := make([]*workerConn, 0, len(h.conns))
	for _, c := range h.conns {
		conns = append(conns, c)
	}
	h.mu.Unlock()

	for _, c := range conns {
		c.mu.Lock()
		want := c.wanted
		queues := c.queues
		c.mu.Unlock()
		if want <= 0 {
			continue
		}
		jobs, err := h.engine.Claim(context.Background(), queues, c.id, want)
		if err != nil || len(jobs) == 0 {
			continue
		}
		c.mu.Lock()
		c.wanted -= len(jobs)
		for _, j := range jobs {
			c.current[j.ID] = true
		}
		c.mu.Unlock()
		for _, j := range jobs {
			c.send(wireMsg{Type: "job", Job: j})
		}
	}
}

func (h *workerHub) serveWorker(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("worker ws upgrade failed", "error", err)
		return
	}

	conn := &workerConn{id: uuid.NewString(), ws: ws, current: make(map[string]bool)}
	registered := false

	ws.SetReadDeadline(time.Now().Add(wsPongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})

	stopPing := make(chan struct{})
	go func() {
		ticker := time.NewTicker(wsPingPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-ticker.C:
				conn.writeMu.Lock()
				ws.SetWriteDeadline(time.Now().Add(wsWriteWait))
				err := ws.WriteMessage(websocket.PingMessage, nil)
				conn.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	defer func() {
		close(stopPing)
		ws.Close()
		if registered {
			h.mu.Lock()
			delete(h.conns, conn.id)
			h.mu.Unlock()
			h.engine.RemoveWorker(context.Background(), conn.id)
			slog.Info("worker disconnected", "worker_id", conn.id)
		}
	}()

	for {
		var msg wireMsg
		if err := ws.ReadJSON(&msg); err != nil {
			return
		}

		switch msg.Type {
		case "register":
			conn.queues = msg.Queues
			if len(conn.queues) == 0 {
				conn.queues = []string{"default"}
			}
			w := &models.Worker{
				ID:            conn.id,
				Name:          msg.Name,
				Queues:        conn.queues,
				JobTypes:      msg.JobTypes,
				Concurrency:   msg.Concurrency,
				ConnectedAt:   time.Now().UTC(),
				LastHeartbeat: time.Now().UTC(),
			}
			if err := h.engine.RegisterWorker(context.Background(), w); err != nil {
				conn.send(wireMsg{Type: "error", Message: err.Error()})
				continue
			}
			h.mu.Lock()
			h.conns[conn.id] = conn
			h.mu.Unlock()
			registered = true
			slog.Info("worker registered", "worker_id", conn.id, "name", msg.Name, "queues", conn.queues)
			conn.send(wireMsg{Type: "registered", WorkerID: conn.id})

		case "request":
			if !registered {
				conn.send(wireMsg{Type: "error", Message: "must register before requesting jobs"})
				continue
			}
			count := msg.Count
			if count <= 0 {
				count = 1
			}
			jobs, err := h.engine.Claim(context.Background(), conn.queues, conn.id, count)
			if err != nil {
				conn.send(wireMsg{Type: "error", Message: err.Error()})
				continue
			}
			conn.mu.Lock()
			for _, j := range jobs {
				conn.current[j.ID] = true
			}
			remaining := count - len(jobs)
			if remaining > 0 {
				conn.wanted += remaining
			}
			conn.mu.Unlock()
			for _, j := range jobs {
				conn.send(wireMsg{Type: "job", Job: j})
			}

		case "heartbeat":
			h.engine.Heartbeat(context.Background(), msg.JobID, conn.id)
			h.engine.TouchWorker(context.Background(), conn.id, conn.currentJobIDs())

		case "complete":
			conn.mu.Lock()
			delete(conn.current, msg.JobID)
			conn.mu.Unlock()
			if err := h.engine.Complete(context.Background(), msg.JobID, msg.Result); err != nil {
				conn.send(wireMsg{Type: "error", Message: err.Error()})
			}
			h.engine.TouchWorker(context.Background(), conn.id, conn.currentJobIDs())

		case "fail":
			conn.mu.Lock()
			delete(conn.current, msg.JobID)
			conn.mu.Unlock()
			if err := h.engine.Fail(context.Background(), msg.JobID, msg.Error); err != nil {
				conn.send(wireMsg{Type: "error", Message: err.Error()})
			}
			h.engine.TouchWorker(context.Background(), conn.id, conn.currentJobIDs())

		default:
			conn.send(wireMsg{Type: "error", Message: "unknown message type: " + msg.Type})
		}
	}
}
