// Package aegis is the Go worker SDK: register handlers for job types,
// connect to an Aegis server over its WebSocket dispatch protocol, and let
// the SDK handle registration, concurrency limiting, heartbeats (so
// long-running jobs don't get reclaimed as dead), graceful failure
// reporting, and automatic reconnect with backoff.
//
// Minimal usage:
//
//	w := aegis.NewWorker("ws://localhost:8080/ws/worker", "image-workers")
//	w.Handle("resize_image", func(ctx context.Context, job *aegis.Job) (json.RawMessage, error) {
//	    // ... do work using job.Payload ...
//	    return json.RawMessage(`{"ok":true}`), nil
//	})
//	w.Run(context.Background())
package aegis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Heet852003/aegis/internal/models"
)

// Job is the unit of work handed to a Handler. It is the same shape the
// server persists, so handlers can inspect attempt counts, priority, etc.
type Job = models.Job

// Handler executes one job and returns either a JSON-serializable result or
// an error. Returning an error fails the job, which the server will retry
// with backoff (up to the job's MaxAttempts) before dead-lettering it —
// handlers don't need their own retry logic.
type Handler func(ctx context.Context, job *Job) (json.RawMessage, error)

type wireMsg struct {
	Type        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	Queues      []string        `json:"queues,omitempty"`
	JobTypes    []string        `json:"job_types,omitempty"`
	Concurrency int             `json:"concurrency,omitempty"`
	Count       int             `json:"count,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
	Job         *Job            `json:"job,omitempty"`
	JobID       string          `json:"job_id,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	Message     string          `json:"message,omitempty"`
}

// Worker connects to an Aegis server and executes jobs using registered
// Handlers.
type Worker struct {
	ServerAddr     string // e.g. "ws://localhost:8080/ws/worker"
	Name           string
	Queues         []string      // defaults to ["default"]
	Concurrency    int           // max jobs in flight at once; defaults to 4
	HeartbeatEvery time.Duration // defaults to 15s

	handlers map[string]Handler
	mu       sync.Mutex
}

func NewWorker(serverAddr, name string) *Worker {
	return &Worker{
		ServerAddr:  serverAddr,
		Name:        name,
		Queues:      []string{"default"},
		Concurrency: 4,
		handlers:    make(map[string]Handler),
	}
}

// Handle registers fn as the handler for jobType. Call before Run.
func (w *Worker) Handle(jobType string, fn Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = fn
}

func (w *Worker) jobTypes() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	types := make([]string, 0, len(w.handlers))
	for t := range w.handlers {
		types = append(types, t)
	}
	return types
}

func (w *Worker) handlerFor(jobType string) (Handler, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	h, ok := w.handlers[jobType]
	return h, ok
}

// Run connects and processes jobs until ctx is cancelled, automatically
// reconnecting with backoff if the connection drops.
func (w *Worker) Run(ctx context.Context) error {
	if w.Concurrency <= 0 {
		w.Concurrency = 4
	}
	if w.HeartbeatEvery <= 0 {
		w.HeartbeatEvery = 15 * time.Second
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := w.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("aegis worker disconnected, retrying", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) error {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, w.ServerAddr, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer ws.Close()

	var writeMu sync.Mutex
	send := func(m wireMsg) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return ws.WriteJSON(m)
	}

	if err := send(wireMsg{
		Type:        "register",
		Name:        w.Name,
		Queues:      w.Queues,
		JobTypes:    w.jobTypes(),
		Concurrency: w.Concurrency,
	}); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	active := &sync.Map{} // jobID -> struct{} while running, used for heartbeats
	var inflight sync.WaitGroup

	go func() {
		ticker := time.NewTicker(w.HeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				active.Range(func(k, _ any) bool {
					send(wireMsg{Type: "heartbeat", JobID: k.(string)})
					return true
				})
			}
		}
	}()

	// Ask for our first batch of work immediately.
	if err := send(wireMsg{Type: "request", Count: w.Concurrency}); err != nil {
		return fmt.Errorf("initial request: %w", err)
	}

	for {
		var msg wireMsg
		if err := ws.ReadJSON(&msg); err != nil {
			cancel()
			inflight.Wait()
			return fmt.Errorf("read: %w", err)
		}

		switch msg.Type {
		case "registered":
			slog.Info("aegis worker registered", "worker_id", msg.WorkerID, "name", w.Name)

		case "job":
			job := msg.Job
			handler, ok := w.handlerFor(job.Type)
			if !ok {
				send(wireMsg{Type: "fail", JobID: job.ID, Error: fmt.Sprintf("no handler registered for job type %q", job.Type)})
				send(wireMsg{Type: "request", Count: 1})
				continue
			}
			active.Store(job.ID, struct{}{})
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer active.Delete(job.ID)
				result, err := handler(sessionCtx, job)
				if err != nil {
					send(wireMsg{Type: "fail", JobID: job.ID, Error: err.Error()})
				} else {
					send(wireMsg{Type: "complete", JobID: job.ID, Result: result})
				}
				send(wireMsg{Type: "request", Count: 1})
			}()

		case "error":
			slog.Error("aegis server error", "message", msg.Message)
		}
	}
}
