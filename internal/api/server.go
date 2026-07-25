// Package api exposes Aegis over HTTP: a JSON REST API for job/workflow/cron
// management, a WebSocket endpoint workers connect to for real-time job
// dispatch, and a WebSocket endpoint the dashboard uses for a live event
// feed. It is a thin transport layer — all policy lives in internal/engine
// and internal/workflow.
package api

import (
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/workflow"
)

type Server struct {
	Engine *engine.Engine
	Wf     *workflow.Coordinator
	hub    *workerHub
	router chi.Router
	distFS fs.FS // built dashboard assets, may be nil in dev
}

// NewServer wires the router. distFS, if non-nil, is served at "/" (the
// production build of web/); in development the dashboard runs from its own
// Vite dev server and talks to the API via CORS instead.
func NewServer(e *engine.Engine, wf *workflow.Coordinator, distFS fs.FS) *Server {
	s := &Server{Engine: e, Wf: wf, hub: newWorkerHub(e), distFS: distFS}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.router }

func (s *Server) routes() {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(loggingMiddleware)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/jobs", s.handleSubmitJob)
		r.Get("/jobs", s.handleListJobs)
		r.Get("/jobs/{id}", s.handleGetJob)
		r.Post("/jobs/{id}/cancel", s.handleCancelJob)
		r.Post("/jobs/{id}/requeue", s.handleRequeueJob)

		r.Post("/workflows", s.handleSubmitWorkflow)
		r.Get("/workflows", s.handleListWorkflows)
		r.Get("/workflows/{id}", s.handleGetWorkflow)

		r.Post("/cron", s.handleUpsertCron)
		r.Get("/cron", s.handleListCron)

		r.Get("/workers", s.handleListWorkers)
		r.Get("/stats", s.handleStats)
	})

	r.Get("/ws/worker", s.hub.serveWorker)
	r.Get("/ws/events", s.serveDashboardEvents)

	if s.distFS != nil {
		fileServer := http.FileServer(http.FS(s.distFS))
		r.Handle("/*", spaHandler(s.distFS, fileServer))
	}

	s.router = r
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		slog.Info("http", "method", r.Method, "path", r.URL.Path, "status", ww.Status(), "dur", time.Since(start))
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// spaHandler serves the built React app, falling back to index.html for any
// path that isn't a real static file so client-side routing works.
func spaHandler(distFS fs.FS, fileServer http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path != "/" {
			if _, err := fs.Stat(distFS, path[1:]); err == nil {
				fileServer.ServeHTTP(w, r)
				return
			}
		}
		r2 := new(http.Request)
		*r2 = *r
		r2.URL.Path = "/"
		fileServer.ServeHTTP(w, r2)
	}
}
