package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Heet852003/aegis/internal/engine"
	"github.com/Heet852003/aegis/internal/models"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- Jobs ---

type submitJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Queue       string          `json:"queue"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	DelaySec    int             `json:"delay_seconds"`
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Type == "" {
		writeErr(w, http.StatusBadRequest, "type is required")
		return
	}
	scheduledAt := time.Now().UTC()
	if req.DelaySec > 0 {
		scheduledAt = scheduledAt.Add(time.Duration(req.DelaySec) * time.Second)
	}
	job, err := s.Engine.Enqueue(r.Context(), engine.SubmitJobInput{
		Type:        req.Type,
		Payload:     req.Payload,
		Queue:       req.Queue,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	jobs, err := s.Engine.ListJobs(r.Context(), models.JobFilter{
		Status:     models.JobStatus(q.Get("status")),
		Queue:      q.Get("queue"),
		Type:       q.Get("type"),
		WorkflowID: q.Get("workflow_id"),
		Limit:      limit,
		Offset:     offset,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.Engine.GetJob(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.Engine.Cancel(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (s *Server) handleRequeueJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.Engine.RequeueDeadLetter(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "requeued"})
}

// --- Workflows ---

func (s *Server) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	var spec models.WorkflowSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	wf, err := s.Wf.Submit(r.Context(), spec)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, wf)
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	wfs, err := s.Wf.List(r.Context(), limit, offset)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, wfs)
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	wf, steps, err := s.Wf.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "workflow not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"workflow": wf, "steps": steps})
}

// --- Cron ---

func (s *Server) handleUpsertCron(w http.ResponseWriter, r *http.Request) {
	var cs models.CronSchedule
	if err := json.NewDecoder(r.Body).Decode(&cs); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	cs.Enabled = true
	if err := s.Engine.UpsertCron(r.Context(), &cs); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cs)
}

func (s *Server) handleListCron(w http.ResponseWriter, r *http.Request) {
	list, err := s.Engine.Store.ListCronSchedules(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

// --- Workers & stats ---

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.Engine.ListWorkers(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, workers)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.Engine.Stats(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}
