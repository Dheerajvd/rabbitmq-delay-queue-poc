package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"rabbitmq-delay-poc/backend/internal/jobs"
	"rabbitmq-delay-poc/backend/internal/sse"
	"rabbitmq-delay-poc/backend/internal/storage"
	"rabbitmq-delay-poc/backend/internal/worker"
)

// Publisher waits for a positive RabbitMQ confirm.
type Publisher interface {
	Publish(ctx context.Context, queue, messageID string, body []byte, publishedAt time.Time) (time.Time, error)
}

// Deps wires HTTP handlers to storage, the broker, and live updates.
type Deps struct {
	Store             *storage.Store
	Publisher         Publisher
	Events            *sse.Broker
	ReadyQueue        string
	DefaultProcessing int
	RabbitReady       func() error
	SQLiteReady       func() error
}

// Server is the HTTP API.
type Server struct {
	deps Deps
	mux  *http.ServeMux
}

func New(deps Deps) *Server {
	if deps.ReadyQueue == "" {
		deps.ReadyQueue = jobs.ReadyQueueName
	}
	if deps.DefaultProcessing == 0 {
		deps.DefaultProcessing = 2
	}
	if deps.Events == nil {
		deps.Events = sse.New()
	}
	s := &Server{deps: deps, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /api/jobs", s.publishJob)
	s.mux.HandleFunc("GET /api/jobs", s.listJobs)
	s.mux.HandleFunc("DELETE /api/jobs", s.deleteJobs)
	s.mux.HandleFunc("GET /api/jobs/{jobID}", s.getJob)
	s.mux.HandleFunc("GET /api/events", s.events)
	return s
}

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.mux.ServeHTTP(w, r)
	})
}

type publishRequest struct {
	ProcessingDurationSeconds *int         `json:"processing_duration_seconds"`
	Payload                   jobs.Payload `json:"payload"`
	DelayMinutes              *int         `json:"delay_minutes"`
}

func (s *Server) publishJob(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	var req publishRequest
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON object")
		return
	}
	minutes, err := jobs.ResolveDelay(req.DelayMinutes)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	delayQueue, err := jobs.QueueForDelay(minutes)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	message := strings.TrimSpace(req.Payload.Message)
	if err := jobs.ValidateMessage(message); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	processing, err := jobs.NormalizeProcessing(req.ProcessingDurationSeconds, s.deps.DefaultProcessing)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	publishedAt := time.Now().UTC()
	job := jobs.Job{
		JobID:                     newJobID(),
		Payload:                   jobs.Payload{Message: message},
		DelayMinutes:              minutes,
		ProcessingDurationSeconds: processing,
		DelayQueue:                delayQueue,
		ReadyQueue:                s.deps.ReadyQueue,
		Status:                    jobs.StatusPublishAttempted,
		PublishedAt:               publishedAt,
		ExpectedReadyAt:           jobs.ExpectedReadyAt(publishedAt, minutes),
		CreatedAt:                 publishedAt,
		UpdatedAt:                 publishedAt,
	}
	if err := s.deps.Store.CreateAttempt(r.Context(), job); err != nil {
		slog.Error("persist publish attempt", "error", err)
		writeError(w, http.StatusInternalServerError, "could not persist job")
		return
	}

	payload, err := json.Marshal(worker.Message{
		JobID:                     job.JobID,
		Payload:                   job.Payload,
		ProcessingDurationSeconds: job.ProcessingDurationSeconds,
		PublishedAt:               job.PublishedAt,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not encode message")
		return
	}

	confirmCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	confirmedAt, err := s.deps.Publisher.Publish(confirmCtx, delayQueue, job.JobID, payload, publishedAt)
	if err != nil {
		failed, markErr := s.deps.Store.MarkPublishFailed(r.Context(), job.JobID, err.Error(), time.Now().UTC())
		if markErr != nil {
			slog.Error("record publish failure", "job_id", job.JobID, "error", markErr)
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		s.deps.Events.Publish(jobs.WithMetrics(failed))
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": err.Error(),
			"job":   jobs.WithMetrics(failed),
		})
		return
	}

	confirmed, err := s.deps.Store.MarkConfirmed(r.Context(), job.JobID, confirmedAt)
	if err != nil {
		slog.Error("record publish confirm", "job_id", job.JobID, "error", err)
		writeError(w, http.StatusInternalServerError, "broker confirmed the publish but the confirmation could not be stored")
		return
	}
	confirmed = jobs.WithMetrics(confirmed)
	s.deps.Events.Publish(confirmed)
	writeJSON(w, http.StatusCreated, confirmed)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !validStatus(jobs.Status(status)) {
		writeError(w, http.StatusBadRequest, "unknown status filter")
		return
	}
	list, err := s.deps.Store.List(r.Context(), status)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	decorated := make([]jobs.Job, 0, len(list))
	for _, job := range list {
		decorated = append(decorated, jobs.WithMetrics(job))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": decorated})
}

func (s *Server) deleteJobs(w http.ResponseWriter, r *http.Request) {
	deleted, err := s.deps.Store.DeleteAll(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not delete jobs")
		return
	}
	s.deps.Events.PublishCleared()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")
	if !validJobID(jobID) {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := s.deps.Store.Get(r.Context(), jobID)
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load job")
		return
	}
	writeJSON(w, http.StatusOK, jobs.WithMetrics(job))
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	updates, cleared, cancel := s.deps.Events.Subscribe()
	defer cancel()

	fmt.Fprintf(w, "event: hello\ndata: {\"status\":\"connected\"}\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-cleared:
			fmt.Fprintf(w, "event: cleared\ndata: {}\n\n")
			flusher.Flush()
		case job := <-updates:
			raw, err := json.Marshal(job)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: job\ndata: %s\n\n", raw)
			flusher.Flush()
		case tick := <-heartbeat.C:
			fmt.Fprintf(w, "event: heartbeat\ndata: {\"at\":%q}\n\n", tick.UTC().Format(time.RFC3339Nano))
			flusher.Flush()
		}
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	report := map[string]string{
		"status":   "ok",
		"backend":  "ok",
		"rabbitmq": "ok",
		"sqlite":   "ok",
	}
	code := http.StatusOK
	if s.deps.SQLiteReady != nil {
		if err := s.deps.SQLiteReady(); err != nil {
			report["sqlite"] = "unavailable"
			report["status"] = "degraded"
			code = http.StatusServiceUnavailable
		}
	}
	if s.deps.RabbitReady != nil {
		if err := s.deps.RabbitReady(); err != nil {
			report["rabbitmq"] = "unavailable"
			report["status"] = "degraded"
			code = http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, report)
}

func validStatus(status jobs.Status) bool {
	switch status {
	case jobs.StatusPublishAttempted, jobs.StatusWaiting, jobs.StatusProcessing,
		jobs.StatusCompleted, jobs.StatusPublishFailed, jobs.StatusConsumptionFailed:
		return true
	default:
		return false
	}
}

func validJobID(id string) bool {
	if len(id) < 8 || len(id) > 80 {
		return false
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r == '-':
		default:
			return false
		}
	}
	return true
}

func newJobID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
