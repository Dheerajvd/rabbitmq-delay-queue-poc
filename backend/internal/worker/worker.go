package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rabbitmq-delay-poc/backend/internal/jobs"
	"rabbitmq-delay-poc/backend/internal/rabbitmq"
	"rabbitmq-delay-poc/backend/internal/sse"
	"rabbitmq-delay-poc/backend/internal/storage"
)

const (
	defaultMaxAttempts = 3
	reconnectWait      = 2 * time.Second
)

// Message is the persistent JSON body published to poc.delay.
type Message struct {
	JobID                     string       `json:"job_id"`
	Payload                   jobs.Payload `json:"payload"`
	ProcessingDurationSeconds int          `json:"processing_duration_seconds"`
	PublishedAt               time.Time    `json:"published_at"`
}

// Worker consumes poc.ready. RabbitMQ TTL and dead-letter routing decide
// when a delivery arrives. This type does not schedule that delay.
type Worker struct {
	Client       *rabbitmq.Client
	Topology     rabbitmq.Topology
	Store        *storage.Store
	Events       *sse.Broker
	Process      func(context.Context, jobs.Job) error
	AfterReceipt func(jobs.Job)
	MaxAttempts  int

	mu       sync.Mutex
	attempts map[string]int
}

// Run consumes until ctx is cancelled, reconnecting after broker errors.
func (w *Worker) Run(ctx context.Context) {
	if w.MaxAttempts <= 0 {
		w.MaxAttempts = defaultMaxAttempts
	}
	if w.Process == nil {
		w.Process = func(ctx context.Context, job jobs.Job) error {
			return SimulateProcessing(ctx, job.ProcessingDurationSeconds)
		}
	}
	w.mu.Lock()
	if w.attempts == nil {
		w.attempts = map[string]int{}
	}
	w.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return
		}
		err := w.consumeOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("consumer stopped", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectWait):
		}
	}
}

func (w *Worker) consumeOnce(ctx context.Context) error {
	ch, err := w.Client.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(
		w.Topology.ReadyQueue,
		"poc-worker",
		false, // manual acknowledgement
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	slog.Info("consuming", "queue", w.Topology.ReadyQueue)

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			w.handle(ctx, delivery)
		}
	}
}

func (w *Worker) handle(ctx context.Context, delivery amqp.Delivery) {
	var message Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil || message.JobID == "" {
		slog.Error("discarding unreadable delivery", "error", err)
		_ = delivery.Nack(false, false)
		return
	}

	job, err := w.Store.Get(ctx, message.JobID)
	if errors.Is(err, storage.ErrNotFound) {
		slog.Error("discarding delivery with no sqlite row", "job_id", message.JobID)
		_ = delivery.Nack(false, false)
		return
	}
	if err != nil {
		slog.Error("load job", "job_id", message.JobID, "error", err)
		_ = delivery.Nack(false, true)
		return
	}
	if job.Status == jobs.StatusCompleted {
		_ = delivery.Ack(false)
		return
	}
	if job.Status == jobs.StatusConsumptionFailed {
		slog.Info("dropping redelivery of a failed job", "job_id", job.JobID)
		_ = delivery.Ack(false)
		return
	}

	brokerReady, warning, death := rabbitmq.ExtractExpiredDeath(delivery.Headers, job.DelayQueue)
	updated, alreadyComplete, err := w.Store.RecordReceipt(ctx, job.JobID, brokerReady, warning, time.Now().UTC())
	if alreadyComplete {
		_ = delivery.Ack(false)
		return
	}
	if err != nil {
		slog.Error("record receipt", "job_id", job.JobID, "error", err)
		if errors.Is(err, jobs.ErrInvalidTransition) {
			_ = delivery.Nack(false, false)
			return
		}
		_ = delivery.Nack(false, true)
		return
	}
	if warning != "" {
		slog.Warn("dead-letter metadata", "job_id", job.JobID, "warning", warning)
	} else {
		slog.Info("broker dead-letter time", "job_id", job.JobID, "broker_ready_at", updated.BrokerReadyAt, "count", death.Count, "redelivered", delivery.Redelivered)
	}
	w.publish(updated)
	if w.AfterReceipt != nil {
		w.AfterReceipt(updated)
	}

	if err := w.Process(ctx, updated); err != nil {
		if ctx.Err() != nil {
			_ = delivery.Nack(false, true)
			return
		}
		attempt := w.bump(job.JobID)
		if attempt < w.MaxAttempts {
			slog.Warn("processing failed; requeueing", "job_id", job.JobID, "attempt", attempt, "error", err)
			_ = delivery.Nack(false, true)
			return
		}
		failed, markErr := w.Store.MarkConsumptionFailed(ctx, job.JobID, err.Error(), time.Now().UTC())
		if markErr != nil {
			slog.Error("record consumption failure", "job_id", job.JobID, "error", markErr)
			_ = delivery.Nack(false, true)
			return
		}
		w.publish(failed)
		w.clearAttempts(job.JobID)
		// No failure queue is declared. A final nack without requeue drops
		// the message. See docs/ARCHITECTURE.md.
		_ = delivery.Nack(false, false)
		return
	}

	completed, duplicate, err := w.Store.MarkCompleted(ctx, job.JobID, time.Now().UTC())
	if err != nil {
		slog.Error("record completion", "job_id", job.JobID, "error", err)
		_ = delivery.Nack(false, true)
		return
	}
	w.clearAttempts(job.JobID)
	if !duplicate {
		w.publish(completed)
	}
	if err := delivery.Ack(false); err != nil {
		slog.Error("ack after completion", "job_id", job.JobID, "error", err)
	}
}

func (w *Worker) publish(job jobs.Job) {
	if w.Events == nil {
		return
	}
	w.Events.Publish(jobs.WithMetrics(job))
}

func (w *Worker) bump(jobID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.attempts == nil {
		w.attempts = map[string]int{}
	}
	w.attempts[jobID]++
	return w.attempts[jobID]
}

func (w *Worker) clearAttempts(jobID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.attempts, jobID)
}

// SimulateProcessing waits for the job's configured duration.
// It is not the mechanism that delays delivery.
func SimulateProcessing(ctx context.Context, seconds int) error {
	if seconds <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
