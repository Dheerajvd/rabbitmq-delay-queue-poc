package jobs

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MinProcessingSeconds = 0
	MaxProcessingSeconds = 30
	MaxMessageRunes      = 4000

	DelayQueue1m      = "poc.delay.1m"
	DelayQueue2m      = "poc.delay.2m"
	DelayQueue5m      = "poc.delay.5m"
	ReadyQueueName    = "poc.ready"
	ReadyExchangeName = "poc.ready.exchange"
	ReadyRoutingKey   = "ready"
)

type Status string

const (
	StatusPublishAttempted  Status = "PUBLISH_ATTEMPTED"
	StatusWaiting           Status = "WAITING"
	StatusProcessing        Status = "PROCESSING"
	StatusCompleted         Status = "COMPLETED"
	StatusPublishFailed     Status = "PUBLISH_FAILED"
	StatusConsumptionFailed Status = "CONSUMPTION_FAILED"
)

const (
	EventPublishAttempted     = "PUBLISH_ATTEMPTED"
	EventPublishConfirmed     = "PUBLISH_CONFIRMED"
	EventPublishFailed        = "PUBLISH_FAILED"
	EventBrokerReady          = "BROKER_READY"
	EventConsumptionStarted   = "CONSUMPTION_STARTED"
	EventConsumptionCompleted = "CONSUMPTION_COMPLETED"
	EventConsumptionFailed    = "CONSUMPTION_FAILED"
)

// Payload is the application body carried through RabbitMQ.
type Payload struct {
	Message string `json:"message"`
}

// Event is one appended lifecycle record. Events are not rewritten.
type Event struct {
	EventID        int64          `json:"event_id"`
	JobID          string         `json:"job_id"`
	EventType      string         `json:"event_type"`
	EventTimestamp time.Time      `json:"event_timestamp"`
	Metadata       map[string]any `json:"metadata"`
}

// Job is the persisted lifecycle of one delayed message.
type Job struct {
	JobID                     string     `json:"job_id"`
	Payload                   Payload    `json:"payload"`
	DelayMinutes              int        `json:"delay_minutes"`
	ProcessingDurationSeconds int        `json:"processing_duration_seconds"`
	DelayQueue                string     `json:"delay_queue"`
	ReadyQueue                string     `json:"ready_queue"`
	Status                    Status     `json:"status"`
	PublishedAt               time.Time  `json:"published_at"`
	PublishConfirmedAt        *time.Time `json:"publish_confirmed_at"`
	ExpectedReadyAt           time.Time  `json:"expected_ready_at"`
	BrokerReadyAt             *time.Time `json:"broker_ready_at"`
	ConsumptionStartedAt      *time.Time `json:"consumption_started_at"`
	ConsumptionCompletedAt    *time.Time `json:"consumption_completed_at"`
	ProcessingDurationMs      *int64     `json:"processing_duration_ms"`
	Error                     *string    `json:"error"`
	BrokerWarning             *string    `json:"broker_warning"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	Events                    []Event    `json:"events,omitempty"`
	Metrics                   *Metrics   `json:"metrics,omitempty"`
}

// ExpectedReadyAt is the visual estimate published_at plus the selected delay.
// It is not a broker observation.
func ExpectedReadyAt(publishedAt time.Time, minutes int) time.Time {
	return publishedAt.UTC().Add(time.Duration(minutes) * time.Minute)
}

// ResolveDelay accepts 1, 2, or 5 minutes. A missing value uses 5 minutes.
func ResolveDelay(requestedMinutes *int) (int, error) {
	minutes := 5
	if requestedMinutes != nil {
		minutes = *requestedMinutes
	}
	if _, err := QueueForDelay(minutes); err != nil {
		return 0, err
	}
	return minutes, nil
}

// QueueForDelay maps the dropdown choice onto one fixed TTL queue.
func QueueForDelay(minutes int) (string, error) {
	switch minutes {
	case 1:
		return DelayQueue1m, nil
	case 2:
		return DelayQueue2m, nil
	case 5:
		return DelayQueue5m, nil
	default:
		return "", fmt.Errorf("delay must be 1, 2, or 5 minutes")
	}
}

// NormalizeProcessing bounds the simulated handler duration.
// The RabbitMQ TTL does not change with this value.
func NormalizeProcessing(requested *int, fallback int) (int, error) {
	value := fallback
	if requested != nil {
		value = *requested
	}
	if value < MinProcessingSeconds || value > MaxProcessingSeconds {
		return 0, fmt.Errorf("processing_duration_seconds must be between %d and %d", MinProcessingSeconds, MaxProcessingSeconds)
	}
	return value, nil
}

// ValidateMessage checks the sample payload text.
func ValidateMessage(message string) error {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return fmt.Errorf("payload.message is required")
	}
	if utf8.RuneCountInString(trimmed) > MaxMessageRunes {
		return fmt.Errorf("payload.message must be at most %d characters", MaxMessageRunes)
	}
	return nil
}

// WithMetrics attaches derived timing figures without changing stored fields.
func WithMetrics(job Job) Job {
	metrics := Derive(job)
	job.Metrics = &metrics
	return job
}
