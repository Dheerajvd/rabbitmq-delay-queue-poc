package jobs

import "time"

// Metrics are derived from stored timestamps. Expected-ready figures are
// estimates. Broker and consumer figures are observations.
type Metrics struct {
	RequestedDelayMs                   int64  `json:"requested_delay_ms"`
	PublishAttemptToConsumptionStartMs *int64 `json:"publish_attempt_to_consumption_start_ms"`
	ExpectedReadyVsConsumerStartMs     *int64 `json:"expected_ready_vs_consumer_start_ms"`
	BrokerDeadLetterVsConsumerStartMs  *int64 `json:"broker_dead_letter_vs_consumer_start_ms"`
	ProcessingDurationMs               *int64 `json:"processing_duration_ms"`
	PublishAttemptToCompletionMs       *int64 `json:"publish_attempt_to_completion_ms"`
}

// Derive computes durations in milliseconds.
//
// ExpectedReadyVsConsumerStartMs is consumer start minus expected ready.
// A positive value means the consumer started after the estimate.
// BrokerDeadLetterVsConsumerStartMs is consumer start minus the x-death time.
func Derive(job Job) Metrics {
	metrics := Metrics{
		RequestedDelayMs: int64(job.DelayMinutes) * int64(time.Minute/time.Millisecond),
	}
	if job.DelayMinutes == 0 {
		metrics.RequestedDelayMs = 5 * int64(time.Minute/time.Millisecond)
	}
	if job.ConsumptionStartedAt != nil {
		start := durationMs(*job.ConsumptionStartedAt, job.PublishedAt)
		metrics.PublishAttemptToConsumptionStartMs = &start
		expected := durationMs(*job.ConsumptionStartedAt, job.ExpectedReadyAt)
		metrics.ExpectedReadyVsConsumerStartMs = &expected
	}
	if job.ConsumptionStartedAt != nil && job.BrokerReadyAt != nil {
		delta := durationMs(*job.ConsumptionStartedAt, *job.BrokerReadyAt)
		metrics.BrokerDeadLetterVsConsumerStartMs = &delta
	}
	if job.ProcessingDurationMs != nil {
		value := *job.ProcessingDurationMs
		metrics.ProcessingDurationMs = &value
	} else if job.ConsumptionStartedAt != nil && job.ConsumptionCompletedAt != nil {
		value := durationMs(*job.ConsumptionCompletedAt, *job.ConsumptionStartedAt)
		metrics.ProcessingDurationMs = &value
	}
	if job.ConsumptionCompletedAt != nil {
		total := durationMs(*job.ConsumptionCompletedAt, job.PublishedAt)
		metrics.PublishAttemptToCompletionMs = &total
	}
	return metrics
}

func durationMs(later, earlier time.Time) int64 {
	return later.Sub(earlier).Milliseconds()
}
