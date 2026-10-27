package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestResolveDelay(t *testing.T) {
	minutes, err := ResolveDelay(nil)
	if err != nil || minutes != 5 {
		t.Fatalf("default: %d %v", minutes, err)
	}
	for _, allowed := range []int{1, 2, 5} {
		value := allowed
		got, err := ResolveDelay(&value)
		if err != nil || got != allowed {
			t.Fatalf("%d: %d %v", allowed, got, err)
		}
		queue, err := QueueForDelay(allowed)
		if err != nil {
			t.Fatal(err)
		}
		want := map[int]string{1: DelayQueue1m, 2: DelayQueue2m, 5: DelayQueue5m}[allowed]
		if queue != want {
			t.Fatalf("queue for %d = %s", allowed, queue)
		}
	}
	other := 3
	if _, err := ResolveDelay(&other); err == nil {
		t.Fatal("expected a delay other than 1, 2, or 5 minutes to be rejected")
	}
}

func TestExpectedReadyAtUsesSelectedDelay(t *testing.T) {
	published := time.Date(2026, 10, 12, 6, 30, 15, 123456789, time.FixedZone("IST", 5*3600+1800))
	got := ExpectedReadyAt(published, 2)
	want := published.UTC().Add(2 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("expected %s, got %s", want, got)
	}
	if got.Location() != time.UTC {
		t.Fatalf("expected UTC, got %s", got.Location())
	}
	if ExpectedReadyAt(published, 1).Sub(published.UTC()) != time.Minute {
		t.Fatal("one minute delay was not applied")
	}
	if ExpectedReadyAt(published, 5).Sub(published.UTC()) != 5*time.Minute {
		t.Fatal("five minute delay was not applied")
	}
}

func TestNormalizeProcessing(t *testing.T) {
	got, err := NormalizeProcessing(nil, 2)
	if err != nil || got != 2 {
		t.Fatalf("fallback: %d %v", got, err)
	}
	zero := 0
	got, err = NormalizeProcessing(&zero, 2)
	if err != nil || got != 0 {
		t.Fatalf("zero: %d %v", got, err)
	}
	high := 31
	if _, err := NormalizeProcessing(&high, 2); err == nil {
		t.Fatal("expected upper bound error")
	}
	low := -1
	if _, err := NormalizeProcessing(&low, 2); err == nil {
		t.Fatal("expected lower bound error")
	}
}

func TestValidateMessage(t *testing.T) {
	if err := ValidateMessage("  Sample WhatsApp message  "); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMessage("   "); err == nil {
		t.Fatal("expected empty message to fail")
	}
	if err := ValidateMessage(strings.Repeat("a", MaxMessageRunes+1)); err == nil {
		t.Fatal("expected long message to fail")
	}
}

func TestTimestampJSONUsesRFC3339NanoAndNulls(t *testing.T) {
	published := time.Date(2026, 10, 12, 6, 30, 15, 123456789, time.UTC)
	confirmed := published.Add(20 * time.Millisecond)
	job := Job{
		JobID:                     "11111111-1111-4111-8111-111111111111",
		Payload:                   Payload{Message: "Sample WhatsApp message"},
		DelayMinutes:              5,
		ProcessingDurationSeconds: 2,
		DelayQueue:                DelayQueue5m,
		ReadyQueue:                ReadyQueueName,
		Status:                    StatusWaiting,
		PublishedAt:               published,
		PublishConfirmedAt:        &confirmed,
		ExpectedReadyAt:           ExpectedReadyAt(published, 5),
		CreatedAt:                 published,
		UpdatedAt:                 published,
	}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, published.Format(time.RFC3339Nano)) {
		t.Fatalf("published_at was not RFC3339Nano: %s", text)
	}
	if !strings.Contains(text, `"broker_ready_at":null`) {
		t.Fatalf("missing null broker_ready_at: %s", text)
	}
	if !strings.Contains(text, `"consumption_started_at":null`) {
		t.Fatalf("missing null consumption start: %s", text)
	}
	if !strings.Contains(text, `"error":null`) {
		t.Fatalf("missing null error: %s", text)
	}

	var decoded Job
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.PublishedAt.Equal(published) {
		t.Fatalf("parsed published_at %s", decoded.PublishedAt)
	}
	if decoded.PublishConfirmedAt == nil || !decoded.PublishConfirmedAt.Equal(confirmed) {
		t.Fatalf("parsed confirm %#v", decoded.PublishConfirmedAt)
	}
	if decoded.BrokerReadyAt != nil {
		t.Fatal("broker time should stay null")
	}
}

func TestDeriveMetrics(t *testing.T) {
	published := time.Date(2026, 10, 12, 6, 0, 0, 0, time.UTC)
	confirmed := published.Add(10 * time.Millisecond)
	expected := ExpectedReadyAt(published, 5)
	broker := expected.Add(1500 * time.Millisecond)
	started := broker.Add(200 * time.Millisecond)
	completed := started.Add(2 * time.Second)
	processing := int64(2000)
	job := Job{
		DelayMinutes:           5,
		PublishedAt:            published,
		PublishConfirmedAt:     &confirmed,
		ExpectedReadyAt:        expected,
		BrokerReadyAt:          &broker,
		ConsumptionStartedAt:   &started,
		ConsumptionCompletedAt: &completed,
		ProcessingDurationMs:   &processing,
	}
	metrics := Derive(job)
	if metrics.RequestedDelayMs != 300000 {
		t.Fatalf("requested delay %d", metrics.RequestedDelayMs)
	}
	if metrics.PublishAttemptToConsumptionStartMs == nil || *metrics.PublishAttemptToConsumptionStartMs != started.Sub(published).Milliseconds() {
		t.Fatalf("publish to start %#v", metrics.PublishAttemptToConsumptionStartMs)
	}
	if metrics.ExpectedReadyVsConsumerStartMs == nil || *metrics.ExpectedReadyVsConsumerStartMs != started.Sub(expected).Milliseconds() {
		t.Fatalf("expected vs start %#v", metrics.ExpectedReadyVsConsumerStartMs)
	}
	if metrics.BrokerDeadLetterVsConsumerStartMs == nil || *metrics.BrokerDeadLetterVsConsumerStartMs != 200 {
		t.Fatalf("broker vs start %#v", metrics.BrokerDeadLetterVsConsumerStartMs)
	}
	if metrics.ProcessingDurationMs == nil || *metrics.ProcessingDurationMs != 2000 {
		t.Fatalf("processing %#v", metrics.ProcessingDurationMs)
	}
	if metrics.PublishAttemptToCompletionMs == nil || *metrics.PublishAttemptToCompletionMs != completed.Sub(published).Milliseconds() {
		t.Fatalf("total %#v", metrics.PublishAttemptToCompletionMs)
	}
}

func TestDeriveOmitsUnobservedMetrics(t *testing.T) {
	job := Job{
		DelayMinutes:    5,
		PublishedAt:     time.Date(2026, 10, 12, 6, 0, 0, 0, time.UTC),
		ExpectedReadyAt: time.Date(2026, 10, 12, 6, 5, 0, 0, time.UTC),
		Status:          StatusWaiting,
	}
	metrics := Derive(job)
	if metrics.PublishAttemptToConsumptionStartMs != nil || metrics.BrokerDeadLetterVsConsumerStartMs != nil || metrics.PublishAttemptToCompletionMs != nil {
		t.Fatalf("waiting job should not invent durations: %+v", metrics)
	}
}

func TestTransitions(t *testing.T) {
	valid := [][2]Status{
		{StatusPublishAttempted, StatusWaiting},
		{StatusPublishAttempted, StatusPublishFailed},
		{StatusPublishAttempted, StatusProcessing},
		{StatusWaiting, StatusProcessing},
		{StatusProcessing, StatusCompleted},
		{StatusProcessing, StatusConsumptionFailed},
		{StatusProcessing, StatusProcessing},
	}
	for _, pair := range valid {
		if err := ValidateTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("%s -> %s: %v", pair[0], pair[1], err)
		}
	}
	invalid := [][2]Status{
		{StatusPublishAttempted, StatusCompleted},
		{StatusWaiting, StatusCompleted},
		{StatusCompleted, StatusProcessing},
		{StatusPublishFailed, StatusWaiting},
		{StatusConsumptionFailed, StatusCompleted},
		{StatusCompleted, StatusCompleted},
	}
	for _, pair := range invalid {
		err := ValidateTransition(pair[0], pair[1])
		if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("%s -> %s error = %v", pair[0], pair[1], err)
		}
	}
}
