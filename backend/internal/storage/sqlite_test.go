package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"rabbitmq-delay-poc/backend/internal/jobs"
)

func TestLifecyclePersistenceAndDuplicateDelivery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	published := time.Date(2026, 10, 12, 6, 0, 0, 123456789, time.UTC)
	job := sampleJob(published)
	if err := store.CreateAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}

	confirmedAt := published.Add(25 * time.Millisecond)
	confirmed, err := store.MarkConfirmed(ctx, job.JobID, confirmedAt)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.Status != jobs.StatusWaiting || confirmed.PublishConfirmedAt == nil || !confirmed.PublishConfirmedAt.Equal(confirmedAt) {
		t.Fatalf("confirm = %+v", confirmed)
	}

	broker := published.Add(5 * time.Minute).Add(time.Second)
	started := broker.Add(300 * time.Millisecond)
	receipt, duplicate, err := store.RecordReceipt(ctx, job.JobID, &broker, "", started)
	if err != nil || duplicate {
		t.Fatalf("receipt duplicate=%v err=%v", duplicate, err)
	}
	if receipt.BrokerReadyAt == nil || !receipt.BrokerReadyAt.Equal(broker.UTC()) {
		t.Fatalf("broker time %+v", receipt.BrokerReadyAt)
	}
	if receipt.ConsumptionStartedAt == nil || !receipt.ConsumptionStartedAt.Equal(started.UTC()) {
		t.Fatalf("start %+v", receipt.ConsumptionStartedAt)
	}
	if receipt.Status != jobs.StatusProcessing {
		t.Fatal(receipt.Status)
	}

	laterBroker := broker.Add(time.Minute)
	laterStart := started.Add(time.Minute)
	again, duplicate, err := store.RecordReceipt(ctx, job.JobID, &laterBroker, "", laterStart)
	if err != nil || duplicate {
		t.Fatalf("redelivery duplicate=%v err=%v", duplicate, err)
	}
	if !again.BrokerReadyAt.Equal(broker.UTC()) || !again.ConsumptionStartedAt.Equal(started.UTC()) {
		t.Fatalf("timestamps overwritten: broker %s start %s", again.BrokerReadyAt, again.ConsumptionStartedAt)
	}
	if countEvents(t, again, jobs.EventBrokerReady) != 1 || countEvents(t, again, jobs.EventConsumptionStarted) != 1 {
		t.Fatalf("duplicate receipt events: %+v", again.Events)
	}

	completedAt := started.Add(2 * time.Second)
	done, duplicate, err := store.MarkCompleted(ctx, job.JobID, completedAt)
	if err != nil || duplicate {
		t.Fatalf("complete duplicate=%v err=%v", duplicate, err)
	}
	if done.Status != jobs.StatusCompleted || done.ConsumptionCompletedAt == nil || !done.ConsumptionCompletedAt.Equal(completedAt) {
		t.Fatalf("completed %+v", done)
	}
	if done.ProcessingDurationMs == nil || *done.ProcessingDurationMs != completedAt.Sub(started).Milliseconds() {
		t.Fatalf("duration %+v", done.ProcessingDurationMs)
	}
	second, duplicate, err := store.MarkCompleted(ctx, job.JobID, completedAt.Add(time.Second))
	if err != nil || !duplicate {
		t.Fatalf("second complete duplicate=%v err=%v", duplicate, err)
	}
	if !second.ConsumptionCompletedAt.Equal(completedAt) || countEvents(t, second, jobs.EventConsumptionCompleted) != 1 {
		t.Fatalf("completion history changed: %+v", second)
	}

	reopenPath := storePath(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(reopenPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Get(ctx, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.PublishedAt.Equal(published) || !loaded.PublishConfirmedAt.Equal(confirmedAt) || !loaded.BrokerReadyAt.Equal(broker.UTC()) {
		t.Fatalf("reopened timestamps published=%s confirmed=%s broker=%s", loaded.PublishedAt, loaded.PublishConfirmedAt, loaded.BrokerReadyAt)
	}
	if loaded.Status != jobs.StatusCompleted || countEvents(t, loaded, jobs.EventConsumptionCompleted) != 1 {
		t.Fatalf("reopened history %+v", loaded)
	}
}

func TestMissingDeathMetadataDoesNotInventBrokerTime(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	job := sampleJob(time.Date(2026, 10, 12, 7, 0, 0, 0, time.UTC))
	if err := store.CreateAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkConfirmed(ctx, job.JobID, job.PublishedAt.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.RecordReceipt(ctx, job.JobID, nil, "x-death header missing", job.PublishedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.BrokerReadyAt != nil {
		t.Fatalf("broker time = %s", got.BrokerReadyAt)
	}
	if got.BrokerWarning == nil || *got.BrokerWarning == "" {
		t.Fatal("expected warning")
	}
	if got.ConsumptionStartedAt == nil {
		t.Fatal("expected consumer start")
	}
}

func TestPublishFailureDoesNotConfirm(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	job := sampleJob(time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC))
	if err := store.CreateAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}
	failed, err := store.MarkPublishFailed(ctx, job.JobID, "broker nack", job.PublishedAt.Add(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != jobs.StatusPublishFailed || failed.PublishConfirmedAt != nil {
		t.Fatalf("failed job %+v", failed)
	}
	if failed.Error == nil || *failed.Error == "" {
		t.Fatal("expected error")
	}
	if countEvents(t, failed, jobs.EventPublishFailed) != 1 || countEvents(t, failed, jobs.EventPublishConfirmed) != 0 {
		t.Fatalf("events %+v", failed.Events)
	}
}

func TestListNewestFirstAndFilter(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	older := sampleJob(time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC))
	older.JobID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	newer := sampleJob(time.Date(2026, 10, 12, 2, 0, 0, 0, time.UTC))
	newer.JobID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	if err := store.CreateAttempt(ctx, older); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateAttempt(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkConfirmed(ctx, newer.JobID, newer.PublishedAt); err != nil {
		t.Fatal(err)
	}
	list, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].JobID != newer.JobID {
		t.Fatalf("order %+v", list)
	}
	waiting, err := store.List(ctx, string(jobs.StatusWaiting))
	if err != nil {
		t.Fatal(err)
	}
	if len(waiting) != 1 || waiting[0].JobID != newer.JobID {
		t.Fatalf("filter %+v", waiting)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poc.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func storePath(t *testing.T, store *Store) string {
	t.Helper()
	var path string
	if err := store.db.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &path); err != nil {
		t.Fatal(err)
	}
	return path
}

func sampleJob(published time.Time) jobs.Job {
	return jobs.Job{
		JobID:                     "11111111-1111-4111-8111-111111111111",
		Payload:                   jobs.Payload{Message: "Sample WhatsApp message"},
		DelayMinutes:              5,
		ProcessingDurationSeconds: 2,
		DelayQueue:                jobs.DelayQueue5m,
		ReadyQueue:                jobs.ReadyQueueName,
		Status:                    jobs.StatusPublishAttempted,
		PublishedAt:               published.UTC(),
		ExpectedReadyAt:           jobs.ExpectedReadyAt(published, 5),
		CreatedAt:                 published.UTC(),
		UpdatedAt:                 published.UTC(),
	}
}

func countEvents(t *testing.T, job jobs.Job, eventType string) int {
	t.Helper()
	count := 0
	for _, event := range job.Events {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}
