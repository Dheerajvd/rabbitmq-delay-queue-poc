package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rabbitmq-delay-poc/backend/internal/jobs"
	"rabbitmq-delay-poc/backend/internal/sse"
	"rabbitmq-delay-poc/backend/internal/storage"
)

type fakePublisher struct {
	confirmedAt time.Time
	err         error
	queue       string
	body        []byte
	messageID   string
}

func (f *fakePublisher) Publish(_ context.Context, queue, messageID string, body []byte, _ time.Time) (time.Time, error) {
	f.queue = queue
	f.messageID = messageID
	f.body = append([]byte(nil), body...)
	if f.err != nil {
		return time.Time{}, f.err
	}
	if f.confirmedAt.IsZero() {
		return time.Now().UTC(), nil
	}
	return f.confirmedAt, nil
}

func TestPublishJobConfirmsAndLists(t *testing.T) {
	store := openStore(t)
	events := sse.New()
	publisher := &fakePublisher{confirmedAt: time.Date(2026, 10, 12, 6, 0, 1, 0, time.UTC)}
	server := New(Deps{Store: store, Publisher: publisher, Events: events})

	body := []byte(`{"processing_duration_seconds":2,"payload":{"message":"Sample WhatsApp message"}}`)
	res := post(server, "/api/jobs", body)
	if res.Code != http.StatusCreated {
		t.Fatalf("status %d body %s", res.Code, res.Body.String())
	}
	var created jobs.Job
	if err := json.Unmarshal(res.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.DelayMinutes != 5 || created.Status != jobs.StatusWaiting || created.DelayQueue != jobs.DelayQueue5m {
		t.Fatalf("job %+v", created)
	}
	if created.PublishConfirmedAt == nil || !created.PublishConfirmedAt.Equal(publisher.confirmedAt) {
		t.Fatalf("confirm %+v", created.PublishConfirmedAt)
	}
	if !created.ExpectedReadyAt.Equal(jobs.ExpectedReadyAt(created.PublishedAt, 5)) {
		t.Fatalf("expected ready %s published %s", created.ExpectedReadyAt, created.PublishedAt)
	}
	if publisher.queue != jobs.DelayQueue5m || publisher.messageID != created.JobID {
		t.Fatalf("publish target %s %s", publisher.queue, publisher.messageID)
	}
	if created.Metrics == nil || created.Metrics.RequestedDelayMs != 300000 {
		t.Fatalf("metrics %+v", created.Metrics)
	}

	listRes := get(server, "/api/jobs")
	var listed struct {
		Jobs []jobs.Job `json:"jobs"`
	}
	if err := json.Unmarshal(listRes.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Jobs) != 1 || listed.Jobs[0].JobID != created.JobID {
		t.Fatalf("list %+v", listed.Jobs)
	}

	detail := get(server, "/api/jobs/"+created.JobID)
	if detail.Code != http.StatusOK {
		t.Fatalf("detail %d %s", detail.Code, detail.Body.String())
	}
}

func TestPublishRejectsDelayAndUnknownFields(t *testing.T) {
	server := New(Deps{Store: openStore(t), Publisher: &fakePublisher{}, Events: sse.New()})
	cases := []string{
		`{"delay_minutes":3,"payload":{"message":"hi"}}`,
		`{"delay_seconds":30,"payload":{"message":"hi"}}`,
		`{"payload":{"message":"  "}}`,
		`{"processing_duration_seconds":99,"payload":{"message":"hi"}}`,
	}
	for _, body := range cases {
		res := post(server, "/api/jobs", []byte(body))
		if res.Code != http.StatusBadRequest {
			t.Fatalf("body %s status %d %s", body, res.Code, res.Body.String())
		}
	}
}

func TestPublishRoutesSelectedDelay(t *testing.T) {
	cases := []struct {
		minutes int
		queue   string
	}{
		{minutes: 1, queue: jobs.DelayQueue1m},
		{minutes: 2, queue: jobs.DelayQueue2m},
		{minutes: 5, queue: jobs.DelayQueue5m},
	}
	for _, tc := range cases {
		publisher := &fakePublisher{}
		server := New(Deps{Store: openStore(t), Publisher: publisher, Events: sse.New()})
		body := `{"delay_minutes":` + strconv.Itoa(tc.minutes) + `,"payload":{"message":"hello"}}`
		res := post(server, "/api/jobs", []byte(body))
		if res.Code != http.StatusCreated {
			t.Fatalf("%d status %d %s", tc.minutes, res.Code, res.Body.String())
		}
		var created jobs.Job
		if err := json.Unmarshal(res.Body.Bytes(), &created); err != nil {
			t.Fatal(err)
		}
		if created.DelayMinutes != tc.minutes || created.DelayQueue != tc.queue || publisher.queue != tc.queue {
			t.Fatalf("routed %+v publisher %s", created, publisher.queue)
		}
		if !created.ExpectedReadyAt.Equal(created.PublishedAt.Add(time.Duration(tc.minutes) * time.Minute)) {
			t.Fatalf("expected ready %s published %s", created.ExpectedReadyAt, created.PublishedAt)
		}
	}
}

func TestPublishFailureIsNotConfirmed(t *testing.T) {
	store := openStore(t)
	publisher := &fakePublisher{err: io.ErrClosedPipe}
	server := New(Deps{Store: store, Publisher: publisher, Events: sse.New()})
	res := post(server, "/api/jobs", []byte(`{"payload":{"message":"Sample WhatsApp message"}}`))
	if res.Code != http.StatusBadGateway {
		t.Fatalf("status %d %s", res.Code, res.Body.String())
	}
	var payload struct {
		Error string   `json:"error"`
		Job   jobs.Job `json:"job"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Job.Status != jobs.StatusPublishFailed || payload.Job.PublishConfirmedAt != nil {
		t.Fatalf("job %+v", payload.Job)
	}
	if payload.Job.Error == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteAllJobs(t *testing.T) {
	store := openStore(t)
	server := New(Deps{Store: store, Publisher: &fakePublisher{}, Events: sse.New()})
	created := post(server, "/api/jobs", []byte(`{"payload":{"message":"Sample WhatsApp message"}}`))
	if created.Code != http.StatusCreated {
		t.Fatalf("create %d %s", created.Code, created.Body.String())
	}
	var job jobs.Job
	if err := json.Unmarshal(created.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}

	deleted := deleteReq(server, "/api/jobs")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete %d %s", deleted.Code, deleted.Body.String())
	}
	var payload struct {
		Deleted int64 `json:"deleted"`
	}
	if err := json.Unmarshal(deleted.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Deleted != 1 {
		t.Fatalf("deleted = %d", payload.Deleted)
	}
	if res := get(server, "/api/jobs"); !strings.Contains(res.Body.String(), `"jobs":[]`) {
		t.Fatalf("list %s", res.Body.String())
	}
	if res := get(server, "/api/jobs/"+job.JobID); res.Code != http.StatusNotFound {
		t.Fatalf("detail %d", res.Code)
	}
}

func TestStatusFilterAndNotFound(t *testing.T) {
	server := New(Deps{Store: openStore(t), Publisher: &fakePublisher{}, Events: sse.New()})
	if res := get(server, "/api/jobs?status=NOPE"); res.Code != http.StatusBadRequest {
		t.Fatalf("filter %d", res.Code)
	}
	if res := get(server, "/api/jobs/11111111-1111-4111-8111-111111111111"); res.Code != http.StatusNotFound {
		t.Fatalf("missing %d", res.Code)
	}
}

func TestHealthDegraded(t *testing.T) {
	server := New(Deps{
		Store:       openStore(t),
		Publisher:   &fakePublisher{},
		Events:      sse.New(),
		RabbitReady: func() error { return io.ErrClosedPipe },
		SQLiteReady: func() error { return nil },
	})
	res := get(server, "/healthz")
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d %s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"rabbitmq":"unavailable"`) {
		t.Fatal(res.Body.String())
	}
}

func TestSSEHello(t *testing.T) {
	server := New(Deps{Store: openStore(t), Publisher: &fakePublisher{}, Events: sse.New()})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/api/events", nil).WithContext(ctx)
	rec := &flushResponse{header: http.Header{}}
	done := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(rec, req)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(rec.String(), "event: hello") {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sse handler did not return")
	}
	if !strings.Contains(rec.String(), "event: hello") {
		t.Fatalf("body %s", rec.String())
	}
}

func openStore(t *testing.T) *storage.Store {
	t.Helper()
	store, err := storage.Open(t.TempDir() + "/poc.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func post(server *Server, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec
}

func deleteReq(server *Server, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec
}

func get(server *Server, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	return rec
}

type flushResponse struct {
	mu     sync.Mutex
	header http.Header
	body   bytes.Buffer
	code   int
}

func (f *flushResponse) Header() http.Header { return f.header }
func (f *flushResponse) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.code == 0 {
		f.code = http.StatusOK
	}
	return f.body.Write(p)
}
func (f *flushResponse) WriteHeader(code int) {
	f.mu.Lock()
	f.code = code
	f.mu.Unlock()
}
func (f *flushResponse) Flush() {}
func (f *flushResponse) String() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.body.String()
}
