//go:build integration

package rabbitmq_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rabbitmq-delay-poc/backend/internal/jobs"
	"rabbitmq-delay-poc/backend/internal/rabbitmq"
	"rabbitmq-delay-poc/backend/internal/storage"
	"rabbitmq-delay-poc/backend/internal/worker"
)

func TestIntegrationProductionTopology(t *testing.T) {
	requireBroker(t)
	const vhost = "poc-topology"
	mgmt := newManagement()
	mgmt.ensureVhost(t, vhost)
	t.Cleanup(func() { mgmt.deleteVhost(vhost) })

	client := rabbitmq.NewClient(amqpVHostURL(vhost))
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Declare(rabbitmq.ProductionTopology()); err != nil {
		t.Fatal(err)
	}

	queues := mgmt.queues(t, vhost)
	if len(queues) != 4 {
		t.Fatalf("expected three delay queues and poc.ready, got %+v", queues)
	}
	byName := map[string]queueInfo{}
	for _, queue := range queues {
		byName[queue.Name] = queue
	}
	ready, ok := byName["poc.ready"]
	if !ok || !ready.Durable {
		t.Fatalf("ready queue %+v", ready)
	}
	for _, name := range []string{"poc.delay.1m", "poc.delay.2m", "poc.delay.5m"} {
		delay, ok := byName[name]
		if !ok || !delay.Durable {
			t.Fatalf("delay queue %s %+v", name, delay)
		}
		wantTTL := map[string]int64{"poc.delay.1m": 60000, "poc.delay.2m": 120000, "poc.delay.5m": 300000}[name]
		if got := numberArg(delay.Arguments["x-message-ttl"]); got != wantTTL {
			t.Fatalf("%s x-message-ttl = %d arguments=%v", name, got, delay.Arguments)
		}
		if delay.Arguments["x-dead-letter-exchange"] != "poc.ready.exchange" {
			t.Fatalf("%s dlx = %v", name, delay.Arguments["x-dead-letter-exchange"])
		}
		if delay.Arguments["x-dead-letter-routing-key"] != "ready" {
			t.Fatalf("%s routing key = %v", name, delay.Arguments["x-dead-letter-routing-key"])
		}
		if delay.Arguments["x-queue-type"] != "classic" {
			t.Fatalf("%s queue type = %v", name, delay.Arguments["x-queue-type"])
		}
	}
	if ready.Arguments["x-queue-type"] != "classic" {
		t.Fatalf("ready queue type = %v", ready.Arguments["x-queue-type"])
	}

	found := false
	for _, binding := range mgmt.bindings(t, vhost, "poc.ready") {
		if binding.Source == "poc.ready.exchange" && binding.RoutingKey == "ready" && binding.Destination == "poc.ready" {
			found = true
		}
	}
	if !found {
		t.Fatal("ready queue is not bound to poc.ready.exchange with routing key ready")
	}
}

func TestIntegrationMessageStaysDelayedUntilTTL(t *testing.T) {
	requireBroker(t)
	const vhost = "poc-it-delay"
	topo := mustTestTopology(t, 4*time.Second)
	client, store, _ := setupIsolated(t, vhost, topo)

	job := publishTestJob(t, store, client, topo)
	publishedAt := time.Now()
	if getOne(t, client, topo.ReadyQueue) {
		t.Fatal("ready queue had a message immediately after publish")
	}
	if sleep := time.Until(publishedAt.Add(1500 * time.Millisecond)); sleep > 0 {
		time.Sleep(sleep)
	}
	if time.Since(publishedAt) >= topo.MessageTTL() {
		t.Fatal("early observation window slipped past the TTL")
	}
	if getOne(t, client, topo.ReadyQueue) {
		t.Fatal("message became available before the TTL")
	}
	if depth := queueStat(t, vhost, topo.DelayQueueName(), "messages"); depth < 1 {
		t.Fatalf("delay queue depth = %d; message was not waiting in %s", depth, topo.DelayQueueName())
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go (&worker.Worker{
		Client:   client,
		Topology: topo,
		Store:    store,
		Process:  func(context.Context, jobs.Job) error { return nil },
	}).Run(ctx)

	waitStatus(t, store, job.JobID, jobs.StatusCompleted, 15*time.Second)
	loaded, err := store.Get(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.BrokerReadyAt == nil {
		t.Fatal("expired message reached the consumer without x-death time")
	}
	if loaded.ConsumptionStartedAt == nil || loaded.ConsumptionStartedAt.Before(publishedAt.Add(-time.Second)) {
		t.Fatalf("consumer start %v is not after publish %s", loaded.ConsumptionStartedAt, publishedAt)
	}
}

func TestIntegrationAckWaitsForProcessing(t *testing.T) {
	requireBroker(t)
	const vhost = "poc-it-ack"
	topo := mustTestTopology(t, 3*time.Second)
	client, store, _ := setupIsolated(t, vhost, topo)

	release := make(chan struct{})
	entered := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go (&worker.Worker{
		Client:   client,
		Topology: topo,
		Store:    store,
		AfterReceipt: func(jobs.Job) {
			enteredOnce.Do(func() { close(entered) })
			<-release
		},
		Process: func(context.Context, jobs.Job) error { return nil },
	}).Run(ctx)

	job := publishTestJob(t, store, client, topo)
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("receipt hook did not run")
	}
	if unacked := waitStat(t, vhost, topo.ReadyQueue, "messages_unacknowledged", 1, 8*time.Second); unacked < 1 {
		t.Fatalf("expected an unacknowledged delivery during processing, got %d", unacked)
	}
	current, err := store.Get(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status == jobs.StatusCompleted || current.ConsumptionCompletedAt != nil {
		t.Fatal("job was completed before processing was released")
	}

	releaseFn()
	waitStatus(t, store, job.JobID, jobs.StatusCompleted, 10*time.Second)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if queueStat(t, vhost, topo.ReadyQueue, "messages_unacknowledged") == 0 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("message stayed unacknowledged after completion (unacked=%d)", queueStat(t, vhost, topo.ReadyQueue, "messages_unacknowledged"))
}

func TestIntegrationRedeliveryDoesNotDuplicateCompletion(t *testing.T) {
	requireBroker(t)
	const vhost = "poc-it-redelivery"
	topo := mustTestTopology(t, 3*time.Second)
	client, store, _ := setupIsolated(t, vhost, topo)

	job := publishTestJob(t, store, client, topo)
	delivery, ch := waitDelivery(t, client, topo.ReadyQueue, 15*time.Second)
	broker, warning, _ := rabbitmq.ExtractExpiredDeath(delivery.Headers, topo.DelayQueueName())
	if broker == nil {
		t.Fatalf("missing x-death: %s", warning)
	}
	first, duplicate, err := store.RecordReceipt(context.Background(), job.JobID, broker, warning, time.Now().UTC())
	if err != nil || duplicate {
		t.Fatalf("first receipt duplicate=%v err=%v", duplicate, err)
	}
	if err := delivery.Nack(false, true); err != nil {
		t.Fatal(err)
	}
	_ = ch.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go (&worker.Worker{
		Client:   client,
		Topology: topo,
		Store:    store,
		Process:  func(context.Context, jobs.Job) error { return nil },
	}).Run(ctx)

	done := waitStatus(t, store, job.JobID, jobs.StatusCompleted, 15*time.Second)
	if !done.ConsumptionStartedAt.Equal(*first.ConsumptionStartedAt) || !done.BrokerReadyAt.Equal(*first.BrokerReadyAt) {
		t.Fatalf("redelivery overwrote timestamps start %s/%s broker %s/%s", done.ConsumptionStartedAt, first.ConsumptionStartedAt, done.BrokerReadyAt, first.BrokerReadyAt)
	}
	if countTypes(done, jobs.EventConsumptionStarted) != 1 || countTypes(done, jobs.EventConsumptionCompleted) != 1 || countTypes(done, jobs.EventBrokerReady) != 1 {
		t.Fatalf("duplicate events %+v", done.Events)
	}
}

func TestIntegrationHistorySurvivesReopen(t *testing.T) {
	requireBroker(t)
	const vhost = "poc-it-reopen"
	topo := mustTestTopology(t, 3*time.Second)
	client, store, path := setupIsolated(t, vhost, topo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go (&worker.Worker{
		Client:   client,
		Topology: topo,
		Store:    store,
		Process:  func(context.Context, jobs.Job) error { return nil },
	}).Run(ctx)

	job := publishTestJob(t, store, client, topo)
	done := waitStatus(t, store, job.JobID, jobs.StatusCompleted, 15*time.Second)
	cancel()
	time.Sleep(200 * time.Millisecond)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	loaded, err := reopened.Get(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != jobs.StatusCompleted || loaded.PublishConfirmedAt == nil || loaded.BrokerReadyAt == nil || loaded.ConsumptionCompletedAt == nil {
		t.Fatalf("reopened job lost lifecycle data: %+v", loaded)
	}
	if !loaded.PublishedAt.Equal(done.PublishedAt) || !loaded.BrokerReadyAt.Equal(*done.BrokerReadyAt) {
		t.Fatalf("timestamps changed across reopen")
	}
	if countTypes(loaded, jobs.EventPublishConfirmed) != 1 || countTypes(loaded, jobs.EventConsumptionCompleted) != 1 {
		t.Fatalf("events after reopen %+v", loaded.Events)
	}
}

func requireBroker(t *testing.T) {
	t.Helper()
	conn, err := amqp.Dial(amqpVHostURL("/"))
	if err != nil {
		t.Fatalf("RabbitMQ is not available at %s: %v", amqpVHostURL("/"), err)
	}
	_ = conn.Close()
}

func amqpPort() string {
	if port := os.Getenv("RABBITMQ_AMQP_PORT"); port != "" {
		return port
	}
	return "5672"
}

func managementPort() string {
	if port := os.Getenv("RABBITMQ_MANAGEMENT_PORT"); port != "" {
		return port
	}
	return "15672"
}

func mustTestTopology(t *testing.T, ttl time.Duration) rabbitmq.Topology {
	t.Helper()
	topo, err := rabbitmq.IsolatedTestTopology(ttl)
	if err != nil {
		t.Fatal(err)
	}
	return topo
}

func setupIsolated(t *testing.T, vhost string, topo rabbitmq.Topology) (*rabbitmq.Client, *storage.Store, string) {
	t.Helper()
	mgmt := newManagement()
	mgmt.ensureVhost(t, vhost)
	t.Cleanup(func() { mgmt.deleteVhost(vhost) })

	client := rabbitmq.NewClient(amqpVHostURL(vhost))
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Declare(topo); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/poc.db"
	store, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return client, store, path
}

func publishTestJob(t *testing.T, store *storage.Store, client *rabbitmq.Client, topo rabbitmq.Topology) jobs.Job {
	t.Helper()
	publishedAt := time.Now().UTC()
	job := jobs.Job{
		JobID:                     newJobID(t),
		Payload:                   jobs.Payload{Message: "Sample WhatsApp message"},
		DelayMinutes:              5,
		ProcessingDurationSeconds: 0,
		DelayQueue:                topo.DelayQueueName(),
		ReadyQueue:                topo.ReadyQueue,
		Status:                    jobs.StatusPublishAttempted,
		PublishedAt:               publishedAt,
		ExpectedReadyAt:           jobs.ExpectedReadyAt(publishedAt, 5),
		CreatedAt:                 publishedAt,
		UpdatedAt:                 publishedAt,
	}
	if err := store.CreateAttempt(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(worker.Message{
		JobID:                     job.JobID,
		Payload:                   job.Payload,
		ProcessingDurationSeconds: job.ProcessingDurationSeconds,
		PublishedAt:               job.PublishedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher := rabbitmq.NewPublisher(client, topo.DelayQueueName())
	t.Cleanup(func() { _ = publisher.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	confirmedAt, err := publisher.Publish(ctx, topo.DelayQueueName(), job.JobID, body, publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	confirmed, err := store.MarkConfirmed(context.Background(), job.JobID, confirmedAt)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.PublishConfirmedAt == nil {
		t.Fatal("missing confirm timestamp")
	}
	return confirmed
}

func newJobID(t *testing.T) string {
	t.Helper()
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		t.Fatal(err)
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

func getOne(t *testing.T, client *rabbitmq.Client, queue string) bool {
	t.Helper()
	ch, err := client.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	msg, ok, err := ch.Get(queue, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		_ = msg.Nack(false, true)
	}
	return ok
}

func waitDelivery(t *testing.T, client *rabbitmq.Client, queue string, timeout time.Duration) (amqp.Delivery, *amqp.Channel) {
	t.Helper()
	ch, err := client.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}
	msgs, err := ch.Consume(queue, "redelivery-probe", false, true, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatal("delivery channel closed")
		}
		return msg, ch
	case <-time.After(timeout):
		_ = ch.Close()
		t.Fatal("timed out waiting for a delivery")
	}
	return amqp.Delivery{}, nil
}

func waitStatus(t *testing.T, store *storage.Store, jobID string, status jobs.Status, timeout time.Duration) jobs.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.Get(context.Background(), jobID)
		if err == nil && job.Status == status {
			return job
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", jobID, status)
	return jobs.Job{}
}

func amqpVHostURL(vhost string) string {
	if vhost == "/" {
		return "amqp://guest:guest@127.0.0.1:" + amqpPort() + "/"
	}
	return "amqp://guest:guest@127.0.0.1:" + amqpPort() + "/" + url.PathEscape(vhost)
}

type queueInfo struct {
	Name      string         `json:"name"`
	Durable   bool           `json:"durable"`
	Arguments map[string]any `json:"arguments"`
}

type bindingInfo struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	RoutingKey  string `json:"routing_key"`
}

type management struct {
	base string
}

func newManagement() management {
	return management{base: "http://127.0.0.1:" + managementPort() + "/api"}
}

func (m management) ensureVhost(t *testing.T, vhost string) {
	t.Helper()
	m.do(t, http.MethodPut, "/vhosts/"+url.PathEscape(vhost), map[string]any{}, nil)
	m.do(t, http.MethodPut, "/permissions/"+url.PathEscape(vhost)+"/guest", map[string]string{
		"configure": ".*",
		"write":     ".*",
		"read":      ".*",
	}, nil)
}

func (m management) deleteVhost(vhost string) {
	req, err := http.NewRequest(http.MethodDelete, m.base+"/vhosts/"+url.PathEscape(vhost), nil)
	if err != nil {
		return
	}
	req.SetBasicAuth("guest", "guest")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
}

func (m management) queues(t *testing.T, vhost string) []queueInfo {
	t.Helper()
	var queues []queueInfo
	m.do(t, http.MethodGet, "/queues/"+url.PathEscape(vhost), nil, &queues)
	if queues == nil {
		return []queueInfo{}
	}
	return queues
}

func (m management) bindings(t *testing.T, vhost, queue string) []bindingInfo {
	t.Helper()
	var bindings []bindingInfo
	m.do(t, http.MethodGet, "/queues/"+url.PathEscape(vhost)+"/"+url.PathEscape(queue)+"/bindings", nil, &bindings)
	return bindings
}

func (m management) do(t *testing.T, method, path string, payload any, dest any) {
	t.Helper()
	var reader io.Reader = strings.NewReader("")
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, m.base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("guest", "guest")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode >= 300 {
		t.Fatalf("%s %s -> %s: %s", method, path, res.Status, raw)
	}
	if dest != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, dest); err != nil {
			t.Fatalf("decode %s: %v body=%s", path, err, raw)
		}
	}
}

func queueStat(t *testing.T, vhost, name, column string) int {
	t.Helper()
	out, err := exec.Command("docker", "exec", "rabbitmq-delay-poc", "rabbitmqctl", "list_queues", "-p", vhost, "name", column).CombinedOutput()
	if err != nil {
		t.Fatalf("rabbitmqctl: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			var n int
			if _, err := fmt.Sscan(fields[1], &n); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return n
		}
	}
	t.Fatalf("queue %s missing from rabbitmqctl output:\n%s", name, out)
	return 0
}

func waitStat(t *testing.T, vhost, name, column string, minimum int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		last = queueStat(t, vhost, name, column)
		if last >= minimum {
			return last
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last
}

func numberArg(value any) int64 {
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int:
		return int64(typed)
	case int64:
		return typed
	default:
		return -1
	}
}

func countTypes(job jobs.Job, eventType string) int {
	n := 0
	for _, event := range job.Events {
		if event.EventType == eventType {
			n++
		}
	}
	return n
}
