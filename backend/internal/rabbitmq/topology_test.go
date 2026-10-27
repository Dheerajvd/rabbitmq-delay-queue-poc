package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rabbitmq-delay-poc/backend/internal/jobs"
)

func TestProductionTopologyHasThreeDelayQueues(t *testing.T) {
	topo := ProductionTopology()
	if err := topo.Validate(); err != nil {
		t.Fatal(err)
	}
	if topo.ReadyQueue != "poc.ready" || topo.ReadyExchange != "poc.ready.exchange" || topo.RoutingKey != "ready" {
		t.Fatalf("ready routing = %+v", topo)
	}
	if len(topo.DelayQueues) != 3 {
		t.Fatalf("delay queues = %+v", topo.DelayQueues)
	}
	want := map[string]int64{
		jobs.DelayQueue1m: 60000,
		jobs.DelayQueue2m: 120000,
		jobs.DelayQueue5m: 300000,
	}
	for _, queue := range topo.DelayQueues {
		ttl, ok := want[queue.Name]
		if !ok || queue.MessageTTL.Milliseconds() != ttl {
			t.Fatalf("queue %+v", queue)
		}
	}
	if topo.QueueType != "classic" {
		t.Fatal(topo.QueueType)
	}
}

func TestProductionNamesRejectShortTTL(t *testing.T) {
	topo := ProductionTopology()
	topo.DelayQueues[2].MessageTTL = 2 * time.Second
	if err := topo.Validate(); err == nil {
		t.Fatal("expected refusal to retarget a production delay queue")
	}
}

func TestIsolatedTopologyStaysOffProductionNames(t *testing.T) {
	topo, err := IsolatedTestTopology(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if topo.DelayQueueName() == jobs.DelayQueue1m || topo.ReadyQueue == jobs.ReadyQueueName {
		t.Fatal("isolated topology used production queue names")
	}
	if _, err := IsolatedTestTopology(5 * time.Minute); err == nil {
		t.Fatal("expected long test TTL to be rejected")
	}
	if _, err := IsolatedTestTopology(0); err == nil {
		t.Fatal("expected zero TTL to be rejected")
	}
}

func TestExtractExpiredDeath(t *testing.T) {
	when := time.Date(2026, 10, 12, 6, 5, 1, 0, time.UTC)
	headers := amqp.Table{
		"x-death": []any{
			amqp.Table{
				"queue":  "other",
				"reason": "rejected",
				"time":   when.Add(time.Minute),
				"count":  int64(1),
			},
			amqp.Table{
				"queue":  jobs.DelayQueue1m,
				"reason": "expired",
				"time":   when,
				"count":  int32(1),
			},
		},
	}
	got, warning, death := ExtractExpiredDeath(headers, jobs.DelayQueue1m)
	if warning != "" {
		t.Fatal(warning)
	}
	if got == nil || !got.Equal(when) {
		t.Fatalf("time = %v", got)
	}
	if death.Queue != jobs.DelayQueue1m || death.Reason != "expired" || death.Count != 1 {
		t.Fatalf("death = %+v", death)
	}
}

func TestExtractExpiredDeathUnixTimestamp(t *testing.T) {
	when := time.Unix(1_760_000_000, 0).UTC()
	headers := amqp.Table{
		"x-death": []any{
			map[string]any{
				"queue":  "poc.delay",
				"reason": "expired",
				"time":   when.Unix(),
			},
		},
	}
	got, warning, _ := ExtractExpiredDeath(headers, "poc.delay")
	if warning != "" || got == nil || !got.Equal(when) {
		t.Fatalf("got %v warning %q", got, warning)
	}
}

func TestExtractExpiredDeathMissingOrMalformed(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
	}{
		{name: "nil headers"},
		{name: "missing header", headers: amqp.Table{}},
		{name: "wrong type", headers: amqp.Table{"x-death": "expired"}},
		{name: "empty", headers: amqp.Table{"x-death": []any{}}},
		{name: "missing time", headers: amqp.Table{"x-death": []any{amqp.Table{
			"queue": "poc.delay.5m", "reason": "expired",
		}}}},
		{name: "wrong reason", headers: amqp.Table{"x-death": []any{amqp.Table{
			"queue": "poc.delay.5m", "reason": "rejected", "time": time.Now().UTC(),
		}}}},
		{name: "wrong queue", headers: amqp.Table{"x-death": []any{amqp.Table{
			"queue": "poc.ready", "reason": "expired", "time": time.Now().UTC(),
		}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, warning, _ := ExtractExpiredDeath(tc.headers, "poc.delay.5m")
			if got != nil {
				t.Fatalf("fabricated timestamp %v", got)
			}
			if warning == "" {
				t.Fatal("expected a diagnostic warning")
			}
		})
	}
}
