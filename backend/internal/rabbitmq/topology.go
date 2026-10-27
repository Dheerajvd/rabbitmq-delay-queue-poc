package rabbitmq

import (
	"fmt"
	"time"

	"rabbitmq-delay-poc/backend/internal/jobs"
)

// DelayQueue is one durable queue whose TTL is the selected delay.
type DelayQueue struct {
	Name       string
	Minutes    int
	MessageTTL time.Duration
}

// Topology is the delay queues plus the ready queue and direct exchange.
// Exchanges are not counted as queues.
type Topology struct {
	DelayQueues   []DelayQueue
	ReadyQueue    string
	ReadyExchange string
	RoutingKey    string
	QueueType     string
}

// ProductionTopology declares three delay queues and poc.ready.
// TTLs are queue arguments: 60000, 120000, and 300000 milliseconds.
func ProductionTopology() Topology {
	return Topology{
		DelayQueues: []DelayQueue{
			{Name: jobs.DelayQueue1m, Minutes: 1, MessageTTL: time.Minute},
			{Name: jobs.DelayQueue2m, Minutes: 2, MessageTTL: 2 * time.Minute},
			{Name: jobs.DelayQueue5m, Minutes: 5, MessageTTL: 5 * time.Minute},
		},
		ReadyQueue:    jobs.ReadyQueueName,
		ReadyExchange: jobs.ReadyExchangeName,
		RoutingKey:    jobs.ReadyRoutingKey,
		QueueType:     "classic",
	}
}

// IsolatedTestTopology builds a short-TTL topology with different names.
// It cannot declare the production delay queues.
func IsolatedTestTopology(ttl time.Duration) (Topology, error) {
	topo := Topology{
		DelayQueues: []DelayQueue{
			{Name: "poc.it.delay", Minutes: 0, MessageTTL: ttl},
		},
		ReadyQueue:    "poc.it.ready",
		ReadyExchange: "poc.it.ready.exchange",
		RoutingKey:    jobs.ReadyRoutingKey,
		QueueType:     "classic",
	}
	if err := topo.Validate(); err != nil {
		return Topology{}, err
	}
	return topo, nil
}

// DelayQueueName is the delay queue used by a single-queue test topology.
func (t Topology) DelayQueueName() string {
	if len(t.DelayQueues) == 0 {
		return ""
	}
	return t.DelayQueues[0].Name
}

// MessageTTL is the TTL of the first delay queue. Integration tests use a
// topology with exactly one delay queue.
func (t Topology) MessageTTL() time.Duration {
	if len(t.DelayQueues) == 0 {
		return 0
	}
	return t.DelayQueues[0].MessageTTL
}

func (t Topology) usesProductionNames() bool {
	if t.ReadyQueue == jobs.ReadyQueueName || t.ReadyExchange == jobs.ReadyExchangeName {
		return true
	}
	for _, queue := range t.DelayQueues {
		switch queue.Name {
		case jobs.DelayQueue1m, jobs.DelayQueue2m, jobs.DelayQueue5m:
			return true
		}
	}
	return false
}

func (t Topology) isProduction() bool {
	want := ProductionTopology()
	if t.ReadyQueue != want.ReadyQueue || t.ReadyExchange != want.ReadyExchange || t.RoutingKey != want.RoutingKey || t.QueueType != want.QueueType {
		return false
	}
	if len(t.DelayQueues) != len(want.DelayQueues) {
		return false
	}
	for i := range want.DelayQueues {
		got := t.DelayQueues[i]
		expected := want.DelayQueues[i]
		if got.Name != expected.Name || got.Minutes != expected.Minutes || got.MessageTTL != expected.MessageTTL {
			return false
		}
	}
	return true
}

func (t Topology) isIsolatedTest() bool {
	if len(t.DelayQueues) != 1 {
		return false
	}
	queue := t.DelayQueues[0]
	return queue.Name == "poc.it.delay" &&
		t.ReadyQueue == "poc.it.ready" &&
		t.ReadyExchange == "poc.it.ready.exchange" &&
		t.RoutingKey == jobs.ReadyRoutingKey &&
		t.QueueType == "classic" &&
		queue.MessageTTL > 0 &&
		queue.MessageTTL < 5*time.Minute
}

// Validate blocks accidental changes to the application delay queues.
func (t Topology) Validate() error {
	if len(t.DelayQueues) == 0 || t.ReadyQueue == "" || t.ReadyExchange == "" || t.RoutingKey == "" {
		return fmt.Errorf("topology names must be non-empty")
	}
	seen := map[string]bool{}
	for _, queue := range t.DelayQueues {
		if queue.Name == "" || queue.MessageTTL <= 0 {
			return fmt.Errorf("each delay queue needs a name and a positive TTL")
		}
		if queue.Name == t.ReadyQueue {
			return fmt.Errorf("delay and ready queues must be distinct")
		}
		if seen[queue.Name] {
			return fmt.Errorf("duplicate delay queue %s", queue.Name)
		}
		seen[queue.Name] = true
	}
	if t.isProduction() {
		return nil
	}
	if t.usesProductionNames() {
		return fmt.Errorf("refusing to declare production queue names with a non-production TTL or routing setup")
	}
	if t.isIsolatedTest() {
		return nil
	}
	return fmt.Errorf("topology is neither the production delay queues nor the isolated test topology")
}
