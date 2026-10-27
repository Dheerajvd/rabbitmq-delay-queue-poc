package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Declare creates the direct ready exchange, the ready queue and binding,
// and each delay queue. All queues are durable classic queues.
//
// Each delay queue carries its own x-message-ttl and dead-letters to
// poc.ready.exchange with routing key "ready". No per-message expiration
// is set. Queue arguments are immutable in RabbitMQ; changing them requires
// deleting the queue first.
func Declare(ch *amqp.Channel, topo Topology) error {
	if err := topo.Validate(); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(
		topo.ReadyExchange,
		"direct",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", topo.ReadyExchange, err)
	}

	readyArgs := amqp.Table{
		"x-queue-type": topo.QueueType,
	}
	if _, err := ch.QueueDeclare(
		topo.ReadyQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		readyArgs,
	); err != nil {
		return fmt.Errorf("declare queue %s: %w", topo.ReadyQueue, err)
	}
	if err := ch.QueueBind(
		topo.ReadyQueue,
		topo.RoutingKey,
		topo.ReadyExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind %s: %w", topo.ReadyQueue, err)
	}

	for _, queue := range topo.DelayQueues {
		delayArgs := amqp.Table{
			"x-queue-type":              topo.QueueType,
			"x-message-ttl":             queue.MessageTTL.Milliseconds(),
			"x-dead-letter-exchange":    topo.ReadyExchange,
			"x-dead-letter-routing-key": topo.RoutingKey,
		}
		if _, err := ch.QueueDeclare(
			queue.Name,
			true,
			false,
			false,
			false,
			delayArgs,
		); err != nil {
			return fmt.Errorf("declare queue %s: %w", queue.Name, err)
		}
	}
	return nil
}
