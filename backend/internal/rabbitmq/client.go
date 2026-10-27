package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Client owns the AMQP connection used by the publisher and the consumer.
type Client struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
}

func NewClient(url string) *Client {
	return &Client{url: url}
}

func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked()
}

func (c *Client) connectLocked() error {
	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}
	conn, err := amqp.Dial(c.url)
	if err != nil {
		c.conn = nil
		return fmt.Errorf("dial amqp: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() {
		c.conn = nil
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) IsReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.conn.IsClosed()
}

// Ping opens and closes a channel to confirm the broker is usable.
func (c *Client) Ping() error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	return ch.Close()
}

// Channel opens an AMQP channel, reconnecting once if the connection is down.
func (c *Client) Channel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(); err != nil {
		return nil, err
	}
	ch, err := c.conn.Channel()
	if err != nil {
		_ = c.conn.Close()
		c.conn = nil
		if err := c.connectLocked(); err != nil {
			return nil, err
		}
		ch, err = c.conn.Channel()
		if err != nil {
			return nil, fmt.Errorf("open channel: %w", err)
		}
	}
	return ch, nil
}

// Declare applies a validated topology on a short-lived channel.
func (c *Client) Declare(topo Topology) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	return Declare(ch, topo)
}

// Publisher publishes persistent messages to one queue through the default
// exchange and waits for a positive publisher confirm.
type Publisher struct {
	client *Client
	queue  string
	mu     sync.Mutex
	ch     *amqp.Channel
}

func NewPublisher(client *Client, queue string) *Publisher {
	return &Publisher{client: client, queue: queue}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.resetLocked()
}

func (p *Publisher) resetLocked() error {
	if p.ch == nil {
		return nil
	}
	err := p.ch.Close()
	p.ch = nil
	return err
}

func (p *Publisher) ensureChannel() error {
	if p.ch != nil && !p.ch.IsClosed() {
		return nil
	}
	p.ch = nil
	ch, err := p.client.Channel()
	if err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.ch = ch
	return nil
}

// Publish sends a persistent JSON body to the delay queue via the default
// exchange. The returned time is when the positive confirm was observed.
// A nack, return-path failure, or timeout is an error and must not be
// recorded as publish_confirmed_at.
func (p *Publisher) Publish(ctx context.Context, queue, messageID string, body []byte, publishedAt time.Time) (time.Time, error) {
	if queue == "" {
		queue = p.queue
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureChannel(); err != nil {
		return time.Time{}, err
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Timestamp:    publishedAt.UTC(),
		AppId:        "rabbitmq-delay-poc",
		Type:         "poc.job",
		Body:         body,
	})
	if err != nil {
		_ = p.resetLocked()
		return time.Time{}, fmt.Errorf("publish to %s: %w", queue, err)
	}
	if confirmation == nil {
		_ = p.resetLocked()
		return time.Time{}, fmt.Errorf("publish to %s: broker did not issue a confirm handle", queue)
	}

	acked, err := confirmation.WaitContext(ctx)
	confirmedAt := time.Now().UTC()
	if err != nil {
		_ = p.resetLocked()
		return time.Time{}, fmt.Errorf("publisher confirm: %w", err)
	}
	if !acked {
		return time.Time{}, fmt.Errorf("rabbitmq negatively acknowledged publish of %s", messageID)
	}
	return confirmedAt, nil
}
