package sse

import (
	"sync"

	"rabbitmq-delay-poc/backend/internal/jobs"
)

// Broker fans job snapshots out to SSE subscribers.
// A slow client drops an update rather than blocking the consumer.
// REST reads remain the source of truth after reconnect.
type Broker struct {
	mu     sync.Mutex
	subs   map[int]chan jobs.Job
	clears map[int]chan struct{}
	next   int
}

func New() *Broker {
	return &Broker{
		subs:   map[int]chan jobs.Job{},
		clears: map[int]chan struct{}{},
	}
}

func (b *Broker) Subscribe() (<-chan jobs.Job, <-chan struct{}, func()) {
	ch := make(chan jobs.Job, 16)
	cleared := make(chan struct{}, 1)
	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = ch
	b.clears[id] = cleared
	b.mu.Unlock()
	return ch, cleared, func() {
		b.mu.Lock()
		delete(b.subs, id)
		delete(b.clears, id)
		b.mu.Unlock()
	}
}

func (b *Broker) Publish(job jobs.Job) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- job:
		default:
		}
	}
}

// PublishCleared tells dashboards that every stored job was deleted.
func (b *Broker) PublishCleared() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.clears {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
