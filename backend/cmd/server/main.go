package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"rabbitmq-delay-poc/backend/internal/api"
	"rabbitmq-delay-poc/backend/internal/config"
	"rabbitmq-delay-poc/backend/internal/jobs"
	"rabbitmq-delay-poc/backend/internal/rabbitmq"
	"rabbitmq-delay-poc/backend/internal/sse"
	"rabbitmq-delay-poc/backend/internal/storage"
	"rabbitmq-delay-poc/backend/internal/worker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}

	store, err := storage.Open(cfg.SQLitePath)
	if err != nil {
		slog.Error("sqlite", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	client := rabbitmq.NewClient(cfg.AMQPURL)
	if err := client.Connect(); err != nil {
		slog.Error("rabbitmq", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	topo := rabbitmq.ProductionTopology()
	if err := client.Declare(topo); err != nil {
		slog.Error("declare topology", "error", err)
		os.Exit(1)
	}
	slog.Info("topology ready",
		"delay_queues", delayQueueLog(topo),
		"ready_queue", topo.ReadyQueue,
		"exchange", topo.ReadyExchange,
		"routing_key", topo.RoutingKey,
	)

	events := sse.New()
	publisher := rabbitmq.NewPublisher(client, jobs.DelayQueue5m)
	defer publisher.Close()

	consumer := &worker.Worker{
		Client:   client,
		Topology: topo,
		Store:    store,
		Events:   events,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go consumer.Run(ctx)

	httpServer := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: api.New(api.Deps{
			Store:             store,
			Publisher:         publisher,
			Events:            events,
			ReadyQueue:        topo.ReadyQueue,
			DefaultProcessing: cfg.ProcessingDurationSeconds,
			RabbitReady:       client.Ping,
			SQLiteReady:       store.Ping,
		}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("http listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "error", err)
	}
	slog.Info("stopped")
}

func delayQueueLog(topo rabbitmq.Topology) string {
	parts := make([]string, 0, len(topo.DelayQueues))
	for _, queue := range topo.DelayQueues {
		parts = append(parts, fmt.Sprintf("%s=%d", queue.Name, queue.MessageTTL.Milliseconds()))
	}
	return strings.Join(parts, ",")
}
