package worker

import (
	"context"
	"testing"
	"time"
)

func TestSimulateProcessingHonorsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := SimulateProcessing(ctx, 30)
	if err == nil {
		t.Fatal("expected cancellation")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation waited on the processing timer")
	}
}
