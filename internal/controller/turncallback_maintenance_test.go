package controller

import (
	"context"
	"testing"
	"time"
)

// TestRunMaintenance_ReturnsOnCtxCancel verifies the extracted maintenance loop
// exits cleanly when its context is cancelled (no goroutine/ticker leak).
func TestRunMaintenance_ReturnsOnCtxCancel(t *testing.T) {
	s := &CallbackServer{} // fields unused: pre-cancelled ctx means the ticker body never fires
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the ctx.Done() select arm must win before the first tick
	done := make(chan error, 1)
	go func() { done <- s.RunMaintenance(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunMaintenance returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunMaintenance did not return on cancelled context")
	}
}
