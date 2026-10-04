// Package scheduler provides the local due-time dispatcher for scheduled
// run actions. It wakes on a fixed interval, reconciles tasks interrupted
// by a restart, and asks the service to execute due tasks exactly once.
package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

type Scheduler struct {
	service  *service.Service
	interval time.Duration
	batch    int

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

func New(app *service.Service, interval time.Duration, batchSize int) *Scheduler {
	if batchSize < 1 {
		batchSize = 1
	}
	return &Scheduler{service: app, interval: interval, batch: batchSize}
}

// Start launches the dispatch loop in the background. A non-positive
// interval disables the scheduler; Start is idempotent.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.interval <= 0 {
		return
	}
	s.started = true
	ctx, s.cancel = context.WithCancel(ctx)
	s.done = make(chan struct{})
	go s.loop(ctx)
}

// Stop signals the dispatch loop and waits for it to return. It is safe to
// call on a scheduler that was never started.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	s.recoverInterrupted()
	s.dispatch()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatch()
		}
	}
}

func (s *Scheduler) recoverInterrupted() {
	count, err := s.service.RecoverInterruptedTasks()
	if err != nil {
		log.Printf("scheduler: recover interrupted tasks: %v", err)
		return
	}
	if count > 0 {
		log.Printf("scheduler: reconciled %d interrupted task(s)", count)
	}
}

func (s *Scheduler) dispatch() {
	summary := s.service.DispatchDueTasks(s.batch)
	if summary.Claimed > 0 || summary.Failed > 0 {
		log.Printf("scheduler: dispatch claimed=%d succeeded=%d needs_attention=%d failed=%d",
			summary.Claimed, summary.Succeeded, summary.NeedsAttention, summary.Failed)
	}
}
