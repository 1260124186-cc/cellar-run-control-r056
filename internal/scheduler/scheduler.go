// Package scheduler runs the local periodic pass that drives due
// scheduled run actions. It owns only timing and process lifecycle;
// claiming, recovery, and execution live in the service layer.
package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// Triggerer is the slice of the service the scheduler needs.
type Triggerer interface {
	TriggerScheduledActions() (domain.ScheduleReport, error)
}

// Scheduler wakes on a fixed interval and asks the service to process due
// actions. Cross-process coordination and at-most-once semantics are the
// service's responsibility; here a single in-process mutex additionally
// prevents overlapping passes (timer and manual trigger) in one process.
type Scheduler struct {
	trigger  Triggerer
	interval time.Duration
	mu       sync.Mutex
	running  bool
	cancel   context.CancelFunc
	done     chan struct{}
}

func New(trigger Triggerer, interval time.Duration) *Scheduler {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Scheduler{trigger: trigger, interval: interval}
}

// Start launches the background loop. A pass runs immediately so entries
// that came due while the process was stopped do not wait a full interval.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.running = true
	go s.loop(ctx)
}

// Stop signals the loop to exit and waits for the current pass to finish.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.cancel
	done := s.done
	s.running = false
	s.mu.Unlock()

	cancel()
	<-done
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	s.runOnce()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runOnce()
		}
	}
}

// runOnce serializes passes and never lets one faulty pass kill the loop.
func (s *Scheduler) runOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("scheduler pass panicked: %v", recovered)
		}
	}()

	report, err := s.trigger.TriggerScheduledActions()
	if err != nil {
		log.Printf("scheduler pass failed: %v", err)
		return
	}
	if report.Skipped {
		return
	}
	if report.Recovered > 0 || report.NeedsAttention > 0 {
		log.Printf("scheduler pass: recovered=%d due=%d succeeded=%d needs_attention=%d",
			report.Recovered, report.Due, report.Succeeded, report.NeedsAttention)
	}
}
