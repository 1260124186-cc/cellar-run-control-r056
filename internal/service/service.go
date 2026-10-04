package service

import (
	"strings"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/clock"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/storage"
)

// ScheduleLock serializes scheduler passes across processes.
// storage.FileLock satisfies it.
type ScheduleLock interface {
	TryLock() (bool, error)
	Unlock() error
}

type noopScheduleLock struct{}

func (noopScheduleLock) TryLock() (bool, error) { return true, nil }
func (noopScheduleLock) Unlock() error          { return nil }

type Service struct {
	store        *storage.Store
	clock        clock.Clock
	scheduleLock ScheduleLock
}

func New(store *storage.Store, timeSource clock.Clock) *Service {
	if timeSource == nil {
		timeSource = clock.System{}
	}
	return &Service{
		store:        store,
		clock:        timeSource,
		scheduleLock: noopScheduleLock{},
	}
}

// WithScheduleLock installs the cross-process lock used by scheduler
// passes. Without it the service behaves as if every pass acquires the
// lock, which is correct for a single-process deployment.
func (s *Service) WithScheduleLock(lock ScheduleLock) *Service {
	if lock != nil {
		s.scheduleLock = lock
	}
	return s
}

func (s *Service) Revision() int64 {
	return s.store.Revision()
}

func (s *Service) checkExpected(current int64, expected *int64) error {
	if expected != nil && current != *expected {
		return domain.NewError(domain.CodeRevisionConflict,
			"the aggregate version does not match X-Expected-Version")
	}
	return nil
}

func generateEntityID(prefix string) (string, error) {
	return domain.NewID(prefix)
}

func generatedRunCode() (string, error) {
	value, err := domain.NewID("run")
	if err != nil {
		return "", err
	}
	suffix := strings.ToUpper(strings.TrimPrefix(value, "run_"))
	return "RUN-" + suffix, nil
}
