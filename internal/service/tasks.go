package service

import (
	"errors"
	"sort"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// errTaskNotClaimable aborts a claim or execution transaction without
// persisting anything when the task is no longer owned by this controller.
var errTaskNotClaimable = errors.New("scheduled task is not claimable")

func (s *Service) ScheduleRunAction(input ScheduleActionInput) (domain.ScheduledTask, error) {
	now := s.clock.Now()
	task := domain.ScheduledTask{
		RunID:     input.RunID,
		Kind:      input.Kind,
		State:     domain.TaskPending,
		DueAt:     input.DueAt.UTC(),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := task.Validate(); err != nil {
		return domain.ScheduledTask{}, err
	}
	id, err := generateEntityID("task")
	if err != nil {
		return domain.ScheduledTask{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	task.ID = id

	err = s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[task.RunID]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if run.State == domain.RunCompleted || run.State == domain.RunAborted {
			return domain.NewError(domain.CodeRunState,
				"cannot schedule actions for a completed or aborted run")
		}
		snapshot.Tasks[task.ID] = task.Clone()
		return nil
	})
	if err != nil {
		return domain.ScheduledTask{}, err
	}
	return task.Clone(), nil
}

func (s *Service) GetScheduledTask(id string) (domain.ScheduledTask, error) {
	var found domain.ScheduledTask
	err := s.store.View(func(snapshot domain.Snapshot) error {
		task, ok := snapshot.Tasks[id]
		if !ok {
			return domain.NewError(domain.CodeTaskNotFound, "scheduled task was not found")
		}
		found = task.Clone()
		return nil
	})
	if err != nil {
		return domain.ScheduledTask{}, err
	}
	return found, nil
}

func (s *Service) ListScheduledTasks(filter TaskFilter) ([]domain.ScheduledTask, error) {
	var tasks []domain.ScheduledTask
	err := s.store.View(func(snapshot domain.Snapshot) error {
		tasks = make([]domain.ScheduledTask, 0, len(snapshot.Tasks))
		for _, task := range snapshot.Tasks {
			if filter.RunID != "" && task.RunID != filter.RunID {
				continue
			}
			if filter.State != "" && task.State != filter.State {
				continue
			}
			tasks = append(tasks, task.Clone())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].DueAt.Equal(tasks[j].DueAt) {
			return tasks[i].ID < tasks[j].ID
		}
		return tasks[i].DueAt.Before(tasks[j].DueAt)
	})
	return tasks, nil
}

// DispatchDueTasks performs one pass over the due queue. Every due task is
// claimed and executed independently, so a failed task never delays the
// others. A limit of zero or less processes every due task.
func (s *Service) DispatchDueTasks(limit int) DispatchSummary {
	now := s.clock.Now()
	summary := DispatchSummary{}
	for _, task := range s.dueTasks(now, limit) {
		token, err := generateEntityID("claim")
		if err != nil {
			summary.Failed++
			continue
		}
		claimed, err := s.claimTask(task.ID, token, now)
		if err != nil {
			summary.Failed++
			continue
		}
		if !claimed {
			// Another controller claimed the task first; it owns the
			// execution and the outcome.
			continue
		}
		summary.Claimed++
		outcome, err := s.executeTask(task.ID, token, now)
		if err != nil {
			summary.Failed++
			continue
		}
		switch outcome {
		case domain.TaskSucceeded:
			summary.Succeeded++
		case domain.TaskNeedsAttention:
			summary.NeedsAttention++
		}
	}
	return summary
}

func (s *Service) dueTasks(now time.Time, limit int) []domain.ScheduledTask {
	var due []domain.ScheduledTask
	_ = s.store.View(func(snapshot domain.Snapshot) error {
		for _, task := range snapshot.Tasks {
			if task.State == domain.TaskPending && !task.DueAt.After(now) {
				due = append(due, task.Clone())
			}
		}
		return nil
	})
	sort.Slice(due, func(i, j int) bool {
		if due[i].DueAt.Equal(due[j].DueAt) {
			return due[i].ID < due[j].ID
		}
		return due[i].DueAt.Before(due[j].DueAt)
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	return due
}

// claimTask moves one task from waiting to executing inside a single store
// transaction. Controllers racing for the same task serialize on the store
// mutex, so exactly one claim can win; the loser leaves the task untouched.
func (s *Service) claimTask(id, token string, now time.Time) (bool, error) {
	claimed := false
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		task, ok := snapshot.Tasks[id]
		if !ok || task.State != domain.TaskPending {
			return errTaskNotClaimable
		}
		if err := task.Claim(token, now); err != nil {
			return err
		}
		snapshot.Tasks[id] = task
		claimed = true
		return nil
	})
	if errors.Is(err, errTaskNotClaimable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// executeTask applies the registered action and records the terminal outcome
// in the same store transaction. Committing the business transition and the
// task outcome atomically means a crash can never persist one without the
// other, so a run can never be advanced twice by one task.
func (s *Service) executeTask(id, token string, now time.Time) (domain.TaskState, error) {
	outcome := domain.TaskState("")
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		task, ok := snapshot.Tasks[id]
		if !ok || task.State != domain.TaskRunning || task.ClaimToken != token {
			return errTaskNotClaimable
		}
		outcome = applyTask(snapshot, &task, now)
		snapshot.Tasks[id] = task
		return nil
	})
	if errors.Is(err, errTaskNotClaimable) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// applyTask performs the registered business action against the snapshot and
// moves the task to its terminal state. Business rejections are recorded as
// the task reason instead of being propagated, and business copies are
// written back only on success, so a failed action leaves run and vessel
// state untouched.
func applyTask(snapshot *domain.Snapshot, task *domain.ScheduledTask, now time.Time) domain.TaskState {
	run, ok := snapshot.Runs[task.RunID]
	if !ok {
		_ = task.Attend("run was not found; the action cannot be completed", now)
		return domain.TaskNeedsAttention
	}
	switch task.Kind {
	case domain.ActionStartFermentation:
		if startFermentationFulfilled(run.State) {
			// The business effect is already visible (for example a manual
			// start, or an attempt whose outcome was recovered). Record
			// fulfillment instead of advancing the run a second time.
			_ = task.Succeed("run already left its waiting state; no further transition applied", now)
			return domain.TaskSucceeded
		}
		if _, _, err := applyStartFermentation(snapshot, run.ID, now); err != nil {
			_ = task.Attend(err.Error(), now)
			return domain.TaskNeedsAttention
		}
		_ = task.Succeed("", now)
		return domain.TaskSucceeded
	default:
		_ = task.Attend("unknown action kind "+string(task.Kind), now)
		return domain.TaskNeedsAttention
	}
}

// startFermentationFulfilled reports whether the run already reached or
// passed the state the scheduled action was meant to reach.
func startFermentationFulfilled(state domain.RunState) bool {
	return state == domain.RunFermenting ||
		state == domain.RunConditioning ||
		state == domain.RunCompleted
}

// RecoverInterruptedTasks reconciles tasks that were still executing when
// the process stopped. A task whose business effect is already visible is
// recorded as succeeded; any other interrupted task needs human attention
// and is never re-executed automatically.
func (s *Service) RecoverInterruptedTasks() (int, error) {
	interrupted := false
	_ = s.store.View(func(snapshot domain.Snapshot) error {
		for _, task := range snapshot.Tasks {
			if task.State == domain.TaskRunning {
				interrupted = true
			}
		}
		return nil
	})
	if !interrupted {
		return 0, nil
	}

	now := s.clock.Now()
	recovered := 0
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		ids := make([]string, 0)
		for id, task := range snapshot.Tasks {
			if task.State == domain.TaskRunning {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			task := snapshot.Tasks[id]
			run, ok := snapshot.Runs[task.RunID]
			if ok && task.Kind == domain.ActionStartFermentation &&
				startFermentationFulfilled(run.State) {
				_ = task.Succeed("recovered after restart; the action was already fulfilled", now)
			} else {
				_ = task.Attend("execution was interrupted before an outcome was recorded; "+
					"verify the run and register a new action if it is still needed", now)
			}
			snapshot.Tasks[id] = task
			recovered++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return recovered, nil
}
