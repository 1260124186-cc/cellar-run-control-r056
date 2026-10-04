package service

import (
	"errors"
	"sort"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// maxDueActionsPerPass bounds how many waiting entries one scheduler pass
// claims, so a large backlog cannot hold the snapshot mutex for long and
// block API traffic. Remaining entries run on the next pass.
const maxDueActionsPerPass = 100

// ScheduleRunAction registers a standalone one-shot action for a run.
// It is validated and persisted in the same snapshot transaction.
func (s *Service) ScheduleRunAction(
	runID string,
	input ScheduleRunInput,
) (domain.ScheduledAction, error) {
	var created domain.ScheduledAction
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[runID]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		switch input.Action {
		case domain.ActionStartFermentation:
			action, err := s.registerScheduledStart(snapshot, run, input.ScheduledAt, input.Note)
			if err != nil {
				return err
			}
			created = action.Clone()
			return nil
		default:
			return domain.FieldError(domain.CodeInvalidInput, "action",
				"is not a supported scheduled action")
		}
	})
	if err != nil {
		return domain.ScheduledAction{}, err
	}
	return created, nil
}

// GetScheduledAction reads a single queue entry.
func (s *Service) GetScheduledAction(id string) (domain.ScheduledAction, error) {
	var found domain.ScheduledAction
	err := s.store.View(func(snapshot domain.Snapshot) error {
		action, ok := snapshot.ScheduledActions[id]
		if !ok {
			return domain.NewError(domain.CodeScheduleNotFound, "scheduled action was not found")
		}
		found = action.Clone()
		return nil
	})
	if err != nil {
		return domain.ScheduledAction{}, err
	}
	return found, nil
}

// ListScheduledActions returns the queryable queue, newest first by
// scheduled time. Terminal entries are retained for inspection.
func (s *Service) ListScheduledActions(filter domain.ScheduleFilter) ([]domain.ScheduledAction, error) {
	var actions []domain.ScheduledAction
	err := s.store.View(func(snapshot domain.Snapshot) error {
		actions = make([]domain.ScheduledAction, 0, len(snapshot.ScheduledActions))
		for _, action := range snapshot.ScheduledActions {
			if filter.RunID != "" && action.RunID != filter.RunID {
				continue
			}
			if filter.State != "" && action.State != filter.State {
				continue
			}
			actions = append(actions, action.Clone())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(actions, func(i, j int) bool {
		if !actions[i].ScheduledAt.Equal(actions[j].ScheduledAt) {
			return actions[i].ScheduledAt.After(actions[j].ScheduledAt)
		}
		return actions[i].ID < actions[j].ID
	})
	return actions, nil
}

// CancelScheduledAction removes a waiting entry. Claimed and terminal
// entries cannot be cancelled because an attempt may already be underway.
func (s *Service) CancelScheduledAction(id string) error {
	return s.store.Execute(func(snapshot *domain.Snapshot) error {
		action, ok := snapshot.ScheduledActions[id]
		if !ok {
			return domain.NewError(domain.CodeScheduleNotFound, "scheduled action was not found")
		}
		if action.State != domain.ScheduleWaiting {
			return domain.NewError(domain.CodeScheduleState,
				"only a waiting action can be cancelled")
		}
		delete(snapshot.ScheduledActions, id)
		return nil
	})
}

// TriggerScheduledActions runs one scheduler pass:
//
//  1. Acquire the cross-process lock without waiting (other controllers
//     skip).
//  2. Recover every left-over running entry to needs_attention so a
//     half-finished attempt is never applied twice.
//  3. Claim and execute due waiting entries, each in its own transaction,
//     so one failure parks only that entry and never blocks the rest.
//
// The business transition and the terminal result are committed in the
// same transaction; the claim is committed on its own just before.
func (s *Service) TriggerScheduledActions() (domain.ScheduleReport, error) {
	locked, err := s.scheduleLock.TryLock()
	if err != nil {
		return domain.ScheduleReport{}, err
	}
	if !locked {
		return domain.ScheduleReport{Skipped: true}, nil
	}
	defer func() { _ = s.scheduleLock.Unlock() }()

	now := s.clock.Now()
	report := domain.ScheduleReport{}

	recovered, err := s.recoverRunningActions(now)
	if err != nil {
		return domain.ScheduleReport{}, err
	}
	report.Recovered = recovered

	due, err := s.dueWaitingActions(now)
	if err != nil {
		return domain.ScheduleReport{}, err
	}
	report.Due = len(due)

	for _, action := range due {
		claim := action
		if claimErr := s.claimScheduledAction(&claim, now); claimErr != nil {
			// The entry vanished or left the waiting state under us; a
			// storage failure aborts this pass but leaves nothing claimed.
			if domain.IsCode(claimErr, domain.CodeStorage) {
				return report, claimErr
			}
			continue
		}

		outcome := s.executeClaimedAction(claim, now)
		report.Results = append(report.Results, outcome)
		switch outcome.State {
		case domain.ScheduleSucceeded:
			report.Succeeded++
		case domain.ScheduleNeedsAttention:
			report.NeedsAttention++
		}
	}
	return report, nil
}

// recoverRunningActions parks every running entry. Running only exists on
// disk when a previous pass (or the process running it) vanished between
// the claim commit and the result commit. Retrying would risk driving the
// transition twice, so the entry is handed to an operator instead.
func (s *Service) recoverRunningActions(now time.Time) (int, error) {
	var ids []string
	err := s.store.View(func(snapshot domain.Snapshot) error {
		for id, action := range snapshot.ScheduledActions {
			if action.State == domain.ScheduleRunning {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		err := s.store.Execute(func(snapshot *domain.Snapshot) error {
			action, ok := snapshot.ScheduledActions[id]
			if !ok || action.State != domain.ScheduleRunning {
				return nil
			}
			action.MarkInterrupted(now)
			snapshot.ScheduledActions[id] = action
			return nil
		})
		if err != nil {
			return len(ids), err
		}
	}
	return len(ids), nil
}

func (s *Service) dueWaitingActions(now time.Time) ([]domain.ScheduledAction, error) {
	var due []domain.ScheduledAction
	err := s.store.View(func(snapshot domain.Snapshot) error {
		for _, action := range snapshot.ScheduledActions {
			if action.State == domain.ScheduleWaiting &&
				!action.ScheduledAt.After(now) {
				due = append(due, action.Clone())
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].ScheduledAt.Equal(due[j].ScheduledAt) {
			return due[i].ScheduledAt.Before(due[j].ScheduledAt)
		}
		return due[i].ID < due[j].ID
	})
	if len(due) > maxDueActionsPerPass {
		due = due[:maxDueActionsPerPass]
	}
	return due, nil
}

// claimScheduledAction commits the waiting -> running transition in its
// own transaction. If the commit fails, no business work follows, so the
// transition is guaranteed to be attempted at most once.
func (s *Service) claimScheduledAction(action *domain.ScheduledAction, now time.Time) error {
	return s.store.Execute(func(snapshot *domain.Snapshot) error {
		current, ok := snapshot.ScheduledActions[action.ID]
		if !ok {
			return domain.NewError(domain.CodeScheduleNotFound, "scheduled action was not found")
		}
		if current.State != domain.ScheduleWaiting {
			return domain.NewError(domain.CodeScheduleState, "action is no longer waiting")
		}
		if err := current.Claim(now); err != nil {
			return err
		}
		snapshot.ScheduledActions[current.ID] = current
		*action = current.Clone()
		return nil
	})
}

// executeClaimedAction performs the transition and commits its result in a
// single transaction. A failure (business rule, unexpected panic, or
// storage error) parks the entry instead of retrying it.
func (s *Service) executeClaimedAction(
	action domain.ScheduledAction,
	now time.Time,
) domain.ScheduleResult {
	result := domain.ScheduleResult{ActionID: action.ID, RunID: action.RunID}

	persistErr := s.finalizeClaimedAction(action.ID, now)
	if domain.IsCode(persistErr, domain.CodeStorage) {
		// The outcome may or may not be on disk. The next recovery treats
		// the still-running entry as interrupted rather than retrying.
		result.State = domain.ScheduleNeedsAttention
		result.Reason = "result could not be persisted; the entry is parked for recovery"
		return result
	}

	// Re-read to report the state that actually committed.
	if committed, readErr := s.GetScheduledAction(action.ID); readErr == nil {
		result.State = committed.State
		result.Reason = committed.FailureReason
	}
	return result
}

// finalizeClaimedAction runs the transition and writes the terminal queue
// state atomically. A panic inside the transition is converted to a
// needs_attention outcome so one faulty entry cannot crash the pass.
func (s *Service) finalizeClaimedAction(actionID string, now time.Time) (err error) {
	return s.store.Execute(func(snapshot *domain.Snapshot) error {
		action, ok := snapshot.ScheduledActions[actionID]
		if !ok {
			return domain.NewError(domain.CodeScheduleNotFound, "scheduled action was not found")
		}
		if action.State != domain.ScheduleRunning {
			return nil
		}

		var outcomeErr error
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					outcomeErr = domain.NewError(domain.CodeScheduleState,
						"unexpected failure while executing the action")
				}
			}()
			outcomeErr = s.applyScheduledTransition(snapshot, action, now)
		}()

		if outcomeErr == nil {
			action.MarkSucceeded(now)
		} else {
			code := string(domain.CodeScheduleState)
			var domainErr *domain.Error
			if errors.As(outcomeErr, &domainErr) {
				code = string(domainErr.Code)
			}
			action.MarkNeedsAttention(code, outcomeErr.Error(), now)
		}
		snapshot.ScheduledActions[action.ID] = action
		return nil
	})
}

// applyScheduledTransition executes one action against the snapshot. All
// domain guards still apply, so a run that an operator moved manually
// cannot be moved a second time.
func (s *Service) applyScheduledTransition(
	snapshot *domain.Snapshot,
	action domain.ScheduledAction,
	now time.Time,
) error {
	run, ok := snapshot.Runs[action.RunID]
	if !ok {
		return domain.NewError(domain.CodeRunNotFound, "run was not found")
	}

	switch action.Action {
	case domain.ActionStartFermentation:
		// Already fermenting (or beyond) counts as success-by-guard: the
		// intended transition has effectively happened, e.g. an operator
		// started it manually between claim and execution.
		if run.State == domain.RunFermenting || run.State == domain.RunConditioning ||
			run.State == domain.RunCompleted {
			return nil
		}
		if run.State != domain.RunReserved || run.VesselID == nil {
			return domain.NewError(domain.CodeRunState,
				"run is no longer reserved and cannot start fermentation")
		}
		vessel, ok := snapshot.Vessels[*run.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := vessel.StartRun(run.ID, now); err != nil {
			return err
		}
		if err := run.StartFermentation(now); err != nil {
			return err
		}
		snapshot.Runs[run.ID] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		return nil
	default:
		return domain.NewError(domain.CodeInvalidInput, "unsupported scheduled action")
	}
}
