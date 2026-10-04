package service

import (
	"sort"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

func (s *Service) CreateRun(input CreateRunInput) (domain.FermentationRun, error) {
	now := s.clock.Now()
	code := domain.NormalizeRunCode(input.Code)
	if code == "" {
		generated, err := generatedRunCode()
		if err != nil {
			return domain.FermentationRun{}, domain.NewError(domain.CodeStorage, err.Error())
		}
		code = generated
	}
	run := domain.FermentationRun{
		Code:           code,
		FormulaID:      input.FormulaID,
		PlannedVolumeL: input.PlannedVolumeL,
		Notes:          input.Notes,
		State:          domain.RunPlanned,
		Observations:   []domain.GravityObservation{},
		Version:        1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := run.Validate(); err != nil {
		return domain.FermentationRun{}, err
	}
	id, err := generateEntityID("run")
	if err != nil {
		return domain.FermentationRun{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	run.ID = id

	err = s.store.Execute(func(snapshot *domain.Snapshot) error {
		formula, ok := snapshot.Formulas[run.FormulaID]
		if !ok {
			return domain.NewError(domain.CodeFormulaNotFound, "formula was not found")
		}
		if formula.State != domain.FormulaApproved {
			return domain.NewError(domain.CodeFormulaState,
				"run requires an approved formula")
		}
		if _, exists := snapshot.RunIndex[run.Code]; exists {
			return domain.NewError(domain.CodeRunConflict,
				"a run with the same code already exists")
		}
		snapshot.Runs[run.ID] = run.Clone()
		snapshot.RunIndex[run.Code] = run.ID
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return run.Clone(), nil
}

func (s *Service) ReserveRun(
	id string,
	input ReserveRunInput,
) (domain.FermentationRun, domain.Vessel, *domain.ScheduledAction, error) {
	var updatedRun domain.FermentationRun
	var updatedVessel domain.Vessel
	var scheduled *domain.ScheduledAction
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		vessel, ok := snapshot.Vessels[input.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if run.PlannedVolumeL > vessel.CapacityL {
			return domain.FieldError(domain.CodeInvalidInput,
				"planned_volume_liters", "exceeds vessel capacity")
		}
		if err := vessel.Reserve(run.ID, s.clock.Now()); err != nil {
			return err
		}
		if err := run.ReserveVessel(vessel.ID, s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		updatedRun = run.Clone()
		updatedVessel = vessel
		if input.ScheduledStartAt != nil {
			action, err := s.registerScheduledStart(snapshot, run, *input.ScheduledStartAt, "")
			if err != nil {
				return err
			}
			actionCopy := action.Clone()
			scheduled = &actionCopy
		}
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, nil, err
	}
	return updatedRun, updatedVessel, scheduled, nil
}

// registerScheduledStart appends a waiting start_fermentation entry to the
// snapshot. It runs inside an open snapshot transaction so that a run
// change and its scheduled transition either both persist or both do not.
func (s *Service) registerScheduledStart(
	snapshot *domain.Snapshot,
	run domain.FermentationRun,
	scheduledAt time.Time,
	note string,
) (domain.ScheduledAction, error) {
	if run.State == domain.RunFermenting || run.State == domain.RunConditioning ||
		run.State == domain.RunCompleted {
		return domain.ScheduledAction{}, domain.NewError(domain.CodeScheduleConflict,
			"run has already passed the reservable fermentation start")
	}
	if run.State == domain.RunAborted {
		return domain.ScheduledAction{}, domain.NewError(domain.CodeScheduleConflict,
			"an aborted run cannot be scheduled to start")
	}
	if err := rejectDuplicateSchedule(snapshot, run.ID, domain.ActionStartFermentation); err != nil {
		return domain.ScheduledAction{}, err
	}
	id, err := generateEntityID("sched")
	if err != nil {
		return domain.ScheduledAction{}, domain.NewError(domain.CodeStorage, err.Error())
	}
	action, err := domain.NewScheduledAction(id, domain.ScheduledActionRegistration{
		RunID:       run.ID,
		Action:      domain.ActionStartFermentation,
		ScheduledAt: scheduledAt,
		Note:        note,
	}, s.clock.Now())
	if err != nil {
		return domain.ScheduledAction{}, err
	}
	snapshot.ScheduledActions[action.ID] = action
	return action, nil
}

// rejectDuplicateSchedule refuses a second active (waiting or running)
// registration for the same run and action, so an entry cannot be claimed
// twice through different records.
func rejectDuplicateSchedule(
	snapshot *domain.Snapshot,
	runID string,
	action domain.ScheduleAction,
) error {
	for _, existing := range snapshot.ScheduledActions {
		if existing.RunID != runID || existing.Action != action {
			continue
		}
		if existing.State == domain.ScheduleWaiting || existing.State == domain.ScheduleRunning {
			return domain.NewError(domain.CodeScheduleConflict,
				"an active scheduled action for this run already exists")
		}
	}
	return nil
}

func (s *Service) StartRun(
	id string,
	input VersionInput,
) (domain.FermentationRun, domain.Vessel, error) {
	var updatedRun domain.FermentationRun
	var updatedVessel domain.Vessel
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		if run.VesselID == nil {
			return domain.NewError(domain.CodeRunState, "run has no vessel")
		}
		vessel, ok := snapshot.Vessels[*run.VesselID]
		if !ok {
			return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
		}
		if err := vessel.StartRun(run.ID, s.clock.Now()); err != nil {
			return err
		}
		if err := run.StartFermentation(s.clock.Now()); err != nil {
			return err
		}
		snapshot.Runs[id] = run.Clone()
		snapshot.Vessels[vessel.ID] = vessel
		updatedRun = run.Clone()
		updatedVessel = vessel
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, err
	}
	return updatedRun, updatedVessel, nil
}

func (s *Service) AbortRun(
	id string,
	input VersionInput,
) (domain.FermentationRun, domain.Vessel, error) {
	var updatedRun domain.FermentationRun
	var updatedVessel domain.Vessel
	err := s.store.Execute(func(snapshot *domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		if err := s.checkExpected(run.Version, input.ExpectedVersion); err != nil {
			return err
		}
		hadStarted := run.HadStarted()
		if err := run.Abort(s.clock.Now()); err != nil {
			return err
		}
		if run.VesselID != nil {
			vessel, ok := snapshot.Vessels[*run.VesselID]
			if !ok {
				return domain.NewError(domain.CodeVesselNotFound, "vessel was not found")
			}
			if hadStarted {
				if err := vessel.StartCleaning(run.ID, s.clock.Now()); err != nil {
					return err
				}
			} else if err := vessel.ReleaseReservation(run.ID, s.clock.Now()); err != nil {
				return err
			}
			snapshot.Vessels[vessel.ID] = vessel
			updatedVessel = vessel
		}
		snapshot.Runs[id] = run.Clone()
		updatedRun = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, domain.Vessel{}, err
	}
	return updatedRun, updatedVessel, nil
}

func (s *Service) GetRun(id string) (domain.FermentationRun, error) {
	var found domain.FermentationRun
	err := s.store.View(func(snapshot domain.Snapshot) error {
		run, ok := snapshot.Runs[id]
		if !ok {
			return domain.NewError(domain.CodeRunNotFound, "run was not found")
		}
		found = run.Clone()
		return nil
	})
	if err != nil {
		return domain.FermentationRun{}, err
	}
	return found, nil
}

func (s *Service) ListRuns() ([]domain.FermentationRun, error) {
	var runs []domain.FermentationRun
	err := s.store.View(func(snapshot domain.Snapshot) error {
		runs = make([]domain.FermentationRun, 0, len(snapshot.Runs))
		for _, run := range snapshot.Runs {
			runs = append(runs, run.Clone())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].Code == runs[j].Code {
			return runs[i].ID < runs[j].ID
		}
		return runs[i].Code < runs[j].Code
	})
	return runs, nil
}
