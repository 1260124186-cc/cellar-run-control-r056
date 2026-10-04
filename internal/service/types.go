package service

import (
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

type CreateFormulaInput struct {
	Name                  string
	Style                 string
	TargetOriginalGravity float64
	TargetFinalGravity    float64
	MaxFermentationTempC  float64
	MinimumDays           int
	MaximumDays           int
}

type SetFormulaStepsInput struct {
	ExpectedVersion *int64
	Steps           []domain.FormulaStep
}

type VersionInput struct {
	ExpectedVersion *int64
}

type CreateVesselInput struct {
	Code      string
	CapacityL float64
}

type CreateRunInput struct {
	Code           string
	FormulaID      string
	PlannedVolumeL float64
	Notes          string
}

type ReserveRunInput struct {
	ExpectedVersion *int64
	VesselID        string
}

type ObservationInput struct {
	Sequence     int
	Gravity      float64
	TemperatureC float64
	ObservedAt   time.Time
	Note         string
}

type FinishCleaningInput struct {
	ExpectedVersion *int64
}

type ScheduleActionInput struct {
	RunID string
	Kind  domain.ActionKind
	DueAt time.Time
}

type TaskFilter struct {
	RunID string
	State domain.TaskState
}

// DispatchSummary reports one pass over the due queue. Claimed counts the
// tasks this controller claimed; Succeeded and NeedsAttention count the
// terminal outcomes it recorded; Failed counts internal errors where the
// outcome could not be persisted.
type DispatchSummary struct {
	Claimed        int `json:"claimed"`
	Succeeded      int `json:"succeeded"`
	NeedsAttention int `json:"needs_attention"`
	Failed         int `json:"failed"`
}
