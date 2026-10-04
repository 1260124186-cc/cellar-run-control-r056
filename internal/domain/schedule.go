package domain

import "time"

type ScheduleState string

const (
	ScheduleWaiting        ScheduleState = "waiting"
	ScheduleRunning        ScheduleState = "running"
	ScheduleSucceeded      ScheduleState = "succeeded"
	ScheduleNeedsAttention ScheduleState = "needs_attention"
)

// ScheduleAction names the transition a scheduled entry is allowed to drive.
// The scheduler executes every entry at most once, so only idempotently
// guarded lifecycle transitions are accepted.
type ScheduleAction string

const (
	// ActionStartFermentation moves a reserved run into fermentation.
	ActionStartFermentation ScheduleAction = "start_fermentation"
)

const maxScheduleReasonRunes = 500

// ScheduledAction is a durable queue entry for one one-shot, at-most-once
// transition. A claimed entry is never retried: if the process disappears
// while it is running, recovery routes it to NeedsAttention instead.
type ScheduledAction struct {
	ID            string         `json:"id"`
	RunID         string         `json:"run_id"`
	Action        ScheduleAction `json:"action"`
	ScheduledAt   time.Time      `json:"scheduled_at"`
	State         ScheduleState  `json:"state"`
	Note          string         `json:"note,omitempty"`
	Attempts      int            `json:"attempts"`
	ClaimedAt     *time.Time     `json:"claimed_at,omitempty"`
	LastAttemptAt *time.Time     `json:"last_attempt_at,omitempty"`
	FinishedAt    *time.Time     `json:"finished_at,omitempty"`
	FailureCode   string         `json:"failure_code,omitempty"`
	FailureReason string         `json:"failure_reason,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// ScheduledActionRegistration carries the client-supplied fields of a new
// queue entry.
type ScheduledActionRegistration struct {
	RunID       string
	Action      ScheduleAction
	ScheduledAt time.Time
	Note        string
}

// ScheduleFilter narrows a queue listing. Empty fields are not applied.
type ScheduleFilter struct {
	RunID string
	State ScheduleState
}

// ScheduleReport summarizes one scheduler pass.
type ScheduleReport struct {
	Skipped        bool             `json:"skipped"`
	Recovered      int              `json:"recovered"`
	Due            int              `json:"due"`
	Succeeded      int              `json:"succeeded"`
	NeedsAttention int              `json:"needs_attention"`
	Results        []ScheduleResult `json:"results,omitempty"`
}

// ScheduleResult records the terminal outcome of one due entry in a pass.
type ScheduleResult struct {
	ActionID string        `json:"action_id"`
	RunID    string        `json:"run_id"`
	State    ScheduleState `json:"state"`
	Reason   string        `json:"reason,omitempty"`
}

func (a ScheduledAction) Clone() ScheduledAction {
	cloned := a
	if a.ClaimedAt != nil {
		value := *a.ClaimedAt
		cloned.ClaimedAt = &value
	}
	if a.LastAttemptAt != nil {
		value := *a.LastAttemptAt
		cloned.LastAttemptAt = &value
	}
	if a.FinishedAt != nil {
		value := *a.FinishedAt
		cloned.FinishedAt = &value
	}
	return cloned
}

// IsKnownScheduleAction reports whether action can be executed.
func IsKnownScheduleAction(action ScheduleAction) bool {
	return action == ActionStartFermentation
}

// IsKnownScheduleState reports whether state is a queue state.
func IsKnownScheduleState(state ScheduleState) bool {
	switch state {
	case ScheduleWaiting, ScheduleRunning, ScheduleSucceeded, ScheduleNeedsAttention:
		return true
	default:
		return false
	}
}

// NewScheduledAction builds a waiting entry and validates the fields that do
// not depend on snapshot contents.
func NewScheduledAction(
	id string,
	registration ScheduledActionRegistration,
	now time.Time,
) (ScheduledAction, error) {
	if !IsKnownScheduleAction(registration.Action) {
		return ScheduledAction{}, FieldError(CodeInvalidInput, "action",
			"is not a supported scheduled action")
	}
	if registration.ScheduledAt.IsZero() {
		return ScheduledAction{}, FieldError(CodeInvalidInput, "scheduled_at",
			"is required")
	}
	if len([]rune(registration.Note)) > maxScheduleReasonRunes {
		return ScheduledAction{}, FieldError(CodeInvalidInput, "note",
			"must not exceed 500 characters")
	}
	stamp := now.UTC()
	return ScheduledAction{
		ID:          id,
		RunID:       registration.RunID,
		Action:      registration.Action,
		ScheduledAt: registration.ScheduledAt.UTC(),
		State:       ScheduleWaiting,
		Note:        registration.Note,
		Attempts:    0,
		CreatedAt:   stamp,
		UpdatedAt:   stamp,
	}, nil
}

// Claim moves a waiting entry to running and records the single attempt.
// The state check is the in-process half of the at-most-once guarantee; the
// claim happens inside a persisted snapshot transaction.
func (a *ScheduledAction) Claim(now time.Time) error {
	if a.State != ScheduleWaiting {
		return NewError(CodeScheduleState, "only a waiting action can be claimed")
	}
	stamp := now.UTC()
	a.State = ScheduleRunning
	a.Attempts++
	a.ClaimedAt = &stamp
	a.LastAttemptAt = &stamp
	a.UpdatedAt = stamp
	return nil
}

// MarkSucceeded records a completed attempt.
func (a *ScheduledAction) MarkSucceeded(now time.Time) {
	stamp := now.UTC()
	a.State = ScheduleSucceeded
	a.FinishedAt = &stamp
	a.FailureCode = ""
	a.FailureReason = ""
	a.UpdatedAt = stamp
}

// MarkNeedsAttention records a failed attempt and parks the entry for a
// human. It is never retried by the scheduler.
func (a *ScheduledAction) MarkNeedsAttention(code, reason string, now time.Time) {
	stamp := now.UTC()
	a.State = ScheduleNeedsAttention
	a.FinishedAt = &stamp
	a.FailureCode = code
	a.FailureReason = truncateRunes(reason, maxScheduleReasonRunes)
	a.UpdatedAt = stamp
}

// MarkInterrupted is the crash-recovery transition for a claim whose
// execution result never reached disk. The entry is parked rather than
// retried so the transition cannot be applied twice.
func (a *ScheduledAction) MarkInterrupted(now time.Time) {
	a.MarkNeedsAttention(string(CodeScheduleInterrupted),
		"the process stopped while the action was running; "+
			"the run state must be checked manually before retrying", now)
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
