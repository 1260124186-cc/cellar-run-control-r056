package domain

import "time"

type TaskState string

const (
	TaskPending        TaskState = "pending"
	TaskRunning        TaskState = "running"
	TaskSucceeded      TaskState = "succeeded"
	TaskNeedsAttention TaskState = "needs_attention"
)

func (s TaskState) Valid() bool {
	switch s {
	case TaskPending, TaskRunning, TaskSucceeded, TaskNeedsAttention:
		return true
	}
	return false
}

type ActionKind string

const ActionStartFermentation ActionKind = "start_fermentation"

func (k ActionKind) Valid() bool {
	switch k {
	case ActionStartFermentation:
		return true
	}
	return false
}

// ScheduledTask is a registered one-shot action for a run. A task is
// attempted exactly once at or after its due time; the outcome stays in the
// queue so operators can query waiting, executing, succeeded, and
// attention-needed results.
type ScheduledTask struct {
	ID         string     `json:"id"`
	RunID      string     `json:"run_id"`
	Kind       ActionKind `json:"kind"`
	State      TaskState  `json:"state"`
	DueAt      time.Time  `json:"due_at"`
	ClaimToken string     `json:"claim_token,omitempty"`
	ClaimedAt  *time.Time `json:"claimed_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	ResultNote string     `json:"result_note,omitempty"`
	Version    int64      `json:"version"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

func (t ScheduledTask) Clone() ScheduledTask {
	cloned := t
	if t.ClaimedAt != nil {
		value := *t.ClaimedAt
		cloned.ClaimedAt = &value
	}
	if t.FinishedAt != nil {
		value := *t.FinishedAt
		cloned.FinishedAt = &value
	}
	return cloned
}

func (t ScheduledTask) Validate() error {
	if t.RunID == "" {
		return FieldError(CodeInvalidInput, "run_id", "must identify a run")
	}
	if !t.Kind.Valid() {
		return FieldError(CodeInvalidInput, "kind", "must be a known action kind")
	}
	if t.DueAt.IsZero() {
		return FieldError(CodeInvalidInput, "due_at", "must be a valid time")
	}
	return nil
}

// Claim moves a waiting task into execution. The token fences the claimant
// so only the controller that recorded the claim can finish the task.
func (t *ScheduledTask) Claim(token string, now time.Time) error {
	if t.State != TaskPending {
		return NewError(CodeTaskState, "only a waiting task can be claimed")
	}
	stamp := now.UTC()
	t.State = TaskRunning
	t.ClaimToken = token
	t.ClaimedAt = &stamp
	t.UpdatedAt = stamp
	t.Version++
	return nil
}

func (t *ScheduledTask) Succeed(note string, now time.Time) error {
	if t.State != TaskRunning {
		return NewError(CodeTaskState, "only an executing task can succeed")
	}
	stamp := now.UTC()
	t.State = TaskSucceeded
	t.ResultNote = note
	t.Reason = ""
	t.FinishedAt = &stamp
	t.UpdatedAt = stamp
	t.Version++
	return nil
}

func (t *ScheduledTask) Attend(reason string, now time.Time) error {
	if t.State != TaskRunning {
		return NewError(CodeTaskState, "only an executing task can need attention")
	}
	stamp := now.UTC()
	t.State = TaskNeedsAttention
	t.Reason = reason
	t.FinishedAt = &stamp
	t.UpdatedAt = stamp
	t.Version++
	return nil
}
