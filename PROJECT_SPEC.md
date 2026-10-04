# Cellar Run Control Specification

## Goal

Cellar Run Control is a local Go HTTP service for a small production cellar.
It coordinates approved formulas, vessel reservation, active fermentation
runs, gravity observations, conditioning, completion, and vessel release.

The project is a healthy initialization baseline. It contains no intentional
defects, no model traces, and no automated tests. Verification is intentionally
deferred to a later engineering task.

## Users

- Production planner: creates formulas, updates process steps, and approves a
  formula before it can be used.
- Cellar operator: registers vessels, creates runs, reserves a vessel, and
  starts fermentation.
- Fermentation observer: appends timestamped gravity observations and moves a
  run through conditioning and completion.
- Integration client: calls the stable JSON API from a local production script.

## Entities

### Formula

A formula contains an identifier, normalized name, style label, target original
gravity, target final gravity, maximum fermentation temperature, minimum and
maximum planned days, ordered process steps, lifecycle state, version, and
timestamps.

States are `draft`, `approved`, and `retired`.

- Names are unique after case folding and whitespace normalization.
- Target original gravity is in the inclusive range 1.030 to 1.120.
- Target final gravity is positive, lower than target original gravity, and
  differs from it by at least 0.005.
- Maximum fermentation temperature is in the inclusive range 5 to 40 Celsius.
- Planned duration is 1 to 120 days and the minimum cannot exceed the maximum.
- A formula has 1 to 24 process steps.
- Step sequence values must be contiguous and start at 1.
- Step temperatures are in the inclusive range 5 to 90 Celsius.
- Step hold times are 1 to 1440 minutes.
- `draft -> approved` requires a complete valid step list.
- An approved formula is immutable.
- `approved -> retired` is allowed only when no run currently uses it.

### Vessel

A vessel contains an identifier, unique display code, capacity in liters,
lifecycle state, active run identifier, version, and timestamps.

States are `available`, `reserved`, `in_use`, `cleaning`, and `retired`.

- Capacity is in the inclusive range 10 to 5000 liters.
- Only an available vessel can be reserved.
- A reserved or in-use vessel must identify exactly one active run.
- Completion moves an in-use vessel to cleaning.
- Cleaning can be finished only when the associated run is complete or
  aborted.
- Finishing cleaning releases the vessel to available.
- A vessel with an active run cannot be retired or directly reserved.

### FermentationRun

A run contains an identifier, unique run code, an approved formula identifier,
an optional vessel identifier, planned volume, notes, lifecycle state,
timetable, ordered observations, final metrics, version, and timestamps.

States are `planned`, `reserved`, `fermenting`, `conditioning`, `completed`,
and `aborted`.

- Planned volume is greater than zero and cannot exceed vessel capacity.
- Only an approved formula may be attached to a run.
- `planned -> reserved` atomically reserves an available vessel.
- `reserved -> fermenting` requires that the vessel still belongs to the run.
- `fermenting -> conditioning` requires at least two observations.
- `conditioning -> completed` requires at least three observations.
- Completion computes apparent attenuation and estimated alcohol by volume from
  the formula target original gravity and the last observation.
- Completion moves the vessel to cleaning without losing its run association.
- A non-completed run can be aborted. Aborting before fermentation releases an
  available vessel; aborting after fermentation schedules the vessel for
  cleaning.
- Completed and aborted runs are immutable.
- A run can carry at most 500 observations.

### GravityObservation

An observation contains a run identifier, contiguous sequence number, observed
time, gravity, temperature, optional note, and receipt time.

- Gravity is in the inclusive range 0.980 to 1.200.
- Temperature is in the inclusive range -5 to 60 Celsius.
- Observed time begins at or after the run start.
- Observation times strictly increase within a run.
- Gravity must not rise more than 0.010 above the immediately preceding value.
- Observation sequences are contiguous and start at 1.

### ScheduledAction

A scheduled action is a durable, one-shot queue entry that performs a run
transition at a set time. It contains an identifier, target run identifier,
action name, scheduled time, queue state, attempt count, optional note,
claim and finish timestamps, failure code and reason, version fields, and
timestamps.

States are `waiting`, `running`, `succeeded`, and `needs_attention`.

- The only action is `start_fermentation`, moving a reserved run into
  fermentation.
- A run can have at most one active (waiting or running) entry per action.
- Registration and the business change that enables it commit in one
  snapshot revision. In particular, a reservation can carry
  `scheduled_start_at`, so vessel reservation and schedule cannot split.
- A due pass claims the entry (`waiting -> running`) in its own committed
  transaction, then executes the transition and writes the terminal state
  (`succeeded` or `needs_attention`) together with the business change.
- An entry is attempted at most once. It is never retried.
- On startup or the next pass, every left-over `running` entry is parked as
  `needs_attention` with code `scheduled_action_interrupted`. A claim
  without a committed result might or might not have applied, so an operator
  must inspect the run before any further action.
- A waiting entry can be cancelled. Claimed and terminal entries cannot.
- Succeeded and needs-attention entries are retained and stay queryable.
- Domain transition guards remain authoritative, so a run moved manually,
  aborted, or otherwise out of state is never advanced twice; that attempt
  is parked with a stable failure code and reason.
- Each due entry is processed independently, so one parked entry cannot
  block the other due entries.

## Connected Workflows

### 1. Formula Approval

Public entry points are:

- `POST /v1/formulas`
- `PUT /v1/formulas/{id}/steps`
- `POST /v1/formulas/{id}/approve`
- `GET /v1/formulas/{id}`

The client creates a draft, sets its process step list atomically, approves it,
and reads the approved record. Invalid ranges, duplicate names, empty steps,
and repeated approval return stable error codes without partial writes.

This path crosses HTTP protocol handling, application orchestration, domain
rules, and snapshot storage.

### 2. Vessel Run Lifecycle

Public entry points are:

- `POST /v1/vessels`
- `POST /v1/runs`
- `POST /v1/runs/{id}/reserve`
- `POST /v1/runs/{id}/start`
- `GET /v1/runs/{id}`

The client registers a vessel, creates a planned run against an approved
formula, reserves the vessel, starts fermentation, and reads the run. Capacity
conflicts, an unavailable vessel, duplicate run codes, and stale versions
produce stable failures without changing vessel or run state.

This path crosses HTTP protocol handling, application orchestration, domain
rules, storage coordination, and resource lifecycle.

### 3. Observations And Completion

Public entry points are:

- `POST /v1/runs/{id}/observations`
- `POST /v1/runs/{id}/conditioning`
- `POST /v1/runs/{id}/complete`
- `GET /v1/runs/{id}`

The client appends three ordered observations, starts conditioning, completes
the run, and reads the calculated result. Non-monotonic times, rising gravity,
out-of-range values, excessive sequences, incomplete data, and repeated
transitions return stable failures without changing persisted state.

This path crosses HTTP protocol handling, application orchestration, numeric
domain rules, and snapshot storage.

### 4. Scheduled Fermentation Start

Public entry points are:

- `POST /v1/runs/{id}/reserve` (with `scheduled_start_at`)
- `POST /v1/runs/{id}/schedule`
- `GET /v1/scheduled-actions`
- `GET /v1/scheduled-actions/{id}`
- `POST /v1/scheduled-actions/{id}/cancel`
- `POST /v1/scheduler/tick`

The client reserves a vessel and registers a start time atomically, or
schedules an already-reserved run. At the scheduled time the local scheduler
claims the entry once and moves the run to fermentation. Entries that are due
while their run is no longer eligible are parked with a reason instead of
being retried, and they do not block other due entries. Duplicate active
registrations and cancellation of non-waiting entries return stable failures.

This path crosses HTTP protocol handling, background scheduling,
cross-process locking, use-case recovery rules, domain transition guards, and
snapshot storage.

## Modules

- `cmd/fermentctl`: executable entry point for the HTTP service and bounded
  workflow checks.
- `internal/config`: environment and command-line configuration.
- `internal/clock`: deterministic clock boundary for production and checks.
- `internal/domain`: entities, validation, state transitions, and metrics.
- `internal/service`: use-case orchestration and revision coordination.
- `internal/storage`: snapshot repository, atomic replacement, locking, and
  the cross-process scheduler lock.
- `internal/scheduler`: background loop that triggers scheduler passes.
- `internal/httpapi`: routing, strict JSON decoding, error mapping, and health.
- `internal/workflowcheck`: real HTTP workflow checks used by the executable.

The dependency direction is:

`cmd -> workflowcheck/httpapi -> service -> domain`

`service -> storage`

`storage -> domain`

The domain package does not import outer packages.

## API Behavior

All business routes are JSON under `/v1`. A successful write returns the
updated aggregate and its version. A failed write returns:

```json
{
  "error": {
    "code": "stable_machine_code",
    "detail": "human-readable explanation"
  }
}
```

Write requests may include `X-Expected-Version`. When present, the service
rejects a stale write instead of overwriting newer state.

`GET /healthz` returns process health and the current snapshot revision.

## Persistence And Concurrency

The configured data directory contains `state.json`. Writes are serialized by
an in-process mutex. The complete state is encoded, committed to durable
storage, and then atomically renamed over the target. An update is not
published in memory unless persistence succeeds.

Scheduled actions live in the same revisioned snapshot. Their claim and
terminal result are separate committed transitions, which is what makes a
scheduled transition at-most-once. Scheduler passes across processes are
serialized by an exclusive, non-blocking flock on `schedule.lock` in the
data directory; a controller that cannot acquire it skips its pass. The
kernel releases the lock if the process exits.

Reads use copied values and never expose repository-owned maps or slices. The
default address is `127.0.0.1:8088`, the request body limit is 1 MiB, and all
workflow checks use temporary directories.

## Validation Plan

The baseline declares `testing: deferred` and intentionally contains no unit
tests, fixtures, integration tests, or browser E2E files. A later verification
task owns systematic tests for validation boundaries, state transitions,
numeric calculations, stale revisions, storage failures, and concurrent
reservation attempts.

The current build command is `go build ./...`. Three bounded production checks
start the real HTTP handler, issue real JSON requests, check success and failure
behavior, and exit without external services:

- `go run ./cmd/fermentctl check formula-approval`
- `go run ./cmd/fermentctl check vessel-run-lifecycle`
- `go run ./cmd/fermentctl check observations-completion`
- `go run ./cmd/fermentctl check scheduled-start`

## Scope Limits

This service does not control physical equipment, connect to a remote identity
provider, send notifications, expose a browser interface, replicate data, or
replace laboratory measurements. The API is intended for a trusted local
network or an internal adapter.
