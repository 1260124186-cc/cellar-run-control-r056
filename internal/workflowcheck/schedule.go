package workflowcheck

import (
	"fmt"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

type scheduledActionView struct {
	ID            string                `json:"id"`
	RunID         string                `json:"run_id"`
	Action        domain.ScheduleAction `json:"action"`
	State         domain.ScheduleState  `json:"state"`
	Attempts      int                   `json:"attempts"`
	FailureCode   string                `json:"failure_code"`
	FailureReason string                `json:"failure_reason"`
}

type reserveScheduleView struct {
	Run             runView              `json:"run"`
	Vessel          vesselView           `json:"vessel"`
	ScheduledAction *scheduledActionView `json:"scheduled_action"`
}

type tickReportView struct {
	Skipped        bool                 `json:"skipped"`
	Recovered      int                  `json:"recovered"`
	Due            int                  `json:"due"`
	Succeeded      int                  `json:"succeeded"`
	NeedsAttention int                  `json:"needs_attention"`
	Results        []scheduleResultView `json:"results"`
}

type scheduleResultView struct {
	ActionID string               `json:"action_id"`
	State    domain.ScheduleState `json:"state"`
}

type scheduledActionList struct {
	ScheduledActions []scheduledActionView `json:"scheduled_actions"`
}

// scheduledStart exercises the durable local scheduler end to end over the
// real HTTP API: one due action succeeds, a later due action whose run was
// aborted is parked without retry and without blocking the first, and a
// future action stays untouched until cancelled.
func scheduledStart(api *client) error {
	formula, err := ensureApprovedFormula(api, "Scheduled Start Formula")
	if err != nil {
		return err
	}

	vesselResponse, err := api.call("POST", "/v1/vessels", map[string]any{
		"code":            "VSL-201",
		"capacity_liters": 500.0,
	}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(vesselResponse, 201, "create vessel"); err != nil {
		return err
	}
	var vessel vesselView
	if err := vesselResponse.decode(&vessel); err != nil {
		return err
	}

	runA, err := createScheduledRun(api, formula.ID, "RUN-SCHED-A")
	if err != nil {
		return err
	}
	runB, err := createScheduledRun(api, formula.ID, "RUN-SCHED-B")
	if err != nil {
		return err
	}
	runC, err := createScheduledRun(api, formula.ID, "RUN-SCHED-C")
	if err != nil {
		return err
	}

	// A is already due at reservation time; B is an hour out. The schedule
	// must commit in the same revision as the reservation.
	dueAt := time.Now().Add(-1 * time.Minute).UTC()
	futureAt := time.Now().Add(1 * time.Hour).UTC()

	reserveA, err := api.call("POST", "/v1/runs/"+runA.ID+"/reserve", map[string]any{
		"vessel_id":          vessel.ID,
		"scheduled_start_at": dueAt,
	}, ptrInt64(1))
	if err != nil {
		return err
	}
	if err := requireStatus(reserveA, 200, "reserve run A with schedule"); err != nil {
		return err
	}
	var reserveAView reserveScheduleView
	if err := reserveA.decode(&reserveAView); err != nil {
		return err
	}
	if err := requireRunState(reserveAView.Run, domain.RunReserved); err != nil {
		return err
	}
	if reserveAView.ScheduledAction == nil ||
		reserveAView.ScheduledAction.State != domain.ScheduleWaiting {
		return errInvalidWorkflow("reservation did not register a waiting action")
	}
	actionA := *reserveAView.ScheduledAction

	reserveB, err := api.call("POST", "/v1/runs/"+runB.ID+"/reserve", map[string]any{
		"vessel_id":          vessel.ID,
		"scheduled_start_at": futureAt,
	}, ptrInt64(1))
	if err != nil {
		return err
	}
	if err := requireCode(reserveB, 409, string(domain.CodeVesselUnavailable),
		"reserve a second run onto the same vessel"); err != nil {
		return err
	}

	// A vessel is occupied, so give B and C their own vessels.
	vesselB, err := createSchedulerVessel(api, "VSL-202")
	if err != nil {
		return err
	}
	vesselC, err := createSchedulerVessel(api, "VSL-203")
	if err != nil {
		return err
	}

	reserveB2, err := api.call("POST", "/v1/runs/"+runB.ID+"/reserve", map[string]any{
		"vessel_id":          vesselB.ID,
		"scheduled_start_at": futureAt,
	}, ptrInt64(1))
	if err != nil {
		return err
	}
	if err := requireStatus(reserveB2, 200, "reserve run B with future schedule"); err != nil {
		return err
	}
	var reserveBView reserveScheduleView
	if err := reserveB2.decode(&reserveBView); err != nil {
		return err
	}
	actionB := *reserveBView.ScheduledAction

	reserveC, err := api.call("POST", "/v1/runs/"+runC.ID+"/reserve", map[string]any{
		"vessel_id": vesselC.ID,
	}, ptrInt64(1))
	if err != nil {
		return err
	}
	if err := requireStatus(reserveC, 200, "reserve run C without schedule"); err != nil {
		return err
	}

	// Standalone registration for an already-reserved run.
	scheduleC, err := api.call("POST", "/v1/runs/"+runC.ID+"/schedule", map[string]any{
		"action":       string(domain.ActionStartFermentation),
		"scheduled_at": dueAt,
	}, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(scheduleC, 201, "schedule run C"); err != nil {
		return err
	}
	var actionC scheduledActionView
	if err := scheduleC.decode(&actionC); err != nil {
		return err
	}

	duplicate, err := api.call("POST", "/v1/runs/"+runC.ID+"/schedule", map[string]any{
		"action":       string(domain.ActionStartFermentation),
		"scheduled_at": dueAt,
	}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(duplicate, 409, string(domain.CodeScheduleConflict),
		"register a duplicate active schedule"); err != nil {
		return err
	}

	// Abort C after scheduling it: its due action must now fail and park.
	abortC, err := api.call("POST", "/v1/runs/"+runC.ID+"/abort", nil, ptrInt64(2))
	if err != nil {
		return err
	}
	if err := requireStatus(abortC, 200, "abort run C before its schedule fires"); err != nil {
		return err
	}

	tickResponse, err := api.call("POST", "/v1/scheduler/tick", nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(tickResponse, 200, "trigger scheduler pass"); err != nil {
		return err
	}
	var report tickReportView
	if err := tickResponse.decode(&report); err != nil {
		return err
	}
	if report.Skipped || report.Recovered != 0 || report.Due != 2 {
		return fmt.Errorf("unexpected pass summary: %+v", report)
	}
	if report.Succeeded != 1 || report.NeedsAttention != 1 {
		return fmt.Errorf("expected one success and one parked action, got: %+v", report)
	}

	// A advanced exactly once; its vessel moved with it.
	readA, err := api.call("GET", "/v1/runs/"+runA.ID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(readA, 200, "read run A after tick"); err != nil {
		return err
	}
	var runAView runView
	if err := readA.decode(&runAView); err != nil {
		return err
	}
	if err := requireRunState(runAView, domain.RunFermenting); err != nil {
		return err
	}
	if runAView.Version != 3 {
		return fmt.Errorf("run A should have advanced exactly once to version 3, got %d",
			runAView.Version)
	}

	vesselAResponse, err := api.call("GET", "/v1/vessels/"+vessel.ID, nil, nil)
	if err != nil {
		return err
	}
	var vesselAView vesselView
	if err := vesselAResponse.decode(&vesselAView); err != nil {
		return err
	}
	if err := requireVesselState(vesselAView, domain.VesselInUse); err != nil {
		return err
	}

	// B is still reserved because its time has not come.
	readB, err := api.call("GET", "/v1/runs/"+runB.ID, nil, nil)
	if err != nil {
		return err
	}
	var runBView runView
	if err := readB.decode(&runBView); err != nil {
		return err
	}
	if err := requireRunState(runBView, domain.RunReserved); err != nil {
		return err
	}

	// C parked with a reason and exactly one attempt.
	parkedResponse, err := api.call("GET", "/v1/scheduled-actions/"+actionC.ID, nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(parkedResponse, 200, "read parked action C"); err != nil {
		return err
	}
	var parked scheduledActionView
	if err := parkedResponse.decode(&parked); err != nil {
		return err
	}
	if parked.State != domain.ScheduleNeedsAttention || parked.Attempts != 1 ||
		parked.FailureReason == "" {
		return fmt.Errorf("aborted run action should park with a reason after one attempt: %+v",
			parked)
	}

	// Terminal entries stay queryable through the queue filter.
	successListResponse, err := api.call("GET",
		"/v1/scheduled-actions?state=succeeded", nil, nil)
	if err != nil {
		return err
	}
	var successList scheduledActionList
	if err := successListResponse.decode(&successList); err != nil {
		return err
	}
	if !listContainsAction(successList.ScheduledActions, actionA.ID) {
		return errInvalidWorkflow("succeeded action was not retained in the queue")
	}

	// A second pass must not retry the parked action or re-advance A.
	secondTick, err := api.call("POST", "/v1/scheduler/tick", nil, nil)
	if err != nil {
		return err
	}
	var secondReport tickReportView
	if err := secondTick.decode(&secondReport); err != nil {
		return err
	}
	if secondReport.Due != 0 || secondReport.Succeeded != 0 ||
		secondReport.NeedsAttention != 0 {
		return fmt.Errorf("the second pass had nothing due but reported: %+v", secondReport)
	}

	// B's future action can be cancelled while waiting.
	cancelB, err := api.call("POST",
		"/v1/scheduled-actions/"+actionB.ID+"/cancel", nil, nil)
	if err != nil {
		return err
	}
	if err := requireStatus(cancelB, 204, "cancel waiting action B"); err != nil {
		return err
	}
	cancelAgain, err := api.call("POST",
		"/v1/scheduled-actions/"+actionB.ID+"/cancel", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(cancelAgain, 404, string(domain.CodeScheduleNotFound),
		"cancel an already removed action"); err != nil {
		return err
	}

	// A terminal action cannot be cancelled.
	cancelTerminal, err := api.call("POST",
		"/v1/scheduled-actions/"+actionA.ID+"/cancel", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(cancelTerminal, 409, string(domain.CodeScheduleState),
		"cancel a succeeded action"); err != nil {
		return err
	}
	return nil
}

func createScheduledRun(api *client, formulaID, code string) (runView, error) {
	response, err := api.call("POST", "/v1/runs", createRunPayload(code, formulaID), nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(response, 201, "create scheduled run "+code); err != nil {
		return runView{}, err
	}
	var run runView
	if err := response.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func createSchedulerVessel(api *client, code string) (vesselView, error) {
	response, err := api.call("POST", "/v1/vessels", map[string]any{
		"code":            code,
		"capacity_liters": 500.0,
	}, nil)
	if err != nil {
		return vesselView{}, err
	}
	if err := requireStatus(response, 201, "create vessel "+code); err != nil {
		return vesselView{}, err
	}
	var vessel vesselView
	if err := response.decode(&vessel); err != nil {
		return vesselView{}, err
	}
	return vessel, nil
}

func listContainsAction(actions []scheduledActionView, id string) bool {
	for _, action := range actions {
		if action.ID == id {
			return true
		}
	}
	return false
}
