package workflowcheck

import (
	"fmt"
	"sync"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
)

// scheduledRunStart exercises the local scheduler through real HTTP
// requests: registration, the queryable queue, autonomous due-time
// execution, retained failure reasons, idempotent fulfillment, and
// exactly-once advancement under concurrent dispatches.
func scheduledRunStart(api *client) error {
	formula, err := ensureApprovedFormula(api, "Scheduled Formula")
	if err != nil {
		return err
	}

	invalidKind, err := api.call("POST", "/v1/tasks", map[string]any{
		"run_id": "run_missing", "kind": "bogus", "due_at": time.Now().UTC(),
	}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(invalidKind, 400, string(domain.CodeInvalidInput),
		"schedule unknown action kind"); err != nil {
		return err
	}
	missingRun, err := api.call("POST", "/v1/tasks", map[string]any{
		"run_id": "run_missing",
		"kind":   string(domain.ActionStartFermentation),
		"due_at": time.Now().UTC(),
	}, nil)
	if err != nil {
		return err
	}
	if err := requireCode(missingRun, 404, string(domain.CodeRunNotFound),
		"schedule for missing run"); err != nil {
		return err
	}
	missingTask, err := api.call("GET", "/v1/tasks/task_missing", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(missingTask, 404, string(domain.CodeTaskNotFound),
		"read missing task"); err != nil {
		return err
	}
	badFilter, err := api.call("GET", "/v1/tasks?state=bogus", nil, nil)
	if err != nil {
		return err
	}
	if err := requireCode(badFilter, 400, string(domain.CodeInvalidInput),
		"filter by unknown state"); err != nil {
		return err
	}

	vesselOne, err := createCheckedVessel(api, "VSL-201")
	if err != nil {
		return err
	}
	vesselTwo, err := createCheckedVessel(api, "VSL-202")
	if err != nil {
		return err
	}
	vesselThree, err := createCheckedVessel(api, "VSL-203")
	if err != nil {
		return err
	}

	// Run A is reserved and due shortly; the background ticker must execute
	// the task autonomously and advance the run exactly once.
	runA, err := createCheckedRun(api, "RUN-SCHED-A", formula.ID)
	if err != nil {
		return err
	}
	if err := reserveCheckedRun(api, runA.ID, vesselOne.ID); err != nil {
		return err
	}
	taskA, err := scheduleCheckedTask(api, runA.ID, time.Now().UTC().Add(200*time.Millisecond))
	if err != nil {
		return err
	}
	if taskA.State != domain.TaskPending || taskA.Version != 1 {
		return errInvalidWorkflow("scheduled task did not start as a waiting task")
	}
	early, err := getCheckedTask(api, taskA.ID)
	if err != nil {
		return err
	}
	if early.State != domain.TaskPending {
		return errInvalidWorkflow("task executed before its due time")
	}
	doneA, err := waitCheckedTask(api, taskA.ID, domain.TaskSucceeded)
	if err != nil {
		return err
	}
	if doneA.Reason != "" {
		return errInvalidWorkflow("succeeded task carries a failure reason")
	}
	runA, err = getCheckedRun(api, runA.ID)
	if err != nil {
		return err
	}
	if err := requireRunState(runA, domain.RunFermenting); err != nil {
		return err
	}
	if runA.Version != 3 {
		return errInvalidWorkflow("scheduled start did not advance the run exactly once")
	}

	// Run B cannot start (no vessel); run C can. One dispatch must record
	// the failure reason for B and still execute C.
	runB, err := createCheckedRun(api, "RUN-SCHED-B", formula.ID)
	if err != nil {
		return err
	}
	runC, err := createCheckedRun(api, "RUN-SCHED-C", formula.ID)
	if err != nil {
		return err
	}
	if err := reserveCheckedRun(api, runC.ID, vesselTwo.ID); err != nil {
		return err
	}
	taskB, err := scheduleCheckedTask(api, runB.ID, time.Now().UTC())
	if err != nil {
		return err
	}
	taskC, err := scheduleCheckedTask(api, runC.ID, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := dispatchChecked(api); err != nil {
		return err
	}
	doneB, err := waitCheckedTask(api, taskB.ID, domain.TaskNeedsAttention)
	if err != nil {
		return err
	}
	if doneB.Reason == "" {
		return errInvalidWorkflow("failed task did not retain its reason")
	}
	runB, err = getCheckedRun(api, runB.ID)
	if err != nil {
		return err
	}
	if err := requireRunState(runB, domain.RunPlanned); err != nil {
		return err
	}
	if runB.Version != 1 {
		return errInvalidWorkflow("failed task changed the run")
	}
	if _, err := waitCheckedTask(api, taskC.ID, domain.TaskSucceeded); err != nil {
		return err
	}
	runC, err = getCheckedRun(api, runC.ID)
	if err != nil {
		return err
	}
	if err := requireRunState(runC, domain.RunFermenting); err != nil {
		return err
	}
	if runC.Version != 3 {
		return errInvalidWorkflow("a blocked task delayed the execution behind it")
	}

	// A duplicate registration for the already-fermenting run A is
	// fulfilled idempotently and must not advance the run again.
	taskA2, err := scheduleCheckedTask(api, runA.ID, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := dispatchChecked(api); err != nil {
		return err
	}
	doneA2, err := waitCheckedTask(api, taskA2.ID, domain.TaskSucceeded)
	if err != nil {
		return err
	}
	if doneA2.ResultNote == "" {
		return errInvalidWorkflow("duplicate task was not fulfilled idempotently")
	}
	runA, err = getCheckedRun(api, runA.ID)
	if err != nil {
		return err
	}
	if runA.Version != 3 {
		return errInvalidWorkflow("duplicate task advanced the run a second time")
	}

	// Concurrent dispatches racing for the same due task advance the run
	// exactly once.
	runD, err := createCheckedRun(api, "RUN-SCHED-D", formula.ID)
	if err != nil {
		return err
	}
	if err := reserveCheckedRun(api, runD.ID, vesselThree.ID); err != nil {
		return err
	}
	taskD, err := scheduleCheckedTask(api, runD.ID, time.Now().UTC())
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	raceErrors := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := dispatchChecked(api); err != nil {
				raceErrors <- err
			}
		}()
	}
	wg.Wait()
	close(raceErrors)
	for raceErr := range raceErrors {
		if raceErr != nil {
			return raceErr
		}
	}
	if _, err := waitCheckedTask(api, taskD.ID, domain.TaskSucceeded); err != nil {
		return err
	}
	runD, err = getCheckedRun(api, runD.ID)
	if err != nil {
		return err
	}
	if runD.Version != 3 {
		return errInvalidWorkflow("concurrent dispatches advanced the run more than once")
	}

	// The queue stays queryable by run and by state.
	byRun, err := listCheckedTasks(api, "run_id="+runA.ID)
	if err != nil {
		return err
	}
	if len(byRun) != 2 {
		return errInvalidWorkflow("run task history was not queryable")
	}
	for _, task := range byRun {
		if task.RunID != runA.ID {
			return errInvalidWorkflow("run filter returned a task for another run")
		}
	}
	attention, err := listCheckedTasks(api, "state="+string(domain.TaskNeedsAttention))
	if err != nil {
		return err
	}
	if len(attention) != 1 || attention[0].ID != taskB.ID {
		return errInvalidWorkflow("attention queue did not contain exactly the failed task")
	}
	return nil
}

func createCheckedVessel(api *client, code string) (vesselView, error) {
	resp, err := api.call("POST", "/v1/vessels", map[string]any{
		"code":            code,
		"capacity_liters": 240.0,
	}, nil)
	if err != nil {
		return vesselView{}, err
	}
	if err := requireStatus(resp, 201, "create vessel "+code); err != nil {
		return vesselView{}, err
	}
	var vessel vesselView
	if err := resp.decode(&vessel); err != nil {
		return vesselView{}, err
	}
	return vessel, nil
}

func createCheckedRun(api *client, code, formulaID string) (runView, error) {
	resp, err := api.call("POST", "/v1/runs", createRunPayload(code, formulaID), nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(resp, 201, "create run "+code); err != nil {
		return runView{}, err
	}
	var run runView
	if err := resp.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func reserveCheckedRun(api *client, runID, vesselID string) error {
	resp, err := api.call("POST", "/v1/runs/"+runID+"/reserve",
		map[string]any{"vessel_id": vesselID}, ptrInt64(1))
	if err != nil {
		return err
	}
	return requireStatus(resp, 200, "reserve run")
}

func scheduleCheckedTask(api *client, runID string, dueAt time.Time) (taskView, error) {
	resp, err := api.call("POST", "/v1/tasks", map[string]any{
		"run_id": runID,
		"kind":   string(domain.ActionStartFermentation),
		"due_at": dueAt,
	}, nil)
	if err != nil {
		return taskView{}, err
	}
	if err := requireStatus(resp, 201, "schedule run action"); err != nil {
		return taskView{}, err
	}
	var task taskView
	if err := resp.decode(&task); err != nil {
		return taskView{}, err
	}
	return task, nil
}

func getCheckedTask(api *client, id string) (taskView, error) {
	resp, err := api.call("GET", "/v1/tasks/"+id, nil, nil)
	if err != nil {
		return taskView{}, err
	}
	if err := requireStatus(resp, 200, "read task"); err != nil {
		return taskView{}, err
	}
	var task taskView
	if err := resp.decode(&task); err != nil {
		return taskView{}, err
	}
	return task, nil
}

func getCheckedRun(api *client, id string) (runView, error) {
	resp, err := api.call("GET", "/v1/runs/"+id, nil, nil)
	if err != nil {
		return runView{}, err
	}
	if err := requireStatus(resp, 200, "read run"); err != nil {
		return runView{}, err
	}
	var run runView
	if err := resp.decode(&run); err != nil {
		return runView{}, err
	}
	return run, nil
}

func dispatchChecked(api *client) error {
	resp, err := api.call("POST", "/v1/tasks/dispatch", nil, nil)
	if err != nil {
		return err
	}
	return requireStatus(resp, 200, "dispatch due tasks")
}

func waitCheckedTask(api *client, id string, want domain.TaskState) (taskView, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		task, err := getCheckedTask(api, id)
		if err != nil {
			return taskView{}, err
		}
		if task.State == want {
			return task, nil
		}
		if time.Now().After(deadline) {
			return taskView{}, fmt.Errorf("task %s did not reach %q; last state %q",
				id, want, task.State)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func listCheckedTasks(api *client, query string) ([]taskView, error) {
	resp, err := api.call("GET", "/v1/tasks?"+query, nil, nil)
	if err != nil {
		return nil, err
	}
	if err := requireStatus(resp, 200, "list tasks"); err != nil {
		return nil, err
	}
	var listing struct {
		Tasks []taskView `json:"tasks"`
	}
	if err := resp.decode(&listing); err != nil {
		return nil, err
	}
	return listing.Tasks, nil
}
