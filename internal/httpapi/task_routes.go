package httpapi

import (
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

type createTaskRequest struct {
	RunID string    `json:"run_id"`
	Kind  string    `json:"kind"`
	DueAt time.Time `json:"due_at"`
}

func registerTaskRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("POST /v1/tasks", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[createTaskRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		task, err := app.ScheduleRunAction(service.ScheduleActionInput{
			RunID: body.RunID,
			Kind:  domain.ActionKind(body.Kind),
			DueAt: body.DueAt,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, task)
	})

	mux.HandleFunc("GET /v1/tasks", func(response http.ResponseWriter, request *http.Request) {
		filter := service.TaskFilter{
			RunID: request.URL.Query().Get("run_id"),
		}
		if raw := request.URL.Query().Get("state"); raw != "" {
			state := domain.TaskState(raw)
			if !state.Valid() {
				writeDomainError(response, domain.FieldError(domain.CodeInvalidInput,
					"state", "must be a known task state"))
				return
			}
			filter.State = state
		}
		tasks, err := app.ListScheduledTasks(filter)
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{"tasks": tasks})
	})

	mux.HandleFunc("GET /v1/tasks/{id}", func(response http.ResponseWriter, request *http.Request) {
		task, err := app.GetScheduledTask(request.PathValue("id"))
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, task)
	})

	mux.HandleFunc("POST /v1/tasks/dispatch", func(response http.ResponseWriter, _ *http.Request) {
		summary := app.DispatchDueTasks(0)
		writeJSON(response, http.StatusOK, summary)
	})
}
