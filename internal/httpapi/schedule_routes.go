package httpapi

import (
	"net/http"
	"time"

	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/domain"
	"github.com/1260124186-cc/solo-0016-cellar-run-control/internal/service"
)

type scheduleRunRequest struct {
	Action      string    `json:"action"`
	ScheduledAt time.Time `json:"scheduled_at"`
	Note        string    `json:"note"`
}

func registerScheduleRoutes(mux *http.ServeMux, app *service.Service) {
	mux.HandleFunc("POST /v1/runs/{id}/schedule", func(response http.ResponseWriter, request *http.Request) {
		body, err := decodeJSON[scheduleRunRequest](response, request)
		if !handleDecode(response, err) {
			return
		}
		action, err := app.ScheduleRunAction(request.PathValue("id"), service.ScheduleRunInput{
			Action:      domain.ScheduleAction(body.Action),
			ScheduledAt: body.ScheduledAt,
			Note:        body.Note,
		})
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusCreated, action)
	})

	mux.HandleFunc("GET /v1/scheduled-actions", func(response http.ResponseWriter, request *http.Request) {
		filter := domain.ScheduleFilter{
			RunID: request.URL.Query().Get("run_id"),
			State: domain.ScheduleState(request.URL.Query().Get("state")),
		}
		if filter.State != "" && !domain.IsKnownScheduleState(filter.State) {
			writeDomainError(response, domain.FieldError(domain.CodeInvalidInput,
				"state", "is not a known scheduled action state"))
			return
		}
		actions, err := app.ListScheduledActions(filter)
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, map[string]any{"scheduled_actions": actions})
	})

	mux.HandleFunc("GET /v1/scheduled-actions/{id}", func(response http.ResponseWriter, request *http.Request) {
		action, err := app.GetScheduledAction(request.PathValue("id"))
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, action)
	})

	mux.HandleFunc("POST /v1/scheduled-actions/{id}/cancel", func(response http.ResponseWriter, request *http.Request) {
		if err := app.CancelScheduledAction(request.PathValue("id")); err != nil {
			writeDomainError(response, err)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})

	// Trigger runs one pass immediately. It is intended for the local
	// adapter and operator tooling; a locked controller reports skipped.
	mux.HandleFunc("POST /v1/scheduler/tick", func(response http.ResponseWriter, _ *http.Request) {
		report, err := app.TriggerScheduledActions()
		if err != nil {
			writeDomainError(response, err)
			return
		}
		writeJSON(response, http.StatusOK, report)
	})
}
