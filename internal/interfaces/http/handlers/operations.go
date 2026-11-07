package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/dto/request"
	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

// ---------- incidentes ----------

func (h *Handlers) SearchIncidents(c *gin.Context) {
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	f := domain.IncidentFilter{
		ProcessInstanceKey:  c.Query("processInstanceKey"),
		ProcessDefinitionID: c.Query("processDefinitionId"),
		State:               strings.ToUpper(c.DefaultQuery("state", "ACTIVE")),
		ErrorType:           c.Query("errorType"),
		TenantID:            c.Query("tenantId"),
	}
	if f.State == "ALL" {
		f.State = ""
	}
	res, err := h.Incidents.Search(c.Request.Context(), f, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToPage(res, mappers.ToIncident))
}

func (h *Handlers) GetIncident(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	inc, err := h.Incidents.Get(c.Request.Context(), key)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToIncident(*inc))
}

func (h *Handlers) ResolveIncident(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	var req request.ResolveIncident
	if !validators.BindJSON(c, &req, true) {
		return
	}
	if err := req.Validate(); err != nil {
		respond.Fail(c, err)
		return
	}
	inc, err := h.Incidents.Resolve(c.Request.Context(), key, req.Retries)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.ResolveIncident{Resolved: true, Incident: mappers.ToIncident(*inc)})
}

// ---------- tarefas humanas ----------

func (h *Handlers) SearchUserTasks(c *gin.Context) {
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	f := domain.UserTaskFilter{
		State:              strings.ToUpper(c.DefaultQuery("state", "CREATED")),
		Assignee:           c.Query("assignee"),
		CandidateGroup:     c.Query("candidateGroup"),
		CandidateUser:      c.Query("candidateUser"),
		ProcessInstanceKey: c.Query("processInstanceKey"),
		ElementID:          c.Query("elementId"),
		TenantID:           c.Query("tenantId"),
	}
	if f.State == "ALL" {
		f.State = ""
	}
	res, err := h.Tasks.Search(c.Request.Context(), f, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToPage(res, mappers.ToUserTask))
}

func (h *Handlers) GetUserTask(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	t, err := h.Tasks.Get(c.Request.Context(), key)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToUserTask(*t))
}

func (h *Handlers) AssignUserTask(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	var req request.AssignUserTask
	if !validators.BindJSON(c, &req, false) {
		return
	}
	if err := h.Tasks.Assign(c.Request.Context(), key, req.Assignee, req.AllowOverride); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handlers) UnassignUserTask(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	if err := h.Tasks.Unassign(c.Request.Context(), key); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *Handlers) CompleteUserTask(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	var req request.CompleteUserTask
	if !validators.BindJSON(c, &req, true) {
		return
	}
	if err := h.Tasks.Complete(c.Request.Context(), key, req.Variables, req.Action); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
