package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/dto/request"
	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

func (h *Handlers) StartInstance(c *gin.Context) {
	var req request.StartInstance
	if !validators.BindJSON(c, &req, false) {
		return
	}
	cmd, err := req.ToCommand()
	if err != nil {
		respond.Fail(c, err)
		return
	}
	inst, err := h.Processes.Start(c.Request.Context(), cmd)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.Header("Location", "/api/v1/process-instances/"+inst.ProcessInstanceKey)
	c.JSON(http.StatusCreated, mappers.ToStartedInstance(inst))
}

func (h *Handlers) SearchInstances(c *gin.Context) {
	if bk := c.Query("businessKey"); bk != "" {
		recs, err := h.Processes.FindByBusinessKey(c.Request.Context(), bk)
		if err != nil {
			respond.Fail(c, err)
			return
		}
		c.JSON(http.StatusOK, response.Page[response.InstanceRecord]{
			Items: mappers.MapSlice(recs, mappers.ToInstanceRecord), Total: len(recs),
		})
		return
	}
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	hasInc, ok := validators.Bool(c, "hasIncident")
	if !ok {
		return
	}
	sort, ok := validators.Sort(c, validators.InstanceSortFields)
	if !ok {
		return
	}
	f := domain.ProcessInstanceFilter{
		ProcessDefinitionID:  c.Query("processDefinitionId"),
		ProcessDefinitionKey: c.Query("processDefinitionKey"),
		State:                domain.InstanceState(strings.ToUpper(c.Query("state"))),
		HasIncident:          hasInc,
		ParentInstanceKey:    c.Query("parentProcessInstanceKey"),
		TenantID:             c.Query("tenantId"),
	}
	res, err := h.Processes.Search(c.Request.Context(), f, p, sort)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToPage(res, mappers.ToProcessInstance))
}

func (h *Handlers) GetInstance(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	d, err := h.Processes.Get(c.Request.Context(), key)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToInstanceDetail(d))
}

func (h *Handlers) InstanceVariables(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	res, err := h.Processes.Variables(c.Request.Context(), key, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToPage(res, mappers.ToVariable))
}

func (h *Handlers) InstanceEvents(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	evs, err := h.Monitoring.Events(c.Request.Context(), ports.EventFilter{ProcessInstanceKey: key}, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.List[response.Event]{Items: mappers.MapSlice(evs, mappers.ToEvent)})
}

func (h *Handlers) CancelInstance(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	var req request.CancelInstance
	if !validators.BindJSON(c, &req, true) {
		return
	}
	if err := h.Processes.Cancel(c.Request.Context(), key, req.Reason); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// ---------- mensagens / sinais ----------

func (h *Handlers) PublishMessage(c *gin.Context) {
	var req request.PublishMessage
	if !validators.BindJSON(c, &req, false) {
		return
	}
	cmd, err := req.ToCommand()
	if err != nil {
		respond.Fail(c, err)
		return
	}
	res, err := h.Processes.PublishMessage(c.Request.Context(), cmd)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToMessage(res))
}

func (h *Handlers) BroadcastSignal(c *gin.Context) {
	var req request.BroadcastSignal
	if !validators.BindJSON(c, &req, false) {
		return
	}
	res, err := h.Processes.BroadcastSignal(c.Request.Context(), req.ToCommand())
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToSignal(res))
}
