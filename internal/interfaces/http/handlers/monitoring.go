package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

const maxWindow = 31 * 24 * time.Hour

func (h *Handlers) Ready(c *gin.Context) {
	health := h.Monitoring.Readiness(c.Request.Context())
	status := http.StatusOK
	if health.Status != "UP" {
		status = http.StatusServiceUnavailable
	}
	c.JSON(status, mappers.ToHealth(health))
}

func (h *Handlers) Me(c *gin.Context) {
	p, _ := auth.PrincipalFrom(c.Request.Context())
	c.JSON(http.StatusOK, mappers.ToPrincipal(p))
}

func (h *Handlers) Overview(c *gin.Context) {
	w, ok := validators.Duration(c, "window", 24*time.Hour, maxWindow)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, mappers.ToOverview(h.Monitoring.Overview(c.Request.Context(), w)))
}

func (h *Handlers) Topology(c *gin.Context) {
	t, err := h.Monitoring.Topology(c.Request.Context())
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToTopology(t))
}

func (h *Handlers) JobStats(c *gin.Context) {
	w, ok := validators.Duration(c, "window", time.Hour, maxWindow)
	if !ok {
		return
	}
	stats, err := h.Monitoring.JobStats(c.Request.Context(), w)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.JobStats{Window: w.String(), Items: mappers.MapSlice(stats, mappers.ToJobStat)})
}

func (h *Handlers) ListEvents(c *gin.Context) {
	f, ok := validators.EventFilter(c)
	if !ok {
		return
	}
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	evs, err := h.Monitoring.Events(c.Request.Context(), f, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.List[response.Event]{Items: mappers.MapSlice(evs, mappers.ToEvent)})
}
