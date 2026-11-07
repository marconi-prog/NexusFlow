// Package routes monta o roteador HTTP: middlewares, grupos por papel e rotas.
package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/marconi-prog/nexusflow/internal/auth"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/handlers"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/middleware"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
)

func New(d handlers.Deps) *gin.Engine {
	if d.Config.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.ContextWithFallback = true
	_ = r.SetTrustedProxies(nil)
	r.MaxMultipartMemory = 32 << 20

	r.Use(middleware.RequestContext(d.Logger), middleware.Recovery(), middleware.CORS(d.Config.HTTP.CORSOrigins))
	r.NoRoute(func(c *gin.Context) { respond.Problem(c, http.StatusNotFound, "rota não encontrada") })
	r.NoMethod(func(c *gin.Context) { respond.Problem(c, http.StatusMethodNotAllowed, "método não permitido") })

	h := handlers.New(d)

	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "UP", "service": "nexusflow-api", "version": d.Version})
	})
	r.GET("/health/ready", h.Ready)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := r.Group("/api/v1", middleware.Authenticate(d.Config.Auth.APIKeys))
	viewer := v1.Group("", middleware.Require(auth.RoleViewer))
	operator := v1.Group("", middleware.Require(auth.RoleOperator))
	admin := v1.Group("", middleware.Require(auth.RoleAdmin))
	idem := middleware.Idempotent(d.Store)

	viewer.GET("/me", h.Me)

	// definições
	admin.POST("/deployments", h.Deploy)
	viewer.GET("/process-definitions", h.SearchDefinitions)
	viewer.GET("/process-definitions/:key", h.GetDefinition)
	viewer.GET("/process-definitions/:key/xml", h.GetDefinitionXML)

	// instâncias
	operator.POST("/process-instances", idem, h.StartInstance)
	viewer.GET("/process-instances", h.SearchInstances)
	viewer.GET("/process-instances/:key", h.GetInstance)
	viewer.GET("/process-instances/:key/variables", h.InstanceVariables)
	viewer.GET("/process-instances/:key/events", h.InstanceEvents)
	operator.POST("/process-instances/:key/cancellation", h.CancelInstance)

	operator.PUT("/process-instances/:key/variables", h.SetVariables)

	// saga de pedido (processes/bpmn/pedido-saga.bpmn)
	operator.POST("/orders", idem, h.PlaceOrder)
	viewer.GET("/orders", h.ListOrders)
	viewer.GET("/orders/:id", h.GetOrder)
	operator.POST("/orders/:id/delivery-confirmation", h.ConfirmDelivery)
	viewer.GET("/inventory", h.ListInventory)
	admin.PUT("/inventory/:sku", h.SetStock)

	// mensagens e sinais
	operator.POST("/messages", idem, h.PublishMessage)
	operator.POST("/signals", idem, h.BroadcastSignal)

	// incidentes
	viewer.GET("/incidents", h.SearchIncidents)
	viewer.GET("/incidents/:key", h.GetIncident)
	operator.POST("/incidents/:key/resolution", h.ResolveIncident)

	// tarefas humanas
	viewer.GET("/user-tasks", h.SearchUserTasks)
	viewer.GET("/user-tasks/:key", h.GetUserTask)
	operator.POST("/user-tasks/:key/assignment", h.AssignUserTask)
	operator.DELETE("/user-tasks/:key/assignee", h.UnassignUserTask)
	operator.POST("/user-tasks/:key/completion", h.CompleteUserTask)

	// monitoramento
	viewer.GET("/monitoring/overview", h.Overview)
	viewer.GET("/monitoring/topology", h.Topology)
	viewer.GET("/monitoring/jobs", h.JobStats)
	viewer.GET("/events", h.ListEvents)
	viewer.GET("/events/stream", h.StreamEvents)

	return r
}
