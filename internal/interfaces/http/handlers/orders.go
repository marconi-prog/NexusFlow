package handlers

import (
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/dto/request"
	"github.com/marconi-prog/nexusflow/internal/application/dto/response"
	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func idParam(c *gin.Context, name string) (string, bool) {
	v := c.Param(name)
	if !idPattern.MatchString(v) {
		respond.BadRequest(c, name+" inválido")
		return "", false
	}
	return v, true
}

// PlaceOrder inicia a saga de pedido. Responde 202: o pedido é processado de forma
// assíncrona pelos workers; acompanhe por GET /orders/:id ou /events/stream.
func (h *Handlers) PlaceOrder(c *gin.Context) {
	var req request.PlaceOrder
	if !validators.BindJSON(c, &req, false) {
		return
	}
	placed, err := h.Orders.Place(c.Request.Context(), req.ToCommand())
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.Header("Location", "/api/v1/orders/"+placed.OrderID)
	c.JSON(http.StatusAccepted, mappers.ToPlacedOrder(placed))
}

func (h *Handlers) ListOrders(c *gin.Context) {
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	orders, err := h.Orders.List(c.Request.Context(), c.Query("status"), p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.List[response.Order]{Items: mappers.MapSlice(orders, mappers.ToOrder)})
}

func (h *Handlers) GetOrder(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	d, err := h.Orders.Get(c.Request.Context(), id)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToOrderDetail(d))
}

// ConfirmDelivery publica a mensagem "entrega-confirmada" para o pedido.
func (h *Handlers) ConfirmDelivery(c *gin.Context) {
	id, ok := idParam(c, "id")
	if !ok {
		return
	}
	res, err := h.Orders.ConfirmDelivery(c.Request.Context(), id)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToMessage(res))
}

func (h *Handlers) ListInventory(c *gin.Context) {
	items, err := h.Orders.Inventory(c.Request.Context())
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, response.List[response.InventoryItem]{Items: mappers.MapSlice(items, mappers.ToInventoryItem)})
}

func (h *Handlers) SetStock(c *gin.Context) {
	sku, ok := idParam(c, "sku")
	if !ok {
		return
	}
	var req request.SetStock
	if !validators.BindJSON(c, &req, false) {
		return
	}
	if err := req.Validate(); err != nil {
		respond.Fail(c, err)
		return
	}
	if err := h.Orders.SetStock(c.Request.Context(), sku, *req.Available); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// SetVariables altera variáveis globais de uma instância (ex.: corrigir dado antes de resolver incidente).
func (h *Handlers) SetVariables(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	var req request.SetVariables
	if !validators.BindJSON(c, &req, false) {
		return
	}
	if err := h.Processes.SetVariables(c.Request.Context(), key, req.Variables); err != nil {
		respond.Fail(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
