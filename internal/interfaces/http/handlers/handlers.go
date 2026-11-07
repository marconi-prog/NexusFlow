// Package handlers contém os handlers HTTP da API, um arquivo por recurso.
package handlers

import (
	"log/slog"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/config"
	"github.com/marconi-prog/nexusflow/internal/infrastructure/integrations/hotglue"
)

// Deps são as dependências injetadas nos handlers.
type Deps struct {
	Config     *config.Config
	Logger     *slog.Logger
	Store      ports.Store
	Processes  *services.Processes
	Orders     *services.Orders
	Incidents  *services.Incidents
	Tasks      *services.Tasks
	Monitoring *services.Monitoring
	Hub        *Hub
	// HotglueWebhook recebe os webhooks do Hotglue (autenticados por HMAC, não por API key).
	HotglueWebhook *hotglue.WebhookHandler
	Version        string
}

type Handlers struct{ Deps }

func New(d Deps) *Handlers { return &Handlers{d} }
