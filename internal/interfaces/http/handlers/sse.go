package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

// Hub recebe os eventos do store (de todas as réplicas, via LISTEN/NOTIFY no Postgres) e
// distribui para as conexões SSE locais.
type Hub struct {
	mu     sync.RWMutex
	subs   map[chan ports.Event]struct{}
	logger *slog.Logger
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{subs: map[chan ports.Event]struct{}{}, logger: logger}
}

// Run conecta o hub ao store até ctx ser cancelado.
func (h *Hub) Run(ctx context.Context, s ports.Store) {
	if err := s.Listen(ctx, h.broadcast); err != nil {
		h.logger.Error("hub de eventos encerrado", "error", err)
	}
}

func (h *Hub) broadcast(e ports.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default: // cliente lento: descarta (ele pode reconectar com Last-Event-ID)
		}
	}
}

func (h *Hub) subscribe() chan ports.Event {
	ch := make(chan ports.Event, 128)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan ports.Event) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

func matchesFilter(e ports.Event, f ports.EventFilter) bool {
	if len(f.Types) > 0 && !slices.Contains(f.Types, e.Type) {
		return false
	}
	if f.ProcessInstanceKey != "" && e.ProcessInstanceKey != f.ProcessInstanceKey {
		return false
	}
	if f.ProcessDefinitionID != "" && e.ProcessDefinitionID != f.ProcessDefinitionID {
		return false
	}
	if f.JobType != "" && e.JobType != f.JobType {
		return false
	}
	return true
}

// streamEvents entrega eventos em tempo real via Server-Sent Events. Suporta os mesmos
// filtros de /events e retomada via header Last-Event-ID (ou query lastEventId).
func (h *Handlers) StreamEvents(c *gin.Context) {
	f, ok := validators.EventFilter(c)
	if !ok {
		return
	}
	f.Since, f.Until = nil, nil

	var lastID int64
	if v := c.GetHeader("Last-Event-ID"); v != "" {
		lastID, _ = strconv.ParseInt(v, 10, 64)
	} else if v := c.Query("lastEventId"); v != "" {
		lastID, _ = strconv.ParseInt(v, 10, 64)
	}

	// conexões SSE são longas: remove o write timeout do servidor para esta resposta
	rc := http.NewResponseController(c.Writer)
	_ = rc.SetWriteDeadline(time.Time{})

	ch := h.Hub.subscribe()
	defer h.Hub.unsubscribe(ch)

	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	write := func(e ports.Event) error {
		b, err := json.Marshal(mappers.ToEvent(e))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.ID, e.Type, b); err != nil {
			return err
		}
		lastID = e.ID
		return nil
	}

	fmt.Fprint(w, "retry: 3000\n\n")
	// replay do que foi perdido desde o último id visto
	if lastID > 0 {
		replay := f
		replay.AfterID = lastID
		missed, err := h.Monitoring.Events(c.Request.Context(), replay, domain.Page{Limit: 500})
		if err == nil {
			for _, e := range missed {
				if write(e) != nil {
					return
				}
			}
		}
	}
	w.Flush()

	ctx := c.Request.Context()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			w.Flush()
		case e := <-ch:
			if e.ID <= lastID || !matchesFilter(e, f) {
				continue
			}
			if write(e) != nil {
				return
			}
			w.Flush()
		}
	}
}
