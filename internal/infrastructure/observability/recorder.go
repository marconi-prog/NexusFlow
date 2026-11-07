// Package audit grava eventos na trilha de auditoria sem bloquear o fluxo principal em caso de falha.
package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
)

type Recorder struct {
	store  ports.Store
	source string
	logger *slog.Logger
}

func NewRecorder(s ports.Store, source string, logger *slog.Logger) *Recorder {
	return &Recorder{store: s, source: source, logger: logger}
}

// Record grava o evento. Falhas são logadas e contabilizadas, mas não propagadas: o comando
// já foi aceito pelo motor e não deve ser reportado como falho por causa da auditoria.
func (r *Recorder) Record(ctx context.Context, e ports.Event) {
	if e.Source == "" {
		e.Source = r.source
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	// desacopla do cancelamento da requisição para não perder o evento
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.store.AppendEvent(ctx, &e); err != nil {
		EventRecordErrors.Inc()
		r.logger.Error("falha ao gravar evento", "type", e.Type, "error", err)
		return
	}
	EventsRecorded.WithLabelValues(e.Type).Inc()
}
