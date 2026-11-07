package payment

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// Cartões de teste do gateway simulado (pelos 4 últimos dígitos):
//
//	0002 => recusado pelo emissor (erro de negócio => compensação)
//	0003 => gateway instável: falha nas 2 primeiras tentativas (exercita retries)
//	0004 => gateway fora do ar: sempre falha (esgota retries => incidente)
//	outros => aprovado, se o valor não passar do limite
const (
	CardDeclined = "0002"
	CardUnstable = "0003"
	CardOffline  = "0004"
	// LimitCents é o limite por cobrança; acima dele a cobrança é recusada.
	LimitCents = 5_000_00
)

// SimulatedGateway substitui um provedor real para estudo. Troque por um adapter real
// implementando Gateway.
type SimulatedGateway struct {
	mu       sync.Mutex
	attempts map[string]int
}

func NewSimulatedGateway() *SimulatedGateway {
	return &SimulatedGateway{attempts: map[string]int{}}
}

var ErrGatewayUnavailable = errors.New("gateway de pagamento indisponível")

func (g *SimulatedGateway) Charge(_ context.Context, req ChargeRequest) (string, error) {
	switch req.CardLast4 {
	case CardDeclined:
		return "", &DeclinedError{Reason: "cartão recusado pelo emissor"}
	case CardOffline:
		return "", ErrGatewayUnavailable
	case CardUnstable:
		g.mu.Lock()
		g.attempts[req.OrderID]++
		n := g.attempts[req.OrderID]
		g.mu.Unlock()
		if n <= 2 {
			return "", fmt.Errorf("%w (tentativa %d)", ErrGatewayUnavailable, n)
		}
	}
	if req.AmountCents > LimitCents {
		return "", &DeclinedError{Reason: "valor acima do limite do cartão"}
	}
	return "TX-" + uuid.NewString()[:8], nil
}

func (g *SimulatedGateway) Refund(context.Context, string) error { return nil }
