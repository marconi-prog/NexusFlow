// Package auth define o principal autenticado e os papéis de acesso.
package auth

import "context"

type Role string

const (
	RoleViewer   Role = "viewer"   // leitura e monitoramento
	RoleOperator Role = "operator" // + iniciar/cancelar instâncias, mensagens, incidentes, tarefas
	RoleAdmin    Role = "admin"    // + deploy de definições
)

var rank = map[Role]int{RoleViewer: 1, RoleOperator: 2, RoleAdmin: 3}

// Allows indica se o papel atende ao mínimo exigido.
func (r Role) Allows(min Role) bool { return rank[r] >= rank[min] }

type Principal struct {
	ID   string
	Role Role
}

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// ActorID devolve o identificador do principal para auditoria ("anonymous" se ausente).
func ActorID(ctx context.Context) string {
	if p, ok := PrincipalFrom(ctx); ok {
		return p.ID
	}
	return "anonymous"
}
