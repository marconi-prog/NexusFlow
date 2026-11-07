// Package migrations embute os scripts SQL do PostgreSQL no binário.
// Adicione novos arquivos seguindo a numeração (0002_descricao.sql, ...): eles são
// aplicados em ordem na subida da aplicação, uma única vez cada.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
