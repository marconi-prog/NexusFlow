// Package validators lê e valida parâmetros de path, query e corpo das requisições.
package validators

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/ports"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
)

const maxJSONBody = 4 << 20

// BindJSON decodifica o corpo rejeitando campos desconhecidos. Com optional=true, corpo vazio é aceito.
func BindJSON(c *gin.Context, dst any, optional bool) bool {
	dec := json.NewDecoder(io.LimitReader(c.Request.Body, maxJSONBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if optional && errors.Is(err, io.EOF) {
			return true
		}
		respond.BadRequest(c, "JSON inválido: "+err.Error())
		return false
	}
	return true
}

// Key valida chaves do motor (inteiros positivos serializados como string).
func Key(c *gin.Context) (string, bool) {
	k := c.Param("key")
	if k == "" || len(k) > 20 || strings.TrimLeft(k, "0123456789") != "" {
		respond.BadRequest(c, "chave inválida: deve ser numérica")
		return "", false
	}
	return k, true
}

func Page(c *gin.Context) (domain.Page, bool) {
	var p domain.Page
	var err error
	if v := c.Query("from"); v != "" {
		if p.From, err = strconv.Atoi(v); err != nil || p.From < 0 {
			respond.BadRequest(c, "from inválido")
			return p, false
		}
	}
	if v := c.Query("limit"); v != "" {
		if p.Limit, err = strconv.Atoi(v); err != nil || p.Limit < 1 || p.Limit > 500 {
			respond.BadRequest(c, "limit deve estar entre 1 e 500")
			return p, false
		}
	}
	return p.Normalize(), true
}

func Bool(c *gin.Context, name string) (*bool, bool) {
	v := c.Query(name)
	if v == "" {
		return nil, true
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		respond.BadRequest(c, name+" deve ser true ou false")
		return nil, false
	}
	return &b, true
}

func Duration(c *gin.Context, name string, def, max time.Duration) (time.Duration, bool) {
	v := c.Query(name)
	if v == "" {
		return def, true
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 || d > max {
		respond.BadRequest(c, name+" inválido (ex.: 1h, 24h; máximo "+max.String()+")")
		return 0, false
	}
	return d, true
}

func Time(c *gin.Context, name string) (*time.Time, bool) {
	v := c.Query(name)
	if v == "" {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		respond.BadRequest(c, name+" deve estar em RFC 3339")
		return nil, false
	}
	return &t, true
}

var InstanceSortFields = map[string]bool{
	"startDate": true, "endDate": true, "processDefinitionId": true, "processInstanceKey": true, "state": true,
}

// Sort aceita "campo:asc,campo2:desc".
func Sort(c *gin.Context, allowed map[string]bool) ([]domain.Sort, bool) {
	raw := c.Query("sort")
	if raw == "" {
		return nil, true
	}
	var out []domain.Sort
	for _, part := range strings.Split(raw, ",") {
		field, order, _ := strings.Cut(strings.TrimSpace(part), ":")
		if !allowed[field] {
			respond.BadRequest(c, "campo de ordenação inválido: "+field)
			return nil, false
		}
		order = strings.ToUpper(order)
		if order == "" {
			order = "ASC"
		}
		if order != "ASC" && order != "DESC" {
			respond.BadRequest(c, "ordem inválida: "+order)
			return nil, false
		}
		out = append(out, domain.Sort{Field: field, Order: order})
	}
	return out, true
}

// EventFilter lê os filtros comuns de /events e /events/stream.
func EventFilter(c *gin.Context) (ports.EventFilter, bool) {
	f := ports.EventFilter{
		ProcessInstanceKey:  c.Query("processInstanceKey"),
		ProcessDefinitionID: c.Query("processDefinitionId"),
		JobType:             c.Query("jobType"),
	}
	if t := c.Query("type"); t != "" {
		for _, s := range strings.Split(t, ",") {
			if s = strings.TrimSpace(s); s != "" {
				f.Types = append(f.Types, s)
			}
		}
	}
	var ok bool
	if f.Since, ok = Time(c, "since"); !ok {
		return f, false
	}
	if f.Until, ok = Time(c, "until"); !ok {
		return f, false
	}
	return f, true
}
