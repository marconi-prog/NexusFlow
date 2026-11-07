package handlers

import (
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/marconi-prog/nexusflow/internal/application/mappers"
	"github.com/marconi-prog/nexusflow/internal/application/services"
	"github.com/marconi-prog/nexusflow/internal/domain"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/respond"
	"github.com/marconi-prog/nexusflow/internal/interfaces/http/validators"
)

const maxDeployFiles = 50

func (h *Handlers) Deploy(c *gin.Context) {
	form, err := c.MultipartForm()
	if err != nil {
		respond.BadRequest(c, "envie multipart/form-data com o campo 'resources'")
		return
	}
	files := form.File["resources"]
	if len(files) == 0 {
		respond.BadRequest(c, "campo 'resources' vazio")
		return
	}
	if len(files) > maxDeployFiles {
		respond.BadRequest(c, "máximo de 50 arquivos por deployment")
		return
	}
	resources := make([]domain.Resource, 0, len(files))
	for _, fh := range files {
		if fh.Size > services.MaxResourceSize {
			respond.BadRequest(c, "arquivo excede 10 MiB: "+fh.Filename)
			return
		}
		f, err := fh.Open()
		if err != nil {
			respond.BadRequest(c, "falha ao ler "+fh.Filename)
			return
		}
		data, err := io.ReadAll(io.LimitReader(f, services.MaxResourceSize+1))
		f.Close()
		if err != nil {
			respond.BadRequest(c, "falha ao ler "+fh.Filename)
			return
		}
		resources = append(resources, domain.Resource{Name: fh.Filename, Content: data})
	}
	d, err := h.Processes.Deploy(c.Request.Context(), c.PostForm("tenantId"), resources)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, mappers.ToDeployment(d))
}

func (h *Handlers) SearchDefinitions(c *gin.Context) {
	p, ok := validators.Page(c)
	if !ok {
		return
	}
	latest, ok := validators.Bool(c, "latest")
	if !ok {
		return
	}
	f := domain.ProcessDefinitionFilter{ID: c.Query("processDefinitionId"), Name: c.Query("name"), TenantID: c.Query("tenantId")}
	if latest != nil {
		f.Latest = *latest
	}
	if v := c.Query("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			respond.BadRequest(c, "version inválida")
			return
		}
		f.Version = n
	}
	res, err := h.Processes.SearchDefinitions(c.Request.Context(), f, p)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToPage(res, mappers.ToProcessDefinition))
}

func (h *Handlers) GetDefinition(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	d, err := h.Processes.GetDefinition(c.Request.Context(), key)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.JSON(http.StatusOK, mappers.ToProcessDefinition(*d))
}

func (h *Handlers) GetDefinitionXML(c *gin.Context) {
	key, ok := validators.Key(c)
	if !ok {
		return
	}
	x, err := h.Processes.GetDefinitionXML(c.Request.Context(), key)
	if err != nil {
		respond.Fail(c, err)
		return
	}
	c.Data(http.StatusOK, "application/xml; charset=utf-8", []byte(x))
}
