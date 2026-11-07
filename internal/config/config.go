// Package config carrega a configuração da plataforma a partir de variáveis de ambiente.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config agrega toda a configuração dos binários (api e worker).
type Config struct {
	Env      string
	LogLevel string

	HTTP     HTTPConfig
	Camunda  CamundaConfig
	Database DatabaseConfig
	Auth     AuthConfig
	Worker   WorkerConfig
	Monitor  MonitorConfig
}

type HTTPConfig struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	CORSOrigins     []string
}

// CamundaAuthMode define como o cliente se autentica na Orchestration Cluster REST API.
type CamundaAuthMode string

const (
	CamundaAuthNone  CamundaAuthMode = "none"
	CamundaAuthBasic CamundaAuthMode = "basic"
	CamundaAuthOAuth CamundaAuthMode = "oauth"
)

type CamundaConfig struct {
	// BaseURL da Orchestration Cluster REST API (ex.: http://localhost:8080 ou
	// https://<region>.zeebe.camunda.io/<clusterId>). O sufixo /v2 é adicionado pelo cliente.
	BaseURL        string
	AuthMode       CamundaAuthMode
	Username       string
	Password       string
	OAuthURL       string
	ClientID       string
	ClientSecret   string
	Audience       string
	Scope          string
	TenantID       string
	RequestTimeout time.Duration
}

type DatabaseConfig struct {
	// URL do PostgreSQL. Vazio => armazenamento em memória (apenas desenvolvimento/testes).
	URL      string
	MaxConns int32
}

type AuthConfig struct {
	// APIKeys no formato "chave:papel[:nome]" separados por vírgula. Papéis: admin, operator,
	// viewer. O nome identifica o cliente na auditoria. Vazio => autenticação desabilitada
	// (permitido apenas fora de produção).
	APIKeys map[string]APIKey
}

type APIKey struct {
	Role string
	Name string
}

type WorkerConfig struct {
	Name           string
	MaxJobs        int
	Concurrency    int
	JobTimeout     time.Duration
	PollInterval   time.Duration
	RequestTimeout time.Duration
	MetricsPort    string
	// JobTypes habilita apenas os handlers listados (vazio => todos os registrados).
	JobTypes []string
	// HTTPAllowedHosts é a allowlist de destinos do handler nexusflow.http ("*" libera tudo).
	HTTPAllowedHosts []string
	ShutdownGrace    time.Duration
}

type MonitorConfig struct {
	// IncidentPollInterval controla a varredura de incidentes no Camunda (0 desabilita).
	IncidentPollInterval time.Duration
	EventRetention       time.Duration
	IdempotencyRetention time.Duration
}

// Load lê as variáveis de ambiente e valida a configuração.
func Load() (*Config, error) {
	cfg := &Config{
		Env:      getenv("APP_ENV", "development"),
		LogLevel: getenv("LOG_LEVEL", "info"),
		HTTP: HTTPConfig{
			Port:            getenv("SERVER_PORT", "8080"),
			ReadTimeout:     getDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:    getDuration("HTTP_WRITE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: getDuration("HTTP_SHUTDOWN_TIMEOUT", 20*time.Second),
			CORSOrigins:     getList("CORS_ALLOWED_ORIGINS"),
		},
		Camunda: CamundaConfig{
			BaseURL:        strings.TrimRight(getenv("CAMUNDA_REST_ADDRESS", "http://localhost:8088"), "/"),
			AuthMode:       CamundaAuthMode(strings.ToLower(getenv("CAMUNDA_AUTH_MODE", "none"))),
			Username:       os.Getenv("CAMUNDA_USERNAME"),
			Password:       os.Getenv("CAMUNDA_PASSWORD"),
			OAuthURL:       os.Getenv("CAMUNDA_OAUTH_URL"),
			ClientID:       os.Getenv("CAMUNDA_CLIENT_ID"),
			ClientSecret:   os.Getenv("CAMUNDA_CLIENT_SECRET"),
			Audience:       getenv("CAMUNDA_TOKEN_AUDIENCE", "zeebe.camunda.io"),
			Scope:          os.Getenv("CAMUNDA_TOKEN_SCOPE"),
			TenantID:       os.Getenv("CAMUNDA_TENANT_ID"),
			RequestTimeout: getDuration("CAMUNDA_REQUEST_TIMEOUT", 30*time.Second),
		},
		Database: DatabaseConfig{
			URL:      os.Getenv("DATABASE_URL"),
			MaxConns: int32(getInt("DATABASE_MAX_CONNS", 10)),
		},
		Worker: WorkerConfig{
			Name:             getenv("WORKER_NAME", hostname()),
			MaxJobs:          getInt("WORKER_MAX_JOBS", 32),
			Concurrency:      getInt("WORKER_CONCURRENCY", 16),
			JobTimeout:       getDuration("WORKER_JOB_TIMEOUT", 5*time.Minute),
			PollInterval:     getDuration("WORKER_POLL_INTERVAL", 1*time.Second),
			RequestTimeout:   getDuration("WORKER_REQUEST_TIMEOUT", 20*time.Second),
			MetricsPort:      getenv("WORKER_METRICS_PORT", "9091"),
			JobTypes:         getList("WORKER_JOB_TYPES"),
			HTTPAllowedHosts: getList("WORKER_HTTP_ALLOWED_HOSTS"),
			ShutdownGrace:    getDuration("WORKER_SHUTDOWN_GRACE", 30*time.Second),
		},
		Monitor: MonitorConfig{
			IncidentPollInterval: getDuration("MONITOR_INCIDENT_POLL_INTERVAL", 30*time.Second),
			EventRetention:       getDuration("EVENT_RETENTION", 30*24*time.Hour),
			IdempotencyRetention: getDuration("IDEMPOTENCY_RETENTION", 24*time.Hour),
		},
	}

	keys, err := parseAPIKeys(os.Getenv("API_KEYS"))
	if err != nil {
		return nil, err
	}
	cfg.Auth.APIKeys = keys

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	var errs []error
	switch c.Camunda.AuthMode {
	case CamundaAuthNone:
	case CamundaAuthBasic:
		if c.Camunda.Username == "" {
			errs = append(errs, errors.New("CAMUNDA_USERNAME é obrigatório quando CAMUNDA_AUTH_MODE=basic"))
		}
	case CamundaAuthOAuth:
		if c.Camunda.OAuthURL == "" || c.Camunda.ClientID == "" || c.Camunda.ClientSecret == "" {
			errs = append(errs, errors.New("CAMUNDA_OAUTH_URL, CAMUNDA_CLIENT_ID e CAMUNDA_CLIENT_SECRET são obrigatórios quando CAMUNDA_AUTH_MODE=oauth"))
		}
	default:
		errs = append(errs, fmt.Errorf("CAMUNDA_AUTH_MODE inválido: %q (use none, basic ou oauth)", c.Camunda.AuthMode))
	}
	if c.Worker.Concurrency < 1 {
		errs = append(errs, errors.New("WORKER_CONCURRENCY deve ser >= 1"))
	}
	if c.Worker.MaxJobs < 1 {
		errs = append(errs, errors.New("WORKER_MAX_JOBS deve ser >= 1"))
	}
	if c.IsProduction() && len(c.Auth.APIKeys) == 0 {
		errs = append(errs, errors.New("API_KEYS é obrigatório em produção"))
	}
	return errors.Join(errs...)
}

func (c *Config) IsProduction() bool { return c.Env == "production" }

var validRoles = map[string]bool{"admin": true, "operator": true, "viewer": true}

func parseAPIKeys(raw string) (map[string]APIKey, error) {
	keys := map[string]APIKey{}
	for i, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		key := strings.TrimSpace(parts[0])
		role := "admin"
		if len(parts) > 1 {
			role = strings.ToLower(strings.TrimSpace(parts[1]))
		}
		name := fmt.Sprintf("apikey-%d", i+1)
		if len(parts) > 2 && strings.TrimSpace(parts[2]) != "" {
			name = strings.TrimSpace(parts[2])
		}
		if len(key) < 16 {
			return nil, errors.New("API_KEYS: cada chave deve ter ao menos 16 caracteres")
		}
		if !validRoles[role] {
			return nil, fmt.Errorf("API_KEYS: papel inválido %q", role)
		}
		if _, dup := keys[key]; dup {
			return nil, errors.New("API_KEYS: chave duplicada")
		}
		keys[key] = APIKey{Role: role, Name: name}
	}
	return keys, nil
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getList(key string) []string {
	var out []string
	for _, s := range strings.Split(os.Getenv(key), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "nexusflow-worker"
	}
	return h
}
