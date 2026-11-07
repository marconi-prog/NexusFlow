package config

import "testing"

func TestParseAPIKeys(t *testing.T) {
	keys, err := parseAPIKeys("aaaaaaaaaaaaaaaa1:operator:erp, bbbbbbbbbbbbbbbb2:viewer, cccccccccccccccc3")
	if err != nil {
		t.Fatal(err)
	}
	if k := keys["aaaaaaaaaaaaaaaa1"]; k.Role != "operator" || k.Name != "erp" {
		t.Errorf("chave 1: %+v", k)
	}
	if k := keys["bbbbbbbbbbbbbbbb2"]; k.Role != "viewer" || k.Name != "apikey-2" {
		t.Errorf("chave 2: %+v", k)
	}
	if k := keys["cccccccccccccccc3"]; k.Role != "admin" {
		t.Errorf("chave 3 deveria ser admin: %+v", k)
	}
	for _, bad := range []string{"curta:admin", "aaaaaaaaaaaaaaaa1:root", "aaaaaaaaaaaaaaaa1,aaaaaaaaaaaaaaaa1"} {
		if _, err := parseAPIKeys(bad); err == nil {
			t.Errorf("esperava erro para %q", bad)
		}
	}
}

func TestLoadValidation(t *testing.T) {
	t.Setenv("CAMUNDA_AUTH_MODE", "oauth")
	if _, err := Load(); err == nil {
		t.Fatal("oauth sem credenciais deveria falhar")
	}
	t.Setenv("CAMUNDA_AUTH_MODE", "none")
	t.Setenv("APP_ENV", "production")
	t.Setenv("API_KEYS", "")
	if _, err := Load(); err == nil {
		t.Fatal("produção sem API_KEYS deveria falhar")
	}
	t.Setenv("API_KEYS", "aaaaaaaaaaaaaaaa1:admin")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Camunda.BaseURL == "" || cfg.Worker.Concurrency < 1 {
		t.Fatalf("defaults: %+v", cfg)
	}
}
