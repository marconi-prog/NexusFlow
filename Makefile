VERSION ?= $(shell date +%Y.%m.%d)-dev
LDFLAGS := -s -w -X github.com/marconi-prog/nexusflow/internal/app.Version=$(VERSION)

.PHONY: build test test-integration vet run-api run-worker

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/nexusflow-api ./cmd/api
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/nexusflow-worker ./cmd/workers

test:
	go test -race ./...

# Requer um PostgreSQL descartável: make test-integration DB=postgres://...
test-integration:
	NEXUSFLOW_TEST_DATABASE_URL="$(DB)" go test -race -count=1 ./internal/infrastructure/database/...

vet:
	go vet ./...

run-api:
	set -a; [ -f .env ] && . ./.env; set +a; go run ./cmd/api

run-worker:
	set -a; [ -f .env ] && . ./.env; set +a; go run ./cmd/workers
