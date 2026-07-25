# Makefile do dropin-queue
#
# Targets principais:
#   make help              - lista todos os targets
#   make up                - sobe docker-compose (stack dev, backend NATS)
#   make up-postgres       - sobe docker-compose (stack dev, backend Postgres)
#   make down              - derruba docker-compose (todos os profiles)
#   make build             - builda o dropin-server
#   make test              - roda testes Go (adapter Postgres pula sem GQ_TEST_POSTGRES_DSN)
#   make test-int          - roda testes de integração (boto3 contra shim, backend NATS)
#   make test-int-postgres - roda a MESMA suíte de integração, backend Postgres
#   make smoke             - roda smoke test rápido
#   make lint              - roda golangci-lint
#   make fmt               - formata código Go

SHELL := /bin/bash

# Variáveis de paths
SHIM_DIR      := shim
BIN_DIR       := $(SHIM_DIR)/bin
SERVER_BIN    := $(BIN_DIR)/dropin-server
COMPOSE_FILE  := docker-compose.yml
PYTEST        := python3 -m pytest

# Cores para output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

.DEFAULT_GOAL := help

# Detecta o binário Go: prefere $(HOME)/go/bin/go (instalação manual), depois PATH
GO_BIN := $(shell command -v go 2>/dev/null || echo "$(HOME)/go/bin/go")

.PHONY: help
help: ## Lista todos os targets disponíveis
	@echo "$(GREEN)dropin-queue - Makefile$(RESET)"
	@echo ""
	@echo "Targets principais:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "Uso: make <target>"

# --- Setup ---

.PHONY: deps
deps: ## Baixa dependências Go
	cd $(SHIM_DIR) && $(GO_BIN) mod download
	cd $(SHIM_DIR) && $(GO_BIN) mod tidy

.PHONY: deps-py
deps-py: ## Instala dependências Python (boto3, pytest) em venv
	@if [ ! -d "$$HOME/venv" ]; then \
		echo "Criando venv em $$HOME/venv..."; \
		python3 -m venv $$HOME/venv; \
	fi
	$$HOME/venv/bin/pip install --quiet --upgrade boto3 pytest

# --- Build ---

.PHONY: build
build: ## Builda o binário dropin-server
	@mkdir -p $(BIN_DIR)
	cd $(SHIM_DIR) && CGO_ENABLED=0 $(GO_BIN) build -trimpath \
		-ldflags="-s -w" \
		-o ../$(SERVER_BIN) ./cmd/dropin-server
	@echo "$(GREEN)✓$(RESET) dropin-server compilado em $(SERVER_BIN)"

.PHONY: build-debug
build-debug: ## Builda dropin-server com símbolos de debug
	@mkdir -p $(BIN_DIR)
	cd $(SHIM_DIR) && $(GO_BIN) build -o ../$(SERVER_BIN) ./cmd/dropin-server
	@echo "$(GREEN)✓$(RESET) dropin-server (debug) compilado em $(SERVER_BIN)"

# --- Testes ---

.PHONY: test
test: ## Roda testes unitários Go
	cd $(SHIM_DIR) && $(GO_BIN) test -race -short ./...

.PHONY: test-verbose
test-verbose: ## Roda testes unitários com output verboso
	cd $(SHIM_DIR) && $(GO_BIN) test -race -v -short ./...

.PHONY: test-coverage
test-coverage: ## Roda testes com cobertura
	cd $(SHIM_DIR) && $(GO_BIN) test -race -coverprofile=coverage.out ./...
	cd $(SHIM_DIR) && $(GO_BIN) tool cover -func=coverage.out | tail -20

.PHONY: test-int
test-int: up ## Roda testes de integração (boto3 contra shim, backend NATS)
	@echo "Aguardando shim ficar pronto..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:4566/healthz > /dev/null 2>&1; then \
			echo "shim pronto"; break; \
		fi; \
		sleep 1; \
	done
	$(PYTEST) -v shim/test/integration/

.PHONY: test-int-postgres
test-int-postgres: up-postgres ## Roda a MESMA suíte de integração contra o backend Postgres
	@echo "Aguardando shim-postgres ficar pronto..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:4567/healthz > /dev/null 2>&1; then \
			echo "shim-postgres pronto"; break; \
		fi; \
		sleep 1; \
	done
	SHIM_ENDPOINT=http://localhost:4567 $(PYTEST) -v shim/test/integration/

.PHONY: smoke
smoke: up ## Roda apenas o smoke test rápido
	@echo "Aguardando shim ficar pronto..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:4566/healthz > /dev/null 2>&1; then \
			echo "shim pronto"; break; \
		fi; \
		sleep 1; \
	done
	$(PYTEST) -v shim/test/integration/test_sqs_smoke.py

# --- Docker Compose ---

.PHONY: up
up: build ## Sobe stack dev (NATS + MinIO + shim) — backend NATS
	docker compose -f $(COMPOSE_FILE) up -d --build
	@echo "$(GREEN)✓$(RESET) stack dev rodando (backend NATS)"
	@echo ""
	@echo "  shim:     http://localhost:4566"
	@echo "  nats:     nats://localhost:4222"
	@echo "  minio:    http://localhost:9000 (admin/minioadmin)"

.PHONY: up-postgres
up-postgres: build ## Sobe stack dev com backend Postgres (profile "postgres")
	docker compose -f $(COMPOSE_FILE) --profile postgres up -d --build
	@echo "$(GREEN)✓$(RESET) stack dev rodando (backend Postgres)"
	@echo ""
	@echo "  shim-postgres: http://localhost:4567"
	@echo "  postgres:      postgres://dropin:dropin@localhost:5432/dropin"

.PHONY: down
down: ## Derruba stack dev (todos os profiles, inclusive postgres)
	docker compose -f $(COMPOSE_FILE) --profile postgres down

.PHONY: down-v
down-v: ## Derruba stack dev e remove volumes (todos os profiles)
	docker compose -f $(COMPOSE_FILE) --profile postgres down -v

.PHONY: restart
restart: down up ## Reinicia stack dev

.PHONY: ps
ps: ## Lista containers rodando
	docker compose -f $(COMPOSE_FILE) ps

.PHONY: logs
logs: ## Tail logs de todos os serviços
	docker compose -f $(COMPOSE_FILE) logs -f

.PHONY: logs-shim
logs-shim: ## Tail logs do shim
	docker compose -f $(COMPOSE_FILE) logs -f shim

.PHONY: logs-nats
logs-nats: ## Tail logs do NATS
	docker compose -f $(COMPOSE_FILE) logs -f nats

.PHONY: shell-shim
shell-shim: ## Shell dentro do container do shim
	docker compose -f $(COMPOSE_FILE) exec shim /bin/sh

.PHONY: shell-nats
shell-nats: ## Shell dentro do container do NATS
	docker compose -f $(COMPOSE_FILE) exec nats /bin/sh

# --- Lint / Format ---

.PHONY: fmt
fmt: ## Formata código Go
	cd $(SHIM_DIR) && gofmt -s -w .
	@echo "$(GREEN)✓$(RESET) código formatado"

.PHONY: vet
vet: ## Roda go vet
	cd $(SHIM_DIR) && $(GO_BIN) vet ./...

.PHONY: lint
lint: ## Roda golangci-lint (requer instalação)
	@if command -v golangci-lint > /dev/null 2>&1; then \
		cd $(SHIM_DIR) && golangci-lint run ./...; \
	else \
		echo "golangci-lint não instalado. Usando 'go vet' como fallback."; \
		$(MAKE) vet; \
	fi

# --- Limpeza ---

.PHONY: clean
clean: ## Remove artefatos de build
	rm -rf $(BIN_DIR)
	rm -f $(SHIM_DIR)/coverage.out
	rm -rf $(SHIM_DIR)/dist

.PHONY: clean-all
clean-all: clean down-v ## Limpa tudo (artefatos + volumes docker)

# --- Desenvolvimento ---

.PHONY: run-local
run-local: build ## Roda dropin-server localmente (sem docker)
	@./$(SERVER_BIN) \
		--addr=:4566 \
		--nats-url=nats://localhost:4222 \
		--auth-mode=off \
		--log-level=debug

# --- Verificação ---

.PHONY: verify
verify: fmt vet test build ## Verifica que tudo compila, formata e passa testes
	@echo "$(GREEN)✓$(RESET) tudo OK"
