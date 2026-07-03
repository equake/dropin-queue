# Makefile do generic_queue
#
# Targets principais:
#   make help       - lista todos os targets
#   make up         - sobe docker-compose (stack dev)
#   make down       - derruba docker-compose
#   make build      - builda o shimd
#   make test       - roda testes Go
#   make test-int   - roda testes de integração (boto3 contra shim rodando)
#   make smoke      - roda smoke test rápido
#   make lint       - roda golangci-lint
#   make fmt        - formata código Go

SHELL := /bin/bash

# Variáveis de paths
SHIM_DIR      := shim
BIN_DIR       := $(SHIM_DIR)/bin
SHIMD_BIN     := $(BIN_DIR)/shimd
COMPOSE_FILE  := docker-compose.yml
PYTEST        := python3 -m pytest

# Cores para output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

.DEFAULT_GOAL := help

.PHONY: help
help: ## Lista todos os targets disponíveis
	@echo "$(GREEN)generic_queue - Makefile$(RESET)"
	@echo ""
	@echo "Targets principais:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "Uso: make <target>"

# --- Setup ---

.PHONY: deps
deps: ## Baixa dependências Go
	cd $(SHIM_DIR) && go mod download
	cd $(SHIM_DIR) && go mod tidy

.PHONY: deps-py
deps-py: ## Instala dependências Python (boto3, pytest) em venv
	@if [ ! -d "$$HOME/venv" ]; then \
		echo "Criando venv em $$HOME/venv..."; \
		python3 -m venv $$HOME/venv; \
	fi
	$$HOME/venv/bin/pip install --quiet --upgrade boto3 pytest

# --- Build ---

.PHONY: build
build: ## Builda o binário shimd
	@mkdir -p $(BIN_DIR)
	cd $(SHIM_DIR) && CGO_ENABLED=0 go build -trimpath \
		-ldflags="-s -w" \
		-o ../$(SHIMD_BIN) ./cmd/shimd
	@echo "$(GREEN)✓$(RESET) shimd compilado em $(SHIMD_BIN)"

.PHONY: build-debug
build-debug: ## Builda shimd com símbolos de debug
	@mkdir -p $(BIN_DIR)
	cd $(SHIM_DIR) && go build -o ../$(SHIMD_BIN) ./cmd/shimd
	@echo "$(GREEN)✓$(RESET) shimd (debug) compilado em $(SHIMD_BIN)"

# --- Testes ---

.PHONY: test
test: ## Roda testes unitários Go
	cd $(SHIM_DIR) && go test -race -short ./...

.PHONY: test-verbose
test-verbose: ## Roda testes unitários com output verboso
	cd $(SHIM_DIR) && go test -race -v -short ./...

.PHONY: test-coverage
test-coverage: ## Roda testes com cobertura
	cd $(SHIM_DIR) && go test -race -coverprofile=coverage.out ./...
	cd $(SHIM_DIR) && go tool cover -func=coverage.out | tail -20

.PHONY: test-int
test-int: up ## Roda testes de integração (boto3 contra shim)
	@echo "Aguardando shim ficar pronto..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if curl -sf http://localhost:4566/healthz > /dev/null 2>&1; then \
			echo "shim pronto"; break; \
		fi; \
		sleep 1; \
	done
	$(PYTEST) -v shim/test/integration/

.PHONY: smoke
smoke: up ## Roda apenas o smoke test da semana 1
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
up: build ## Sobe stack dev (NATS + MinIO + shim)
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "$(GREEN)✓$(RESET) stack dev rodando"
	@echo ""
	@echo "  shim:     http://localhost:4566"
	@echo "  nats:     nats://localhost:4222"
	@echo "  minio:    http://localhost:9000 (admin/minioadmin)"

.PHONY: down
down: ## Derruba stack dev
	docker compose -f $(COMPOSE_FILE) down

.PHONY: down-v
down-v: ## Derruba stack dev e remove volumes
	docker compose -f $(COMPOSE_FILE) down -v

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
	cd $(SHIM_DIR) && go vet ./...

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
run-local: build ## Roda shimd localmente (sem docker)
	@./$(SHIMD_BIN) \
		--addr=:4566 \
		--nats-url=nats://localhost:4222 \
		--auth-mode=off \
		--log-level=debug

# --- Verificação ---

.PHONY: verify
verify: fmt vet test build ## Verifica que tudo compila, formata e passa testes
	@echo "$(GREEN)✓$(RESET) tudo OK"
