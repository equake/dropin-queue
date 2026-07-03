# Dockerfile para o shimd (clone de SNS/SQS).
#
# Multi-stage build:
#   1. Builder: compila shimd com Go 1.22
#   2. Runtime: scratch + ca-certs + tzdata (mínimo absoluto, distroless-like)
#
# Tamanho esperado do runtime: ~15 MB (binário estático).
# Não inclui o broker (rodando em container separado via docker-compose).

# --- Stage 1: builder ---
FROM golang:1.25-alpine AS builder

# Versão injetada via build-arg para -ldflags.
ARG VERSION=dev

WORKDIR /src

# Copia go.mod/go.sum primeiro para cache de layers de dependências.
COPY shim/go.mod shim/go.sum ./
RUN go mod download

# Copia código-fonte.
COPY shim/ ./

# Build estático (CGO disabled) com símbolos removidos e path trimming.
# -trimpath remove paths absolutos do binário (reprodutibilidade).
# -ldflags '-s -w' remove tabela de símbolos (menor binário).
# -X injeta Version variável.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X github.com/anomalyco/generic_queue/shim/internal/observability.Version=${VERSION}" \
    -o /out/shimd \
    ./cmd/shimd

# --- Stage 2: runtime ---
FROM gcr.io/distroless/static-debian12:nonroot

# Metadata.
LABEL org.opencontainers.image.title="shimd" \
      org.opencontainers.image.description="AWS SNS/SQS clone, NATS JetStream-backed" \
      org.opencontainers.image.source="https://github.com/anomalyco/generic_queue" \
      org.opencontainers.image.licenses="Apache-2.0"

# Copia binário do builder.
COPY --from=builder /out/shimd /shimd

# distroless:nonroot já roda como UID 65532. Expõe porta padrão.
EXPOSE 4566

# Healthcheck usando /healthz.
# Como distroless não tem shell/curl, usamos uma alternativa: o orchestrator
# (k8s/docker) testa /healthz diretamente. Aqui só configuramos entrypoint.
ENTRYPOINT ["/shimd"]
