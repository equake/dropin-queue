// Package server implementa o roteador HTTP do shim.
//
// Endpoints:
//
//	POST /                  — entrada principal de operações AWS (SQS e SNS)
//	GET  /healthz           — liveness probe (sempre 200 se processo vivo)
//	GET  /readyz            — readiness probe (200 só se broker conectado)
//	GET  /metrics           — Prometheus metrics
//
// O roteamento de operações é feito por inspeção do body
// (X-Amz-Target header para JSON, action no form-encoded para Query),
// não pelo path. Isso reflete o comportamento real do AWS SQS/SNS onde
// o mesmo endpoint recebe todas as operações.
package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/sns"
	"github.com/equake/dropin-queue/shim/internal/sqs"
	"github.com/equake/dropin-queue/shim/internal/storage"
)

// Handlers contém os services injetados no servidor.
type Handlers struct {
	Storage storage.Storage
	SQS     *sqs.Service
	SNS     *sns.Service
}

// Server é o servidor HTTP do shim.
type Server struct {
	handlers *Handlers
	addr     string
	srv      *http.Server
	router   *chi.Mux

	// maxBodyBytes limita bytes de body por request em roteamento.
	// Vem de cfg.MaxRequestBodyBytes (default 256 KiB). Antes do
	// refactor/kiss-dry-pass-1 era literal 1 MB hardcoded em
	// isSNSQueryRequest.
	maxBodyBytes int64
}

// New constrói um Server pronto para ListenAndServe.
//
// maxBodyBytes limita o tamanho de body aceito (deve bater com
// cfg.MaxRequestBodyBytes para defesa em profundidade contra oversize).
// Default 5 MB se não fornecido.
func New(addr string, h *Handlers, maxBodyBytes int64) *Server {
	if maxBodyBytes <= 0 {
		maxBodyBytes = 5 << 20 // 5 MB; cobre SQS+SNS no pior caso
	}
	s := &Server{
		handlers:     h,
		addr:         addr,
		router:       chi.NewRouter(),
		maxBodyBytes: maxBodyBytes,
	}
	s.routes()
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	return s
}

// routes registra todas as rotas do server.
func (s *Server) routes() {
	// Middleware global.
	s.router.Use(requestIDMiddleware)
	s.router.Use(recoveryMiddleware)
	s.router.Use(loggingMiddleware)
	s.router.Use(metricsMiddleware)

	// AWS endpoints.
	s.router.Post("/", s.handleAWS)

	// Ops endpoints.
	s.router.Get("/healthz", s.healthz)
	s.router.Get("/readyz", s.readyz)
	s.router.Get("/metrics", promhttp.HandlerFor(observability.Registry(), promhttp.HandlerOpts{
		Registry:          observability.Registry(),
		EnableOpenMetrics: true,
	}).ServeHTTP)
}

// ListenAndServe inicia o servidor. Bloqueia até erro.
func (s *Server) ListenAndServe() error {
	observability.L().Info("shim HTTP server starting", "addr", s.addr)
	return s.srv.ListenAndServe()
}

// Shutdown para o servidor graciosamente.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// --- Handlers ---

// handleAWS é o entrypoint principal para todas as operações AWS.
//
// Pré-fix (refactor/kiss-dry-pass-1) o roteamento lia o body 1× para
// detectar SQS vs SNS (sniff), depois parseava dnv no handler — 2 RTTs
// de parsing por request. Agora lemos o body 1× com limite de
// cfg.MaxRequestBodyBytes e parseamos direto. Substituímos o body do
// request por um buffer, e o handler downstream lê desse buffer.
//
// Detecção de protocolo:
//  1. Content-Type = application/x-amz-json-* → JSON path (decide
//     SQS vs SNS via X-Amz-Target prefix).
//  2. Senão, ler body. O `Action=` é a única forma de discriminar SQS
//     vs SNS em Query protocol.
//
// Fail-fast: body inválido ou > limite → erro coerente antes de dispatch.
func (s *Server) handleAWS(w http.ResponseWriter, r *http.Request) {
	if isJSONProtocol(r) {
		s.handleAWSJSONDispatch(w, r)
		return
	}
	s.handleAWSQueryDispatch(w, r)
}

// handleAWSJSONDispatch roteia requests JSON 1.0 entre SQS e SNS.
//
// O r.Body permanece intocado (cada parser lê o body inteiro uma vez);
// a única "discriminação" extra é a string X-Amz-Target que já está
// em headers — sem sniff.
func (s *Server) handleAWSJSONDispatch(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	if strings.HasPrefix(target, "AmazonSNS.") {
		s.handleSNSJSON(w, r)
		return
	}
	s.handleAWSJSON(w, r)
}

// handleAWSQueryDispatch lê o body 1× com limite explícito e roteia
// Query requests SQS vs SNS.
//
// Substitui o pré-fix isSNSQueryRequest que lia 1 MB do body, parseava,
// e RESTAURAVA o body para o handler reler (desperdício de CPU + memória
// em cada request, com risco de silent fallback se body > 1 MB).
func (s *Server) handleAWSQueryDispatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, s.maxBodyBytes))
	if err != nil {
		writeFatalError(w, transportSQSQuery, "InvalidParameterValue", "falha ao ler body: "+err.Error(), requestFromContext(r))
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Discrimina SQS vs SNS pelo `Action` value (única forma em Query).
	vals, perr := url.ParseQuery(string(body))
	if perr != nil {
		writeFatalError(w, transportSQSQuery, "InvalidParameterValue", "falha ao parsear body: "+perr.Error(), requestFromContext(r))
		return
	}
	if isSNSAction(protocol.Action(vals.Get("Action"))) {
		s.handleSNSQuery(w, r)
		return
	}
	s.handleAWSQuery(w, r)
}

// isSNSAction retorna true se a Action pertence ao set SNS.
//
// ListSubscriptionsByTopic NÃO tem Action dedicada no protocolo AWS
// usado em conjunto com format `entry.N.Name/Value` envelope —
// boto3 manda Action=ListSubscriptionsByTopic (não ListSubscriptions).
// Incluímos explicitamente.
func isSNSAction(action protocol.Action) bool {
	switch action {
	case protocol.ActionCreateTopic, protocol.ActionGetTopicAttributes,
		protocol.ActionListTopics, protocol.ActionSubscribe,
		protocol.ActionUnsubscribe, protocol.ActionDeleteTopic,
		protocol.ActionPublish, protocol.ActionListSubscriptions,
		protocol.ActionListSubscriptionsByTopic,
		protocol.ActionConfirmSubscription:
		return true
	}
	return false
}

// handleAWSQuery trata requests SQS no protocolo Query (form-encoded + XML).
func (s *Server) handleAWSQuery(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSQSQueryRequest(r)
	if err != nil {
		writeFatalError(w, transportSQSQuery, "InvalidParameterValue", err.Error(), requestFromContext(r))
		return
	}

	switch action {
	case protocol.ActionCreateQueue:
		s.handleCreateQueueQuery(w, r, params)
	case protocol.ActionGetQueueUrl:
		s.handleGetQueueUrlQuery(w, r, params)
	case protocol.ActionGetQueueAttributes:
		s.handleGetQueueAttributesQuery(w, r, params)
	case protocol.ActionListQueues:
		s.handleListQueuesQuery(w, r, params)
	case protocol.ActionDeleteQueue:
		s.handleDeleteQueueQuery(w, r, params)
	case protocol.ActionSendMessage:
		s.handleSendMessageQuery(w, r, params)
	case protocol.ActionReceiveMessage:
		s.handleReceiveMessageQuery(w, r, params)
	case protocol.ActionDeleteMessage:
		s.handleDeleteMessageQuery(w, r, params)
	case protocol.ActionChangeMessageVisibility:
		s.handleChangeMessageVisibilityQuery(w, r, params)
	case protocol.ActionPurgeQueue:
		s.handlePurgeQueueQuery(w, r, params)
	case protocol.ActionSetQueueAttributes:
		s.handleSetQueueAttributesQuery(w, r, params)
	case protocol.ActionSendMessageBatch:
		s.handleSendMessageBatchQuery(w, r, params)
	case protocol.ActionDeleteMessageBatch:
		s.handleDeleteMessageBatchQuery(w, r, params)
	default:
		writeFatalError(w, transportSQSQuery, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action), requestFromContext(r))
	}
}

// handleAWSJSON trata requests no protocolo JSON 1.0.
func (s *Server) handleAWSJSON(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSQSJSONRequest(r)
	if err != nil {
		writeFatalError(w, transportSQSJSON, "InvalidParameterValue", err.Error(), requestFromContext(r))
		return
	}

	switch action {
	case protocol.ActionCreateQueue:
		s.handleCreateQueueJSON(w, r, params)
	case protocol.ActionGetQueueUrl:
		s.handleGetQueueUrlJSON(w, r, params)
	case protocol.ActionGetQueueAttributes:
		s.handleGetQueueAttributesJSON(w, r, params)
	case protocol.ActionListQueues:
		s.handleListQueuesJSON(w, r, params)
	case protocol.ActionDeleteQueue:
		s.handleDeleteQueueJSON(w, r, params)
	case protocol.ActionSendMessage:
		s.handleSendMessageJSON(w, r, params)
	case protocol.ActionReceiveMessage:
		s.handleReceiveMessageJSON(w, r, params)
	case protocol.ActionDeleteMessage:
		s.handleDeleteMessageJSON(w, r, params)
	case protocol.ActionChangeMessageVisibility:
		s.handleChangeMessageVisibilityJSON(w, r, params)
	case protocol.ActionPurgeQueue:
		s.handlePurgeQueueJSON(w, r, params)
	case protocol.ActionSetQueueAttributes:
		s.handleSetQueueAttributesJSON(w, r, params)
	case protocol.ActionSendMessageBatch:
		s.handleSendMessageBatchJSON(w, r, params)
	case protocol.ActionDeleteMessageBatch:
		s.handleDeleteMessageBatchJSON(w, r, params)
	default:
		writeFatalError(w, transportSQSJSON, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action), requestFromContext(r))
	}
}

// handleCreateQueueQuery implementa CreateQueue via protocolo Query (XML response).

// --- Handlers SQS ---
//
// Movidos para sqs_handlers.go (refactor/kiss-dry-pass-2 Commit 1).
// Cada operação AWS SQS tem seu handle<Operation>Query e
// handle<Operation>JSON lá. O dispatch é feito pelos switches
// em handleAWSQuery e handleAWSJSON.

// --- SNNS handlers ---
//
// Movidos para sns_handlers.go (refactor/kiss-dry-pass-2 Commit 1).

// healthz é o liveness probe. Retorna 200 se o processo está vivo.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// readyz é o readiness probe. Retorna 200 se broker está conectado.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.handlers.Storage == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"no_storage"}`))
		return
	}
	// Ping é O(1) (status da conexão + AccountInfo). NÃO usar ListQueues
	// aqui: listagem é O(n) filas e o probe roda a cada poucos segundos
	// em cada réplica — com milhares de filas viraria carga real no broker.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.handlers.Storage.Ping(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"broker_unavailable","error":%q}`, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// --- Helpers ---

// isJSONProtocol detecta se a request usa AWS JSON 1.0.
//
// isJSONProtocol foi para response.go (refactor/kiss-dry-pass-1 Commit 6).
// Detectado por detectTransport que usa internamente.

// newRequestID gera um request ID único estilo AWS (hex 16 chars).
//
// ATENÇÃO (refactor/kiss-dry-pass-2 Commit 3): callers devem preferir
// requestFromContext(r) — gera ID consistente com X-Request-ID header
// exposto pelo middleware. newRequestID() direto causa divergência entre
// o ID no header e o ID no body XML. Mantido aqui por fallback
// (handlers fora do chi stack — só em tests).
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback extremamente raro (rand quebrado).
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// requestFromContext retorna o request ID armazenado pelo middleware
// (em r.Context()), ou gera novo se o handler foi invocado fora do
// chi stack (testes, recovery mid-pipeline).
//
// USO: handlers devem chamar requestFromContext(r) em vez de newRequestID()
// direto. Garante que X-Request-ID header (definido pelo middleware) E
// <RequestId> no body XML (escrito pelo handler) sejam o MESMO ID —
// correlação de logs e audit funciona corretamente.
func requestFromContext(r *http.Request) string {
	if id := requestIDFromCtx(r.Context()); id != "" {
		return id
	}
	return newRequestID()
}

// --- SNS handlers ---
