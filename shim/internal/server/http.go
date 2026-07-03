// Package server implementa o roteador HTTP do shim.
//
// Endpoints:
//
//   POST /                  — entrada principal de operações AWS (SQS e SNS)
//   GET  /healthz           — liveness probe (sempre 200 se processo vivo)
//   GET  /readyz            — readiness probe (200 só se broker conectado)
//   GET  /metrics           — Prometheus metrics
//
// O roteamento de operações é feito por inspeção do body
// (X-Amz-Target header para JSON, action no form-encoded para Query),
// não pelo path. Isso reflete o comportamento real do AWS SQS/SNS onde
// o mesmo endpoint recebe todas as operações.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/anomalyco/generic_queue/shim/internal/observability"
	"github.com/anomalyco/generic_queue/shim/internal/protocol"
	"github.com/anomalyco/generic_queue/shim/internal/sqs"
	"github.com/anomalyco/generic_queue/shim/internal/storage"
)

// Handlers contém os services injetados no servidor.
//
// Hoje só temos SQS; SNS entra na semana 4.
type Handlers struct {
	Storage storage.Storage
	SQS     *sqs.Service
	// SNS *sns.Service  // semana 4
}

// Server é o servidor HTTP do shim.
type Server struct {
	handlers *Handlers
	addr     string
	srv      *http.Server
	router   *chi.Mux
}

// New constrói um Server pronto para ListenAndServe.
func New(addr string, h *Handlers) *Server {
	s := &Server{
		handlers: h,
		addr:     addr,
		router:   chi.NewRouter(),
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
// Detecta protocolo (Query vs JSON), parse, dispatch, encode response.
func (s *Server) handleAWS(w http.ResponseWriter, r *http.Request) {
	// Detecta protocolo.
	if isJSONProtocol(r) {
		s.handleAWSJSON(w, r)
		return
	}
	s.handleAWSQuery(w, r)
}

// handleAWSQuery trata requests SQS/SNS no protocolo Query (form-encoded + XML).
func (s *Server) handleAWSQuery(w http.ResponseWriter, r *http.Request) {
	// Por enquanto só SQS Query está implementado. SNS Query virá na semana 4.
	action, params, err := protocol.ParseSQSQueryRequest(r)
	if err != nil {
		writeSQSFatalError(w, "InvalidParameterValue", err.Error(), newRequestID())
		return
	}

	switch action {
	case protocol.ActionCreateQueue:
		s.handleCreateQueue(w, r, params)
	default:
		writeSQSFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action), newRequestID())
	}
}

// handleAWSJSON trata requests no protocolo JSON 1.0.
//
// Será implementado na semana 2 — hoje devolve UnsupportedOperation.
func (s *Server) handleAWSJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, `{"__type":"UnsupportedOperationException","message":"AWS JSON 1.0 protocol will be implemented in week 2"}`)
}

// handleCreateQueue implementa POST / Action=CreateQueue.
func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request, params map[string][]string) {
	urlValues := make(map[string][]string, len(params))
	for k, v := range params {
		urlValues[k] = v
	}

	q, err := s.handlers.SQS.CreateQueue(r.Context(), urlValues)
	if err != nil {
		writeSQSFatalError(w, sqs.AsAWSError(err).Code, sqs.AsAWSError(err).Message, newRequestID())
		return
	}

	// Serializa resposta XML.
	type createQueueResult struct {
		XMLName xml.Name `xml:"CreateQueueResult"`
		QueueURL string  `xml:"QueueUrl"`
	}
	type response struct {
		XMLName  xml.Name `xml:"CreateQueueResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   createQueueResult
		Metadata protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:  "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result: createQueueResult{QueueURL: q.URL},
		Metadata: protocol.ResponseMetadata{
			RequestID: newRequestID(),
		},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// healthz é o liveness probe. Retorna 200 se o processo está vivo.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// readyz é o readiness probe. Retorna 200 se broker está conectado.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.handlers.Storage == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"no_storage"}`))
		return
	}
	// Tenta uma operação leve no storage como health check.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.handlers.Storage.Queues().ListQueues(ctx, ""); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"status":"broker_unavailable","error":%q}`, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ready"}`))
}

// --- Helpers ---

// isJSONProtocol detecta se a request usa AWS JSON 1.0.
//
// Critério: header X-Amz-Target presente (típico de JSON 1.0) E Content-Type
// começa com application/x-amz-json.
func isJSONProtocol(r *http.Request) bool {
	target := r.Header.Get("X-Amz-Target")
	ct := r.Header.Get("Content-Type")
	if target == "" {
		return false
	}
	return len(ct) >= 22 && ct[:22] == "application/x-amz-json"
}

// writeSQSFatalError serializa um erro SQS no formato XML.
//
// Códigos diferentes de 200 conforme tipo do erro (Sender → 400, Receiver → 500).
func writeSQSFatalError(w http.ResponseWriter, code, message, requestID string) {
	awsErr := &sqs.AWSError{Code: code, Message: message}
	w.Header().Set("Content-Type", "text/xml")
	status := http.StatusInternalServerError
	if awsErr.IsSenderFault() {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	protocol.EncodeSQSQueryError(w, code, message, requestID, awsErr.IsSenderFault())
}

// newRequestID gera um request ID único estilo AWS (hex 16 chars).
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback extremamente raro (rand quebrado).
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// ensure unused imports compile in older Go versions
var _ = errors.New
