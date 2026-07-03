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
	"net/url"
	"sort"
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
		s.handleCreateQueueQuery(w, r, params)
	case protocol.ActionGetQueueUrl:
		s.handleGetQueueUrlQuery(w, r, params)
	case protocol.ActionGetQueueAttributes:
		s.handleGetQueueAttributesQuery(w, r, params)
	case protocol.ActionListQueues:
		s.handleListQueuesQuery(w, r, params)
	case protocol.ActionDeleteQueue:
		s.handleDeleteQueueQuery(w, r, params)
	default:
		writeSQSFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action), newRequestID())
	}
}

// handleAWSJSON trata requests no protocolo JSON 1.0.
func (s *Server) handleAWSJSON(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSQSJSONRequest(r)
	if err != nil {
		writeSQSJSONFatalError(w, "InvalidParameterValue", err.Error())
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
	default:
		writeSQSJSONFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action))
	}
}

// handleCreateQueueQuery implementa CreateQueue via protocolo Query (XML response).
func (s *Server) handleCreateQueueQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	defer observability.ObserveHTTP("sqs", string(protocol.ActionCreateQueue), "200", time.Since(time.Now()))

	cqp := sqs.CreateQueueParamsFromQuery(params)
	q, err := s.handlers.SQS.CreateQueue(r.Context(), cqp)
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		observability.ObserveHTTP("sqs", string(protocol.ActionCreateQueue), statusFromAWSError(awsErr), 0)
		return
	}

	observability.ObserveHTTP("sqs", string(protocol.ActionCreateQueue), "200", 0)

	type createQueueResult struct {
		XMLName  xml.Name `xml:"CreateQueueResult"`
		QueueURL string   `xml:"QueueUrl"`
	}
	type response struct {
		XMLName  xml.Name                  `xml:"CreateQueueResponse"`
		Xmlns    string                    `xml:"xmlns,attr"`
		Result   createQueueResult
		Metadata protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   createQueueResult{QueueURL: q.URL},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// handleCreateQueueJSON implementa CreateQueue via protocolo JSON 1.0 (JSON response).
func (s *Server) handleCreateQueueJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	cqp := sqs.CreateQueueParamsFromJSON(params)
	q, err := s.handlers.SQS.CreateQueue(r.Context(), cqp)
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	protocol.EncodeSQSJSONResponse(w, map[string]string{"QueueUrl": q.URL})
}

// writeSQSJSONFatalError serializa erro SQS JSON 1.0 com status correto.
func writeSQSJSONFatalError(w http.ResponseWriter, code, message string) {
	awsErr := &sqs.AWSError{Code: code, Message: message}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	status := http.StatusInternalServerError
	if awsErr.IsSenderFault() {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	protocol.EncodeSQSJSONError(w, code, message)
}

// statusFromAWSError converte classificação para string de status HTTP.
func statusFromAWSError(e *sqs.AWSError) string {
	if e.IsSenderFault() {
		return "400"
	}
	return "500"
}

// --- Handlers Query (XML) ---

// handleGetQueueUrlQuery implementa GetQueueUrl (protocolo Query).
func (s *Server) handleGetQueueUrlQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	q, err := s.handlers.SQS.GetQueueUrl(r.Context(), sqs.GetQueueUrlParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type result struct {
		XMLName  xml.Name `xml:"GetQueueUrlResult"`
		QueueURL string   `xml:"QueueUrl"`
	}
	type response struct {
		XMLName  xml.Name                  `xml:"GetQueueUrlResponse"`
		Xmlns    string                    `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   result{QueueURL: q.URL},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// handleGetQueueAttributesQuery implementa GetQueueAttributes (Query).
func (s *Server) handleGetQueueAttributesQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	attrs, err := s.handlers.SQS.GetQueueAttributes(r.Context(), sqs.GetQueueAttributesParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type attr struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type result struct {
		XMLName   xml.Name `xml:"GetQueueAttributesResult"`
		Attribute []attr   `xml:"Attribute"`
	}
	type response struct {
		XMLName  xml.Name                  `xml:"GetQueueAttributesResponse"`
		Xmlns    string                    `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}

	// Ordena chaves para resposta determinística.
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	attrList := make([]attr, 0, len(attrs))
	for _, k := range keys {
		attrList = append(attrList, attr{Name: k, Value: attrs[k]})
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   result{Attribute: attrList},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// handleListQueuesQuery implementa ListQueues (Query).
func (s *Server) handleListQueuesQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SQS.ListQueues(r.Context(), sqs.ListQueuesParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type result struct {
		XMLName   xml.Name `xml:"ListQueuesResult"`
		QueueURL  []string `xml:"QueueUrl"`
	}
	type response struct {
		XMLName   xml.Name                 `xml:"ListQueuesResponse"`
		Xmlns     string                   `xml:"xmlns,attr"`
		Result    result
		Metadata  protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   result{QueueURL: res.QueueUrls},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// handleDeleteQueueQuery implementa DeleteQueue (Query).
func (s *Server) handleDeleteQueueQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SQS.DeleteQueue(r.Context(), sqs.DeleteQueueParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type response struct {
		XMLName  xml.Name                  `xml:"DeleteQueueResponse"`
		Xmlns    string                    `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// --- Handlers JSON 1.0 ---

func (s *Server) handleGetQueueUrlJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	q, err := s.handlers.SQS.GetQueueUrl(r.Context(), sqs.GetQueueUrlParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	protocol.EncodeSQSJSONResponse(w, map[string]string{"QueueUrl": q.URL})
}

func (s *Server) handleGetQueueAttributesJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	attrs, err := s.handlers.SQS.GetQueueAttributes(r.Context(), sqs.GetQueueAttributesParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	// AWS JSON 1.0 GetQueueAttributes devolve {"Attributes": {"name": "value", ...}}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	protocol.EncodeSQSJSONResponse(w, map[string]map[string]string{
		"Attributes": attrs,
	})
}

func (s *Server) handleListQueuesJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SQS.ListQueues(r.Context(), sqs.ListQueuesParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	protocol.EncodeSQSJSONResponse(w, map[string]any{
		"QueueUrls": res.QueueUrls,
	})
}

func (s *Server) handleDeleteQueueJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SQS.DeleteQueue(r.Context(), sqs.DeleteQueueParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	// AWS JSON devolve {} em DeleteQueue bem-sucedido.
	w.Write([]byte(`{}`))
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
