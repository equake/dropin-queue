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
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/equake/dropin-queue/shim/internal/observability"
	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/sns"
	"github.com/equake/dropin-queue/shim/internal/sqs"
	"github.com/equake/dropin-queue/shim/internal/storage"
	"github.com/equake/dropin-queue/shim/pkg/types"
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
	// Detecta serviço (SQS vs SNS) baseado em X-Amz-Target prefix (JSON)
	// ou namespace do body (Query — não há header, mas SNS usa Action
	// list própria).
	if isJSONProtocol(r) {
		// JSON: usa X-Amz-Target prefix.
		target := r.Header.Get("X-Amz-Target")
		if strings.HasPrefix(target, "AmazonSNS.") {
			s.handleSNSJSON(w, r)
			return
		}
		s.handleAWSJSON(w, r)
		return
	}
	// Query: precisamos sniff o Action antes de parsear.
	// Lemos o body pequeno só para identificar (SNS tem Action values distintos).
	if isSNSQueryRequest(r) {
		s.handleSNSQuery(w, r)
		return
	}
	s.handleAWSQuery(w, r)
}

// isSNSQueryRequest detecta se um request Query é SNS (vs SQS).
//
// Estratégia: lê o body (até maxQuerySniffBytes) e checa se Action está no
// set SNS. O sniff é feito no início do body — Action sempre vem primeiro
// (boto3 ordena alfabeticamente: Action, ...), então ler o início basta.
//
// O body lido é restaurado em r.Body para que o handler posterior possa
// parseá-lo completamente (Publish com Message grande exige isso).
const maxQuerySniffBytes = 1 << 20 // 1 MB — maior que max body útil

func isSNSQueryRequest(r *http.Request) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxQuerySniffBytes))
	if err != nil {
		return false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return false
	}
	action := vals.Get("Action")
	switch protocol.Action(action) {
	case protocol.ActionCreateTopic, protocol.ActionGetTopicAttributes,
		protocol.ActionListTopics, protocol.ActionSubscribe,
		protocol.ActionUnsubscribe, protocol.ActionDeleteTopic,
		protocol.ActionPublish, protocol.ActionListSubscriptions,
		protocol.ActionConfirmSubscription:
		return true
	}
	// ListSubscriptionsByTopic não tem Action dedicada no protocolo AWS —
	// usa ListSubscriptions com TopicArn no body. Mas como Action é
	// "ListSubscriptions", cai em SQS parse → erro. Tratamos explicitamente.
	return action == "ListSubscriptionsByTopic"
}

// handleAWSQuery trata requests SQS no protocolo Query (form-encoded + XML).
func (s *Server) handleAWSQuery(w http.ResponseWriter, r *http.Request) {
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
		writeSQSJSONFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action %q ainda não implementada", action))
	}
}

// handleCreateQueueQuery implementa CreateQueue via protocolo Query (XML response).
func (s *Server) handleCreateQueueQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	start := time.Now()
	defer func() {
		observability.ObserveHTTP("sqs", string(protocol.ActionCreateQueue), "200", time.Since(start))
	}()

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
		XMLName  xml.Name `xml:"CreateQueueResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
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
	_, _ = w.Write([]byte(xml.Header))
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
	_ = protocol.EncodeSQSJSONResponse(w, map[string]string{"QueueUrl": q.URL})
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
	_ = protocol.EncodeSQSJSONError(w, code, message)
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
		XMLName  xml.Name `xml:"GetQueueUrlResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
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
	_, _ = w.Write([]byte(xml.Header))
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
		XMLName  xml.Name `xml:"GetQueueAttributesResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
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
	_, _ = w.Write([]byte(xml.Header))
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
		XMLName  xml.Name `xml:"ListQueuesResult"`
		QueueURL []string `xml:"QueueUrl"`
	}
	type response struct {
		XMLName  xml.Name `xml:"ListQueuesResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   result{QueueURL: res.QueueUrls},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
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
		XMLName  xml.Name `xml:"DeleteQueueResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
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
	_ = protocol.EncodeSQSJSONResponse(w, map[string]string{"QueueUrl": q.URL})
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
	_ = protocol.EncodeSQSJSONResponse(w, map[string]map[string]string{
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
	_ = protocol.EncodeSQSJSONResponse(w, map[string]any{
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
	_, _ = w.Write([]byte(`{}`))
}

// --- SendMessage ---

func (s *Server) handleSendMessageQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SQS.SendMessage(r.Context(), sqs.SendMessageParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type result struct {
		XMLName    xml.Name `xml:"SendMessageResult"`
		MessageID  string   `xml:"MessageId"`
		MD5OfBody  string   `xml:"MD5OfMessage"`
		SequenceNo string   `xml:"SequenceNumber,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:"SendMessageResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns: "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result: result{
			MessageID:  res.MessageID,
			MD5OfBody:  res.MD5OfBody,
			SequenceNo: res.SequenceNo,
		},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSendMessageJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SQS.SendMessage(r.Context(), sqs.SendMessageParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = protocol.EncodeSQSJSONResponse(w, map[string]string{
		"MessageId":      res.MessageID,
		"MD5OfMessage":   res.MD5OfBody,
		"SequenceNumber": res.SequenceNo,
	})
}

// --- ReceiveMessage ---

func (s *Server) handleReceiveMessageQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SQS.ReceiveMessage(r.Context(), sqs.ReceiveMessageParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}

	type msgAttr struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type msgMA struct {
		Name     string `xml:"Name"`
		Value    string `xml:"Value,omitempty"`
		ValueBin []byte `xml:"BinaryValue,omitempty"`
	}
	type msg struct {
		XMLName       xml.Name  `xml:"Message"`
		MessageID     string    `xml:"MessageId"`
		ReceiptHandle string    `xml:"ReceiptHandle"`
		MD5OfBody     string    `xml:"MD5OfBody"`
		Body          string    `xml:"Body"`
		Attribute     []msgAttr `xml:"Attribute"`
		MessageMA     []msgMA   `xml:"MessageAttribute"`
	}
	type result struct {
		XMLName xml.Name `xml:"ReceiveMessageResult"`
		Message []msg
	}
	type response struct {
		XMLName  xml.Name `xml:"ReceiveMessageResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}

	out := make([]msg, 0, len(res.Messages))
	for _, m := range res.Messages {
		mx := msg{
			MessageID:     m.ID,
			ReceiptHandle: m.ReceiptHandle,
			MD5OfBody:     m.MD5OfBody,
			Body:          m.Body,
		}
		// Atributos ordenados.
		keys := make([]string, 0, len(m.Attributes))
		for k := range m.Attributes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			mx.Attribute = append(mx.Attribute, msgAttr{Name: k, Value: m.Attributes[k]})
		}
		// MessageAttributes ordenadas.
		maKeys := make([]string, 0, len(m.MessageAttributes))
		for k := range m.MessageAttributes {
			maKeys = append(maKeys, k)
		}
		sort.Strings(maKeys)
		for _, k := range maKeys {
			attr := m.MessageAttributes[k]
			switch attr.DataType {
			case "Binary":
				mx.MessageMA = append(mx.MessageMA, msgMA{Name: k, ValueBin: attr.BinaryValue})
			default:
				mx.MessageMA = append(mx.MessageMA, msgMA{Name: k, Value: attr.StringValue})
			}
		}
		out = append(out, mx)
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result:   result{Message: out},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleReceiveMessageJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SQS.ReceiveMessage(r.Context(), sqs.ReceiveMessageParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}

	out := make([]map[string]any, 0, len(res.Messages))
	for _, m := range res.Messages {
		mx := map[string]any{
			"MessageId":     m.ID,
			"ReceiptHandle": m.ReceiptHandle,
			"MD5OfBody":     m.MD5OfBody,
			"Body":          m.Body,
			"Attributes":    m.Attributes,
		}
		// MessageAttributes no AWS JSON 1.0 é um MAP:
		// {"foo": {"DataType":"String","StringValue":"bar"}, ...}
		ma := make(map[string]map[string]any, len(m.MessageAttributes))
		for k, v := range m.MessageAttributes {
			entry := map[string]any{"DataType": v.DataType}
			switch v.DataType {
			case "Binary":
				entry["BinaryValue"] = base64.StdEncoding.EncodeToString(v.BinaryValue)
			case "String.List":
				entry["StringListValues"] = strings.Split(v.StringValue, "|")
			default:
				entry["StringValue"] = v.StringValue
			}
			ma[k] = entry
		}
		mx["MessageAttributes"] = ma
		out = append(out, mx)
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = protocol.EncodeSQSJSONResponse(w, map[string]any{"Messages": out})
}

// --- DeleteMessage ---

func (s *Server) handleDeleteMessageQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SQS.DeleteMessage(r.Context(), sqs.DeleteMessageParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"DeleteMessageResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleDeleteMessageJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SQS.DeleteMessage(r.Context(), sqs.DeleteMessageParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

// --- ChangeMessageVisibility ---

func (s *Server) handleChangeMessageVisibilityQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SQS.ChangeMessageVisibility(r.Context(), sqs.ChangeMessageVisibilityParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"ChangeMessageVisibilityResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleChangeMessageVisibilityJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SQS.ChangeMessageVisibility(r.Context(), sqs.ChangeMessageVisibilityParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

// --- PurgeQueue ---

func (s *Server) handlePurgeQueueQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SQS.PurgeQueue(r.Context(), sqs.PurgeQueueParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"PurgeQueueResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handlePurgeQueueJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SQS.PurgeQueue(r.Context(), sqs.PurgeQueueParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

// --- SetQueueAttributes ---

func (s *Server) handleSetQueueAttributesQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SQS.SetQueueAttributes(r.Context(), sqs.SetQueueAttributesParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"SetQueueAttributesResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSetQueueAttributesJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SQS.SetQueueAttributes(r.Context(), sqs.SetQueueAttributesParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

// --- SendMessageBatch ---

// SendMessageBatchResultEntryQuery é uma entry bem-sucedida do SendMessageBatch (Query).
//
// XML tags em ordem lexicográfica para determinismo (não estritamente necessário,
// mas ajuda no diff de testes).
type sendMessageBatchResultEntryQuery struct {
	XMLName          xml.Name `xml:"SendMessageBatchResultEntry"`
	Id               string   `xml:"Id"`
	MessageId        string   `xml:"MessageId"`
	MD5OfMessageBody string   `xml:"MD5OfMessageBody"`
	SequenceNumber   string   `xml:"SequenceNumber,omitempty"`
}

type batchFailureEntryQuery struct {
	XMLName     xml.Name `xml:"BatchResultErrorEntry"`
	Id          string   `xml:"Id"`
	Code        string   `xml:"Code"`
	Message     string   `xml:"Message"`
	SenderFault bool     `xml:"SenderFault"`
}

type sendMessageBatchResultQuery struct {
	XMLName    xml.Name                           `xml:"SendMessageBatchResult"`
	Successful []sendMessageBatchResultEntryQuery `xml:"SendMessageBatchResultEntry"`
	Failed     []batchFailureEntryQuery           `xml:"BatchResultErrorEntry"`
}

type sendMessageBatchResponseQuery struct {
	XMLName  xml.Name `xml:"SendMessageBatchResponse"`
	Xmlns    string   `xml:"xmlns,attr"`
	Result   sendMessageBatchResultQuery
	Metadata protocol.ResponseMetadata
}

func (s *Server) handleSendMessageBatchQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	result, err := s.handlers.SQS.SendMessageBatch(r.Context(), sqs.SendMessageBatchParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)

	resp := sendMessageBatchResponseQuery{
		Xmlns: "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result: sendMessageBatchResultQuery{
			Successful: make([]sendMessageBatchResultEntryQuery, 0, len(result.Successful)),
			Failed:     make([]batchFailureEntryQuery, 0, len(result.Failed)),
		},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	for _, e := range result.Successful {
		resp.Result.Successful = append(resp.Result.Successful, sendMessageBatchResultEntryQuery{
			Id:               e.Id,
			MessageId:        e.MessageID,
			MD5OfMessageBody: e.MD5OfBody,
			SequenceNumber:   e.SequenceNo,
		})
	}
	for _, e := range result.Failed {
		resp.Result.Failed = append(resp.Result.Failed, batchFailureEntryQuery{
			Id:          e.Id,
			Code:        e.Code,
			Message:     e.Message,
			SenderFault: e.SenderFault,
		})
	}

	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// sendMessageBatchResultEntryJSON é uma entry bem-sucedida em JSON.
type sendMessageBatchResultEntryJSON struct {
	Id               string `json:"Id"`
	MessageId        string `json:"MessageId"`
	MD5OfMessageBody string `json:"MD5OfMessageBody"`
	SequenceNumber   string `json:"SequenceNumber,omitempty"`
}

// batchFailureEntryJSON é uma entry falha em JSON.
type batchFailureEntryJSON struct {
	Id          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}

type sendMessageBatchResponseJSON struct {
	Successful []sendMessageBatchResultEntryJSON `json:"Successful"`
	Failed     []batchFailureEntryJSON           `json:"Failed"`
}

func (s *Server) handleSendMessageBatchJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	result, err := s.handlers.SQS.SendMessageBatch(r.Context(), sqs.SendMessageBatchParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)

	resp := sendMessageBatchResponseJSON{
		Successful: make([]sendMessageBatchResultEntryJSON, 0, len(result.Successful)),
		Failed:     make([]batchFailureEntryJSON, 0, len(result.Failed)),
	}
	for _, e := range result.Successful {
		resp.Successful = append(resp.Successful, sendMessageBatchResultEntryJSON{
			Id:               e.Id,
			MessageId:        e.MessageID,
			MD5OfMessageBody: e.MD5OfBody,
			SequenceNumber:   e.SequenceNo,
		})
	}
	for _, e := range result.Failed {
		resp.Failed = append(resp.Failed, batchFailureEntryJSON{
			Id:          e.Id,
			Code:        e.Code,
			Message:     e.Message,
			SenderFault: e.SenderFault,
		})
	}

	_ = json.NewEncoder(w).Encode(resp)
}

// --- DeleteMessageBatch ---

type deleteMessageBatchResultEntryQuery struct {
	XMLName xml.Name `xml:"DeleteMessageBatchResultEntry"`
	Id      string   `xml:"Id"`
}

type deleteMessageBatchResultQuery struct {
	XMLName    xml.Name                             `xml:"DeleteMessageBatchResult"`
	Successful []deleteMessageBatchResultEntryQuery `xml:"DeleteMessageBatchResultEntry"`
	Failed     []batchFailureEntryQuery             `xml:"BatchResultErrorEntry"`
}

type deleteMessageBatchResponseQuery struct {
	XMLName  xml.Name `xml:"DeleteMessageBatchResponse"`
	Xmlns    string   `xml:"xmlns,attr"`
	Result   deleteMessageBatchResultQuery
	Metadata protocol.ResponseMetadata
}

func (s *Server) handleDeleteMessageBatchQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	result, err := s.handlers.SQS.DeleteMessageBatch(r.Context(), sqs.DeleteMessageBatchParamsFromQuery(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)

	resp := deleteMessageBatchResponseQuery{
		Xmlns: "http://queue.amazonaws.com/doc/" + protocol.AWSProtocolVersion,
		Result: deleteMessageBatchResultQuery{
			Successful: make([]deleteMessageBatchResultEntryQuery, 0, len(result.Successful)),
			Failed:     make([]batchFailureEntryQuery, 0, len(result.Failed)),
		},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	for _, e := range result.Successful {
		resp.Result.Successful = append(resp.Result.Successful, deleteMessageBatchResultEntryQuery{Id: e.Id})
	}
	for _, e := range result.Failed {
		resp.Result.Failed = append(resp.Result.Failed, batchFailureEntryQuery{
			Id:          e.Id,
			Code:        e.Code,
			Message:     e.Message,
			SenderFault: e.SenderFault,
		})
	}

	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

type deleteMessageBatchResultEntryJSON struct {
	Id string `json:"Id"`
}

type deleteMessageBatchResponseJSON struct {
	Successful []deleteMessageBatchResultEntryJSON `json:"Successful"`
	Failed     []batchFailureEntryJSON             `json:"Failed"`
}

func (s *Server) handleDeleteMessageBatchJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	result, err := s.handlers.SQS.DeleteMessageBatch(r.Context(), sqs.DeleteMessageBatchParamsFromJSON(params))
	if err != nil {
		awsErr := sqs.AsAWSError(err)
		writeSQSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)

	resp := deleteMessageBatchResponseJSON{
		Successful: make([]deleteMessageBatchResultEntryJSON, 0, len(result.Successful)),
		Failed:     make([]batchFailureEntryJSON, 0, len(result.Failed)),
	}
	for _, e := range result.Successful {
		resp.Successful = append(resp.Successful, deleteMessageBatchResultEntryJSON{Id: e.Id})
	}
	for _, e := range result.Failed {
		resp.Failed = append(resp.Failed, batchFailureEntryJSON{
			Id:          e.Id,
			Code:        e.Code,
			Message:     e.Message,
			SenderFault: e.SenderFault,
		})
	}

	_ = json.NewEncoder(w).Encode(resp)
}

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
	_ = protocol.EncodeSQSQueryError(w, code, message, requestID, awsErr.IsSenderFault())
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

// --- SNS handlers ---

// handleSNSQuery trata requests SNS no protocolo Query (form-encoded + XML).
func (s *Server) handleSNSQuery(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSNSQueryRequest(r)
	if err != nil {
		writeSNSFatalError(w, "InvalidParameterValue", err.Error(), newRequestID())
		return
	}

	switch action {
	case protocol.ActionCreateTopic:
		s.handleSNSCreateTopicQuery(w, r, params)
	case protocol.ActionGetTopicAttributes:
		s.handleSNSGetTopicAttributesQuery(w, r, params)
	case protocol.ActionListTopics:
		s.handleSNSListTopicsQuery(w, r, params)
	case protocol.ActionDeleteTopic:
		s.handleSNSDeleteTopicQuery(w, r, params)
	case protocol.ActionSubscribe:
		s.handleSNSSubscribeQuery(w, r, params)
	case protocol.ActionUnsubscribe:
		s.handleSNSUnsubscribeQuery(w, r, params)
	case protocol.ActionPublish:
		s.handleSNSPublishQuery(w, r, params)
	case protocol.ActionListSubscriptions:
		s.handleSNSListSubscriptionsQuery(w, r, params)
	case protocol.ActionListSubscriptionsByTopic:
		s.handleSNSListSubscriptionsByTopicQuery(w, r, params)
	case protocol.ActionConfirmSubscription:
		s.handleSNSConfirmSubscriptionQuery(w, r, params)
	default:
		writeSNSFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action SNS %q não implementada", action), newRequestID())
	}
}

// handleSNSJSON trata requests SNS no protocolo JSON 1.0.
func (s *Server) handleSNSJSON(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSNSJSONRequest(r)
	if err != nil {
		writeSNSJSONFatalError(w, "InvalidParameterValue", err.Error())
		return
	}

	switch action {
	case protocol.ActionCreateTopic:
		s.handleSNSCreateTopicJSON(w, r, params)
	case protocol.ActionGetTopicAttributes:
		s.handleSNSGetTopicAttributesJSON(w, r, params)
	case protocol.ActionListTopics:
		s.handleSNSListTopicsJSON(w, r, params)
	case protocol.ActionDeleteTopic:
		s.handleSNSDeleteTopicJSON(w, r, params)
	case protocol.ActionSubscribe:
		s.handleSNSSubscribeJSON(w, r, params)
	case protocol.ActionUnsubscribe:
		s.handleSNSUnsubscribeJSON(w, r, params)
	case protocol.ActionPublish:
		s.handleSNSPublishJSON(w, r, params)
	case protocol.ActionListSubscriptions:
		s.handleSNSListSubscriptionsJSON(w, r, params)
	case protocol.ActionListSubscriptionsByTopic:
		s.handleSNSListSubscriptionsByTopicJSON(w, r, params)
	case protocol.ActionConfirmSubscription:
		s.handleSNSConfirmSubscriptionJSON(w, r, params)
	default:
		writeSNSJSONFatalError(w, "UnsupportedOperation",
			fmt.Sprintf("Action SNS %q não implementada", action))
	}
}

// --- SNS Query handlers ---

func (s *Server) handleSNSCreateTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.CreateTopic(r.Context(), sns.CreateTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type result struct {
		XMLName  xml.Name `xml:"CreateTopicResult"`
		TopicArn string   `xml:"TopicArn"`
	}
	type response struct {
		XMLName  xml.Name `xml:"CreateTopicResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result:   result{TopicArn: res.TopicARN},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSGetTopicAttributesQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.GetTopicAttributes(r.Context(), sns.GetTopicAttributesParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	// SNS GetTopicAttributes retorna uma lista de member pairs (Name, Value).
	type member struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
	}
	type result struct {
		XMLName xml.Name `xml:"GetTopicAttributesResult"`
		Members []member `xml:"member"`
	}
	type response struct {
		XMLName  xml.Name `xml:"GetTopicAttributesResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	// Ordena chaves para output determinístico.
	keys := make([]string, 0, len(res.Attributes))
	for k := range res.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	members := make([]member, 0, len(keys))
	for _, k := range keys {
		members = append(members, member{Name: k, Value: res.Attributes[k]})
	}

	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result:   result{Members: members},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSListTopicsQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListTopics(r.Context(), sns.ListTopicsParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type topicMember struct {
		XMLName  xml.Name `xml:"member"`
		TopicArn string   `xml:"TopicArn"`
	}
	type result struct {
		XMLName   xml.Name      `xml:"ListTopicsResult"`
		Members   []topicMember `xml:"Topics>member"`
		NextToken string        `xml:"NextToken,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:"ListTopicsResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	members := make([]topicMember, 0, len(res.Topics))
	for _, t := range res.Topics {
		arn := protocol.NewSNSARN(s.handlers.SNS.Region(), s.handlers.SNS.AccountID(), t.Name).String()
		members = append(members, topicMember{TopicArn: arn})
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns: "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result: result{
			Members:   members,
			NextToken: res.NextToken,
		},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSDeleteTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SNS.DeleteTopic(r.Context(), sns.DeleteTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"DeleteTopicResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSSubscribeQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.Subscribe(r.Context(), sns.SubscribeParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type result struct {
		XMLName         xml.Name `xml:"SubscribeResult"`
		SubscriptionArn string   `xml:"SubscriptionArn"`
	}
	type response struct {
		XMLName  xml.Name `xml:"SubscribeResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result:   result{SubscriptionArn: res.SubscriptionARN},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSUnsubscribeQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SNS.Unsubscribe(r.Context(), sns.UnsubscribeParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type response struct {
		XMLName  xml.Name `xml:"UnsubscribeResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSPublishQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.Publish(r.Context(), sns.PublishParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type result struct {
		XMLName    xml.Name `xml:"PublishResult"`
		MessageId  string   `xml:"MessageId,omitempty"`
		SequenceNo string   `xml:"SequenceNumber,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:"PublishResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result:   result{MessageId: res.MessageID, SequenceNo: res.SequenceNo},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

func (s *Server) handleSNSListSubscriptionsQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListSubscriptions(r.Context(), sns.ListSubscriptionsParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	s.writeSubscriptionsXML(w, res.Subscriptions, res.NextToken, "ListSubscriptionsResponse", "ListSubscriptionsResult")
}

func (s *Server) handleSNSListSubscriptionsByTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListSubscriptionsByTopic(r.Context(), sns.ListSubscriptionsByTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	s.writeSubscriptionsXML(w, res.Subscriptions, res.NextToken, "ListSubscriptionsByTopicResponse", "ListSubscriptionsByTopicResult")
}

func (s *Server) handleSNSConfirmSubscriptionQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ConfirmSubscription(r.Context(), sns.ConfirmSubscriptionParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSFatalError(w, awsErr.Code, awsErr.Message, newRequestID())
		return
	}
	type result struct {
		XMLName         xml.Name `xml:"ConfirmSubscriptionResult"`
		SubscriptionArn string   `xml:"SubscriptionArn"`
	}
	type response struct {
		XMLName  xml.Name `xml:"ConfirmSubscriptionResponse"`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		Xmlns:    "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result:   result{SubscriptionArn: res.SubscriptionARN},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// writeSubscriptionsXML serializa ListSubscriptions / ListSubscriptionsByTopic response.
//
// responseTag é o nome do envelope externo (e.g. "ListSubscriptionsResponse").
// resultTag é o nome do elemento de resultado interno (e.g. "ListSubscriptionsResult"
// ou "ListSubscriptionsByTopicResult").
func (s *Server) writeSubscriptionsXML(w http.ResponseWriter, subs []types.Subscription, nextToken, responseTag, resultTag string) {
	type subMember struct {
		XMLName         xml.Name `xml:"member"`
		TopicArn        string   `xml:"TopicArn"`
		Protocol        string   `xml:"Protocol"`
		SubscriptionArn string   `xml:"SubscriptionArn"`
		Endpoint        string   `xml:"Endpoint"`
		Owner           string   `xml:"Owner"`
	}
	type result struct {
		XMLName   xml.Name    `xml:""`
		Members   []subMember `xml:"Subscriptions>member"`
		NextToken string      `xml:"NextToken,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:""`
		Xmlns    string   `xml:"xmlns,attr"`
		Result   result
		Metadata protocol.ResponseMetadata
	}
	members := make([]subMember, 0, len(subs))
	for _, sub := range subs {
		members = append(members, subMember{
			TopicArn:        sub.TopicARN,
			Protocol:        sub.Protocol,
			SubscriptionArn: sub.ARN,
			Endpoint:        sub.Endpoint,
			Owner:           s.handlers.SNS.AccountID(),
		})
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	resp := response{
		XMLName: xml.Name{Local: responseTag},
		Xmlns:   "http://sns.amazonaws.com/doc/" + protocol.SNSProtocolVersion,
		Result: result{
			XMLName:   xml.Name{Local: resultTag},
			Members:   members,
			NextToken: nextToken,
		},
		Metadata: protocol.ResponseMetadata{RequestID: newRequestID()},
	}
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	_ = enc.Encode(resp)
	_ = enc.Flush()
}

// --- SNS JSON handlers ---

func (s *Server) handleSNSCreateTopicJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.CreateTopic(r.Context(), sns.CreateTopicParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"TopicArn": res.TopicARN})
}

func (s *Server) handleSNSGetTopicAttributesJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.GetTopicAttributes(r.Context(), sns.GetTopicAttributesParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	type attr struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	out := make([]attr, 0, len(res.Attributes))
	keys := make([]string, 0, len(res.Attributes))
	for k := range res.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, attr{Key: k, Value: res.Attributes[k]})
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"Attributes": out})
}

func (s *Server) handleSNSListTopicsJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ListTopics(r.Context(), sns.ListTopicsParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	type topicOut struct {
		TopicArn string `json:"TopicArn"`
	}
	topics := make([]topicOut, 0, len(res.Topics))
	for _, t := range res.Topics {
		arn := protocol.NewSNSARN(s.handlers.SNS.Region(), s.handlers.SNS.AccountID(), t.Name).String()
		topics = append(topics, topicOut{TopicArn: arn})
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	out := map[string]any{"Topics": topics}
	if res.NextToken != "" {
		out["NextToken"] = res.NextToken
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleSNSDeleteTopicJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SNS.DeleteTopic(r.Context(), sns.DeleteTopicParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

func (s *Server) handleSNSSubscribeJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.Subscribe(r.Context(), sns.SubscribeParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"SubscriptionArn": res.SubscriptionARN})
}

func (s *Server) handleSNSUnsubscribeJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	err := s.handlers.SNS.Unsubscribe(r.Context(), sns.UnsubscribeParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

func (s *Server) handleSNSPublishJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.Publish(r.Context(), sns.PublishParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	out := map[string]string{"MessageId": res.MessageID}
	if res.SequenceNo != "" {
		out["SequenceNumber"] = res.SequenceNo
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) handleSNSListSubscriptionsJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ListSubscriptions(r.Context(), sns.ListSubscriptionsParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	s.writeSubscriptionsJSON(w, res.Subscriptions, res.NextToken)
}

func (s *Server) handleSNSListSubscriptionsByTopicJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ListSubscriptionsByTopic(r.Context(), sns.ListSubscriptionsByTopicParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	s.writeSubscriptionsJSON(w, res.Subscriptions, res.NextToken)
}

func (s *Server) handleSNSConfirmSubscriptionJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ConfirmSubscription(r.Context(), sns.ConfirmSubscriptionParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeSNSJSONFatalError(w, awsErr.Code, awsErr.Message)
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"SubscriptionArn": res.SubscriptionARN})
}

// writeSubscriptionsJSON serializa ListSubscriptions/ListSubscriptionsByTopic response JSON.
func (s *Server) writeSubscriptionsJSON(w http.ResponseWriter, subs []types.Subscription, nextToken string) {
	type subOut struct {
		TopicArn        string `json:"TopicArn"`
		Protocol        string `json:"Protocol"`
		SubscriptionArn string `json:"SubscriptionArn"`
		Endpoint        string `json:"Endpoint"`
		Owner           string `json:"Owner"`
	}
	out := make([]subOut, 0, len(subs))
	for _, sub := range subs {
		out = append(out, subOut{
			TopicArn:        sub.TopicARN,
			Protocol:        sub.Protocol,
			SubscriptionArn: sub.ARN,
			Endpoint:        sub.Endpoint,
			Owner:           s.handlers.SNS.AccountID(),
		})
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	resp := map[string]any{"Subscriptions": out}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// --- SNS error helpers ---

func writeSNSFatalError(w http.ResponseWriter, code, message, requestID string) {
	awsErr := &sns.AWSError{Code: code, Message: message}
	w.Header().Set("Content-Type", "text/xml")
	status := http.StatusInternalServerError
	if awsErr.IsSenderFault() {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	_ = protocol.EncodeSNSQueryError(w, code, message, requestID, awsErr.IsSenderFault())
}

func writeSNSJSONFatalError(w http.ResponseWriter, code, message string) {
	awsErr := &sns.AWSError{Code: code, Message: message}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	status := http.StatusInternalServerError
	if awsErr.IsSenderFault() {
		status = http.StatusBadRequest
	}
	w.WriteHeader(status)
	_ = protocol.EncodeSNSJSONError(w, code, message)
}

// ensure unused imports compile in older Go versions
var _ = errors.New
