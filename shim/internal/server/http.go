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
	"net/http"
	"net/url"
	"sort"
	"strings"
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
	w.Write([]byte(xml.Header))
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
	protocol.EncodeSQSJSONResponse(w, map[string]string{
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
	w.Write([]byte(xml.Header))
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
	protocol.EncodeSQSJSONResponse(w, map[string]any{"Messages": out})
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
	w.Write([]byte(xml.Header))
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
	w.Write([]byte(`{}`))
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
	w.Write([]byte(xml.Header))
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
	w.Write([]byte(`{}`))
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
	w.Write([]byte(xml.Header))
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
	w.Write([]byte(`{}`))
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
	w.Write([]byte(xml.Header))
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
	w.Write([]byte(`{}`))
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

	w.Write([]byte(xml.Header))
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

	w.Write([]byte(xml.Header))
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
