// Handlers SNS (Query + JSON).
//
// Pré-refactor (refactor/kiss-dry-pass-2 Commit 1): este código
// vivia em http.go (1763 linhas, inavegável). Agora cada handler
// SNS está aqui, fácil de encontrar e modificar. Mesmo package
// server, então tem acesso a writeFatalError, newRequestID,
// Server struct, etc.

package server

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/equake/dropin-queue/shim/internal/protocol"
	"github.com/equake/dropin-queue/shim/internal/sns"
	"github.com/equake/dropin-queue/shim/pkg/types"
)

// handleSNSQuery trata requests SNS no protocolo Query (form-encoded + XML).
func (s *Server) handleSNSQuery(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSNSQueryRequest(r)
	if err != nil {
		writeFatalError(w, transportSNSQuery, "InvalidParameterValue", err.Error(), requestFromContext(r))
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
		writeFatalError(w, transportSNSQuery, "UnsupportedOperation",
			fmt.Sprintf("Action SNS %q não implementada", action), requestFromContext(r))
	}
}

// handleSNSJSON trata requests SNS no protocolo JSON 1.0.
func (s *Server) handleSNSJSON(w http.ResponseWriter, r *http.Request) {
	action, params, err := protocol.ParseSNSJSONRequest(r)
	if err != nil {
		writeFatalError(w, transportSNSJSON, "InvalidParameterValue", err.Error(), requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, "UnsupportedOperation",
			fmt.Sprintf("Action SNS %q não implementada", action), requestFromContext(r))
	}
}

// --- SNS Query handlers ---

func (s *Server) handleSNSCreateTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.CreateTopic(r.Context(), sns.CreateTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionCreateTopic),
		struct {
			XMLName  xml.Name `xml:"CreateTopicResult"`
			TopicArn string   `xml:"TopicArn"`
		}{TopicArn: res.TopicARN},
		requestFromContext(r))
}

func (s *Server) handleSNSGetTopicAttributesQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.GetTopicAttributes(r.Context(), sns.GetTopicAttributesParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	// SNS GetTopicAttributes retorna uma lista de member pairs (Name, Value).
	type member struct {
		Name  string `xml:"Name"`
		Value string `xml:"Value"`
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

	respondSNSQueryXML(w, string(protocol.ActionGetTopicAttributes),
		struct {
			XMLName xml.Name `xml:"GetTopicAttributesResult"`
			Members []member `xml:"member"`
		}{Members: members},
		requestFromContext(r))
}

func (s *Server) handleSNSListTopicsQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListTopics(r.Context(), sns.ListTopicsParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	type topicMember struct {
		XMLName  xml.Name `xml:"member"`
		TopicArn string   `xml:"TopicArn"`
	}
	members := make([]topicMember, 0, len(res.Topics))
	for _, t := range res.Topics {
		arn := protocol.NewSNSARN(s.handlers.SNS.Region(), s.handlers.SNS.AccountID(), t.Name).String()
		members = append(members, topicMember{TopicArn: arn})
	}

	respondSNSQueryXML(w, string(protocol.ActionListTopics),
		struct {
			XMLName   xml.Name      `xml:"ListTopicsResult"`
			Members   []topicMember `xml:"Topics>member"`
			NextToken string        `xml:"NextToken,omitempty"`
		}{
			Members:   members,
			NextToken: res.NextToken,
		},
		requestFromContext(r))
}

func (s *Server) handleSNSDeleteTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SNS.DeleteTopic(r.Context(), sns.DeleteTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionDeleteTopic), nil, requestFromContext(r))
}

func (s *Server) handleSNSSubscribeQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.Subscribe(r.Context(), sns.SubscribeParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionSubscribe),
		struct {
			XMLName         xml.Name `xml:"SubscribeResult"`
			SubscriptionArn string   `xml:"SubscriptionArn"`
		}{SubscriptionArn: res.SubscriptionARN},
		requestFromContext(r))
}

func (s *Server) handleSNSUnsubscribeQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	err := s.handlers.SNS.Unsubscribe(r.Context(), sns.UnsubscribeParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionUnsubscribe), nil, requestFromContext(r))
}

func (s *Server) handleSNSPublishQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.Publish(r.Context(), sns.PublishParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionPublish),
		struct {
			XMLName    xml.Name `xml:"PublishResult"`
			MessageId  string   `xml:"MessageId,omitempty"`
			SequenceNo string   `xml:"SequenceNumber,omitempty"`
		}{MessageId: res.MessageID, SequenceNo: res.SequenceNo},
		requestFromContext(r))
}

func (s *Server) handleSNSListSubscriptionsQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListSubscriptions(r.Context(), sns.ListSubscriptionsParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	s.writeSubscriptionsXML(w, r, res.Subscriptions, res.NextToken,
		string(protocol.ActionListSubscriptions), "ListSubscriptionsResult")
}

func (s *Server) handleSNSListSubscriptionsByTopicQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ListSubscriptionsByTopic(r.Context(), sns.ListSubscriptionsByTopicParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	s.writeSubscriptionsXML(w, r, res.Subscriptions, res.NextToken,
		string(protocol.ActionListSubscriptionsByTopic), "ListSubscriptionsByTopicResult")
}

func (s *Server) handleSNSConfirmSubscriptionQuery(w http.ResponseWriter, r *http.Request, params url.Values) {
	res, err := s.handlers.SNS.ConfirmSubscription(r.Context(), sns.ConfirmSubscriptionParamsFromQuery(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSQuery, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	respondSNSQueryXML(w, string(protocol.ActionConfirmSubscription),
		struct {
			XMLName         xml.Name `xml:"ConfirmSubscriptionResult"`
			SubscriptionArn string   `xml:"SubscriptionArn"`
		}{SubscriptionArn: res.SubscriptionARN},
		requestFromContext(r))
}

// writeSubscriptionsXML serializa ListSubscriptions / ListSubscriptionsByTopic response.
//
// action é o nome da action AWS (e.g. "ListSubscriptions") — respondSNSQueryXML
// monta o envelope externo como "<action>Response". resultTag é o nome do
// elemento de resultado interno (e.g. "ListSubscriptionsResult" ou
// "ListSubscriptionsByTopicResult").
func (s *Server) writeSubscriptionsXML(w http.ResponseWriter, r *http.Request, subs []types.Subscription, nextToken, action, resultTag string) {
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
	respondSNSQueryXML(w, action,
		result{
			XMLName:   xml.Name{Local: resultTag},
			Members:   members,
			NextToken: nextToken,
		},
		requestFromContext(r))
}

// --- SNS JSON handlers ---

func (s *Server) handleSNSCreateTopicJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.CreateTopic(r.Context(), sns.CreateTopicParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
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
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	s.writeSubscriptionsJSON(w, r, res.Subscriptions, res.NextToken)
}

func (s *Server) handleSNSListSubscriptionsByTopicJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ListSubscriptionsByTopic(r.Context(), sns.ListSubscriptionsByTopicParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	s.writeSubscriptionsJSON(w, r, res.Subscriptions, res.NextToken)
}

func (s *Server) handleSNSConfirmSubscriptionJSON(w http.ResponseWriter, r *http.Request, params map[string]any) {
	res, err := s.handlers.SNS.ConfirmSubscription(r.Context(), sns.ConfirmSubscriptionParamsFromJSON(params))
	if err != nil {
		awsErr := sns.AsAWSError(err)
		writeFatalError(w, transportSNSJSON, awsErr.Code, awsErr.Message, requestFromContext(r))
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"SubscriptionArn": res.SubscriptionARN})
}

// writeSubscriptionsJSON serializa ListSubscriptions/ListSubscriptionsByTopic response JSON.
func (s *Server) writeSubscriptionsJSON(w http.ResponseWriter, r *http.Request, subs []types.Subscription, nextToken string) {
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

// --- SNS error helpers foram para response.go (writeFatalError).
//
// ensure unused imports compile in older Go versions
var _ = errors.New
