package observability

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// metricsGroup contém todos os coletores Prometheus do shim. Inicializado
// uma vez via SetupMetrics e depois consultado diretamente.
type metricsGroup struct {
	// HTTP
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	inflightRequests *prometheus.GaugeVec

	// Storage (NATS)
	storageOpsTotal   *prometheus.CounterVec
	storageOpDuration *prometheus.HistogramVec

	// Auth
	authEvaluationsTotal *prometheus.CounterVec

	// SQS
	longpollActive   *prometheus.GaugeVec
	longpollDuration *prometheus.HistogramVec
	queueDepth       *prometheus.GaugeVec

	// SNS
	snsPublishTotal   *prometheus.CounterVec
	snsFanoutDuration *prometheus.HistogramVec

	registry *prometheus.Registry
}

var (
	mg     *metricsGroup
	mgOnce sync.Once
)

// SetupMetrics inicializa todos os coletores e os registra em um registry
// dedicado (não usa o default global, para evitar conflitos em testes).
//
// Retorna o registry pronto para ser exposto via promhttp.HandlerFor.
func SetupMetrics() *prometheus.Registry {
	mgOnce.Do(func() {
		mg = &metricsGroup{
			registry: prometheus.NewRegistry(),
		}

		mg.requestsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "shim",
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total de requests HTTP processadas pelo shim.",
			},
			[]string{"service", "action", "status"},
		)
		mg.requestDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "shim",
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "Latência de requests HTTP em segundos.",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~16s
			},
			[]string{"service", "action"},
		)
		mg.inflightRequests = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "shim",
				Subsystem: "http",
				Name:      "inflight_requests",
				Help:      "Número de requests HTTP em andamento.",
			},
			[]string{"service"},
		)

		mg.storageOpsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "shim",
				Subsystem: "storage",
				Name:      "ops_total",
				Help:      "Total de operações executadas no storage (broker).",
			},
			[]string{"op", "status"},
		)
		mg.storageOpDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "shim",
				Subsystem: "storage",
				Name:      "op_duration_seconds",
				Help:      "Latência de operações no storage.",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
			},
			[]string{"op"},
		)

		mg.authEvaluationsTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "shim",
				Subsystem: "auth",
				Name:      "evaluations_total",
				Help:      "Total de avaliações IAM/SigV4.",
			},
			[]string{"result"}, // allow|deny|error
		)

		mg.longpollActive = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "shim",
				Subsystem: "sqs",
				Name:      "longpoll_active_connections",
				Help:      "Conexões em long-polling ativas no momento.",
			},
			[]string{"queue"},
		)
		mg.longpollDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "shim",
				Subsystem: "sqs",
				Name:      "longpoll_duration_seconds",
				Help:      "Duração real de chamadas ReceiveMessage (inclui espera).",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14),
			},
			[]string{"queue"},
		)
		mg.queueDepth = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "shim",
				Subsystem: "sqs",
				Name:      "queue_depth",
				Help:      "Profundidade atual da fila (mensagens disponíveis).",
			},
			[]string{"queue"},
		)

		mg.snsPublishTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "shim",
				Subsystem: "sns",
				Name:      "publish_total",
				Help:      "Total de mensagens publicadas em tópicos SNS.",
			},
			[]string{"topic", "status"},
		)
		mg.snsFanoutDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "shim",
				Subsystem: "sns",
				Name:      "fanout_duration_seconds",
				Help:      "Latência do fan-out SNS para subscribers.",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14),
			},
			[]string{"topic"},
		)

		// Coletores runtime padrão do Go.
		mg.registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)

		// Coletores do shim.
		mg.registry.MustRegister(
			mg.requestsTotal,
			mg.requestDuration,
			mg.inflightRequests,
			mg.storageOpsTotal,
			mg.storageOpDuration,
			mg.authEvaluationsTotal,
			mg.longpollActive,
			mg.longpollDuration,
			mg.queueDepth,
			mg.snsPublishTotal,
			mg.snsFanoutDuration,
		)

		// Touch cada métrica para garantir que apareça em /metrics mesmo
		// antes de qualquer tráfego (importante para alertas Prometheus).
		mg.requestsTotal.WithLabelValues("sqs", "_init", "200").Add(0)
		mg.requestDuration.WithLabelValues("sqs", "_init").Observe(0)
		mg.inflightRequests.WithLabelValues("sqs").Set(0)
		mg.storageOpsTotal.WithLabelValues("_init", "ok").Add(0)
		mg.storageOpDuration.WithLabelValues("_init").Observe(0)
		mg.authEvaluationsTotal.WithLabelValues("_init").Add(0)
		mg.longpollActive.WithLabelValues("_init").Set(0)
		mg.longpollDuration.WithLabelValues("_init").Observe(0)
		mg.queueDepth.WithLabelValues("_init").Set(0)
		mg.snsPublishTotal.WithLabelValues("_init", "ok").Add(0)
		mg.snsFanoutDuration.WithLabelValues("_init").Observe(0)
	})
	return mg.registry
}

// Registry retorna o registry de métricas. Panica se não inicializado.
func Registry() *prometheus.Registry {
	if mg == nil {
		// Fallback para uso em testes antes de SetupMetrics.
		return SetupMetrics()
	}
	return mg.registry
}

// --- Helpers para instrumentação ---

// ObserveHTTP registra uma request HTTP completa.
// service: "sqs" | "sns"; action: nome da operação (CreateQueue, SendMessage, ...).
// status: código HTTP como string ("200", "400", "403", "500").
func ObserveHTTP(service, action, status string, duration time.Duration) {
	if mg == nil {
		return
	}
	mg.requestsTotal.WithLabelValues(service, action, status).Inc()
	mg.requestDuration.WithLabelValues(service, action).Observe(duration.Seconds())
}

// IncInflight incrementa contador de requests em andamento.
func IncInflight(service string) {
	if mg == nil {
		return
	}
	mg.inflightRequests.WithLabelValues(service).Inc()
}

// DecInflight decrementa contador de requests em andamento.
func DecInflight(service string) {
	if mg == nil {
		return
	}
	mg.inflightRequests.WithLabelValues(service).Dec()
}

// ObserveStorage registra uma operação de storage.
func ObserveStorage(op string, err error, duration time.Duration) {
	if mg == nil {
		return
	}
	status := "ok"
	if err != nil {
		status = "error"
	}
	mg.storageOpsTotal.WithLabelValues(op, status).Inc()
	mg.storageOpDuration.WithLabelValues(op).Observe(duration.Seconds())
}

// ObserveAuth registra uma avaliação IAM/SigV4.
func ObserveAuth(result string) {
	if mg == nil {
		return
	}
	mg.authEvaluationsTotal.WithLabelValues(result).Inc()
}

// SetQueueDepth atualiza gauge de profundidade de fila.
func SetQueueDepth(queue string, depth float64) {
	if mg == nil {
		return
	}
	mg.queueDepth.WithLabelValues(queue).Set(depth)
}

// IncLongPoll registra uma conexão em long-polling aberta.
func IncLongPoll(queue string) {
	if mg == nil {
		return
	}
	mg.longpollActive.WithLabelValues(queue).Inc()
}

// DecLongPoll registra uma conexão em long-polling fechada.
func DecLongPoll(queue string) {
	if mg == nil {
		return
	}
	mg.longpollActive.WithLabelValues(queue).Dec()
}

// ObserveLongPollDuration registra duração total de uma chamada ReceiveMessage.
func ObserveLongPollDuration(queue string, d time.Duration) {
	if mg == nil {
		return
	}
	mg.longpollDuration.WithLabelValues(queue).Observe(d.Seconds())
}

// ObserveSNSPublish registra um Publish SNS.
func ObserveSNSPublish(topic, status string, duration time.Duration) {
	if mg == nil {
		return
	}
	mg.snsPublishTotal.WithLabelValues(topic, status).Inc()
	mg.snsFanoutDuration.WithLabelValues(topic).Observe(duration.Seconds())
}

// StatusFromHTTP converte http.ResponseWriter status em string.
// Helper para middleware.
func StatusFromHTTP(code int) string {
	return strconv.Itoa(code)
}

// HTTPStatusRecorder é um ResponseWriter que captura o status code
// para que middleware possa emitir métrica após handler executar.
type HTTPStatusRecorder struct {
	http.ResponseWriter
	Status int
	Bytes  int
}

// NewStatusRecorder embrulha um ResponseWriter.
func NewStatusRecorder(w http.ResponseWriter) *HTTPStatusRecorder {
	return &HTTPStatusRecorder{ResponseWriter: w, Status: http.StatusOK}
}

// WriteHeader captura status code.
func (r *HTTPStatusRecorder) WriteHeader(code int) {
	r.Status = code
	r.ResponseWriter.WriteHeader(code)
}

// Write captura bytes escritos.
func (r *HTTPStatusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.Bytes += n
	return n, err
}
