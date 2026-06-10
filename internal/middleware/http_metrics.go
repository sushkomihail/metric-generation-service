package middleware

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/sushkomihail/metric-aggregation-service/pkg/models"
	"github.com/sushkomihail/metric-generation-service/internal/broker/kafka"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
)

type responseWriter struct {
	http.ResponseWriter
	status      int
	size        int64
	body        *bytes.Buffer
	wroteHeader bool
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{
		ResponseWriter: w,
		status:         http.StatusOK,
		body:           &bytes.Buffer{},
	}
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}

	rw.status = code
	rw.wroteHeader = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}

	size, err := rw.ResponseWriter.Write(b)
	rw.size += int64(size)
	rw.body.Write(b)
	return size, err
}

type HttpMetricMiddleware struct {
	producer *kafka.Producer
	log      *logger.Logger
}

func NewHttpMetricMiddleware(producer *kafka.Producer, log *logger.Logger) *HttpMetricMiddleware {
	return &HttpMetricMiddleware{
		producer: producer,
		log:      log,
	}
}

func (m *HttpMetricMiddleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		traceId := r.Header.Get("Trace-ID")
		if traceId == "" {
			traceId = uuid.New().String()
		}

		w.Header().Set("Trace-ID", traceId)

		var requestBody []byte
		if r.Body != nil {
			var err error
			requestBody, err = io.ReadAll(r.Body)
			if err != nil {
				m.log.Warn("Failed to read request body", "trace_id", traceId, "error", err)
				requestBody = []byte{}
			}
			r.Body = io.NopCloser(bytes.NewBuffer(requestBody))
		}

		wrapped := newResponseWriter(w)
		next.ServeHTTP(wrapped, r)

		duration := time.Since(startTime)
		metric := models.HttpMetric{
			TraceId:      traceId,
			Method:       r.Method,
			Endpoint:     r.URL.Path,
			Code:         wrapped.status,
			Duration:     duration,
			RequestSize:  int64(len(requestBody)),
			ResponseSize: wrapped.size,
			Timestamp:    startTime,
		}

		m.producer.EnqueueMetric(&metric)
	})
}
