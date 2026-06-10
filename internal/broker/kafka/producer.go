package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sushkomihail/metric-aggregation-service/pkg/metrics"
	"github.com/sushkomihail/metric-aggregation-service/pkg/models"
	"github.com/sushkomihail/metric-generation-service/internal/config"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
)

const (
	HttpTopic = "http-topic"
)

type Producer struct {
	writer     *kafka.Writer
	log        *logger.Logger
	metricChan chan *models.HttpMetric
	stopChan   chan struct{}
}

func NewProducer(config config.KafkaConfig, log *logger.Logger) *Producer {
	brokers := strings.Split(config.Servers, ",")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		Topic:        HttpTopic,
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
		Async:        false,
	}

	p := &Producer{
		writer:     writer,
		log:        log,
		metricChan: make(chan *models.HttpMetric, 10000),
		stopChan:   make(chan struct{}),
	}

	for i := 0; i < 15; i++ {
		go p.startWorker()
	}

	return p
}

func (p *Producer) EnqueueMetric(metric *models.HttpMetric) {
	select {
	case p.metricChan <- metric:
	default:
		go func() {
			p.metricChan <- metric
		}()
	}
}

func (p *Producer) startWorker() {
	ctx := context.Background()

	for {
		select {
		case <-p.stopChan:
			return
		case metric, ok := <-p.metricChan:
			if !ok {
				return
			}

			if err := p.produce(ctx, metric); err != nil {
				p.log.Error("Worker failed to send metric to kafka", "trace_id", metric.TraceId, "error", err)
			}
		}
	}
}

func (p *Producer) produce(ctx context.Context, metric *models.HttpMetric) error {
	jsonData, err := json.Marshal(metric)
	if err != nil {
		return fmt.Errorf("failed to marshal metric: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(metric.TraceId),
		Value: jsonData,
	}

	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err = p.writer.WriteMessages(writeCtx, kafkaMsg); err != nil {
		return fmt.Errorf("failed to write messages: %w", err)
	}

	metrics.IncMetricsProduced()
	return nil
}

func (p *Producer) Close() error {
	close(p.stopChan)
	close(p.metricChan)
	return p.writer.Close()
}
