package grpc

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	pb "github.com/sushkomihail/metric-aggregation-service/api/proto/generated/metrics"
	"github.com/sushkomihail/metric-generation-service/internal/client"
	"github.com/sushkomihail/metric-generation-service/internal/config"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type unaryResult struct {
	mu      sync.Mutex
	sent    int
	success int
	failed  int
	latency time.Duration
}

func (r *unaryResult) record(latency time.Duration, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.sent++
	r.latency += latency
	if success {
		r.success++
	} else {
		r.failed++
	}
}

func (r *unaryResult) print(workersNumber int) {
	fmt.Println("Sent: ", r.sent)
	fmt.Println("Success: ", r.success)
	fmt.Println("Failed: ", r.failed)
	fmt.Println("Latency: ", r.latency)
	avgLatency := r.latency / time.Duration(workersNumber)
	fmt.Println("Avg latency (per worker): ", avgLatency)
	fmt.Println("Rate (requests/s): ", float64(r.success)/avgLatency.Seconds())
}

type streamResult struct {
	mu           sync.Mutex
	sent         int
	batches      int
	success      int
	failed       int
	batchLatency time.Duration
}

func (r *streamResult) record(metricsCount int, latency time.Duration, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.batches++
	r.sent += metricsCount
	r.batchLatency += latency
	if success {
		r.success += metricsCount
	} else {
		r.failed += metricsCount
	}
}

func (r *streamResult) print(workersNumber int) {
	fmt.Println("Sent: ", r.sent)
	fmt.Println("Batches: ", r.batches)
	fmt.Println("Success: ", r.success)
	fmt.Println("Failed: ", r.failed)
	fmt.Println("Batch latency: ", r.batchLatency)
	avgLatency := r.batchLatency / time.Duration(workersNumber)
	fmt.Println("Avg latency (per worker): ", avgLatency)
	fmt.Println("Rate (requests/s): ", float64(r.success)/avgLatency.Seconds())
}

var metrics = map[string]pb.MetricType{
	"metric_1": pb.MetricType_COUNTER,
	"metric_2": pb.MetricType_HISTOGRAM,
	"metric_3": pb.MetricType_GAUGE,
}

type Generator struct {
	config       config.GrpcGeneratorConfig
	client       *client.GrpcClient
	unaryResult  unaryResult
	streamResult streamResult
	log          *logger.Logger
}

func New(config config.GrpcGeneratorConfig, client *client.GrpcClient, log *logger.Logger) *Generator {
	return &Generator{
		config: config,
		client: client,
		log:    log,
	}
}

func (g *Generator) Start(ctx context.Context) {
	var wg sync.WaitGroup
	startGeneration(ctx, &wg, g.config.UnaryGenerationsNumber, g.config.WorkersNumber, g.startUnaryWorker)
	startGeneration(ctx, &wg, g.config.StreamGenerationsNumber, g.config.WorkersNumber, g.startStreamWorker)
	wg.Wait()
	fmt.Println("Metrics unary generation results:")
	g.unaryResult.print(g.config.WorkersNumber)
	fmt.Println("Metrics stream generation results:")
	g.streamResult.print(g.config.WorkersNumber)
}

func startGeneration(
	ctx context.Context,
	wg *sync.WaitGroup,
	generationsNumber,
	workersNumber int,
	worker func(context.Context, *sync.WaitGroup, int),
) {
	if generationsNumber > 0 {
		perWorker := generationsNumber / workersNumber
		remainder := generationsNumber % workersNumber

		for i := 0; i < workersNumber; i++ {
			metricsCount := perWorker
			if i < remainder {
				metricsCount++
			}
			if metricsCount > 0 {
				wg.Add(1)
				go worker(ctx, wg, metricsCount)
			}
		}
	}
}

func (g *Generator) startUnaryWorker(ctx context.Context, wg *sync.WaitGroup, metricsNumber int) {
	defer wg.Done()

	for i := 0; i < metricsNumber; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			metric := generateMetric()

			start := time.Now()
			resp, err := g.client.SendMetric(ctx, metric)
			latency := time.Since(start)
			g.unaryResult.record(latency, err == nil && resp.Success)

			if err != nil {
				g.log.Error("Failed to send metric", "error", err)
				continue
			}

			if !resp.Success {
				g.log.Error("Failed to send metric", "error", resp.Message)
				continue
			}
		}
	}
}

func (g *Generator) startStreamWorker(ctx context.Context, wg *sync.WaitGroup, metricsNumber int) {
	defer wg.Done()

	sent := 0
	for sent < metricsNumber {
		select {
		case <-ctx.Done():
			return
		default:
			batchSize := g.config.StreamBatchSize
			if sent+batchSize > metricsNumber {
				batchSize = metricsNumber - sent
			}

			metrics := generateStreamMetrics(batchSize)

			start := time.Now()
			resp, err := g.client.StreamMetrics(ctx, metrics)
			latency := time.Since(start)
			success := err == nil && resp.FailedCount == 0
			g.streamResult.record(len(metrics), latency, success)

			if err != nil {
				g.log.Error("Failed to stream metrics", "error", err)
				continue
			}

			if resp.FailedCount > 0 {
				for _, errMessage := range resp.Errors {
					g.log.Error("Failed to stream metrics", "error", errMessage)
				}
				continue
			}

			sent += batchSize
		}
	}
}

func generateMetric() *pb.MetricRequest {
	metricName, metricType := getRandomNameType()
	return &pb.MetricRequest{
		Name:      metricName,
		Type:      metricType,
		Value:     getRandomValue(metricType),
		Tags:      generateTags(),
		Timestamp: timestamppb.Now(),
	}
}

func generateStreamMetrics(batchSize int) []*pb.MetricRequest {
	metrics := make([]*pb.MetricRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		metrics[i] = generateMetric()
	}

	return metrics
}

func getRandomNameType() (string, pb.MetricType) {
	var metricName string
	var metricType pb.MetricType

	for k, v := range metrics {
		metricName = k
		metricType = v
		break
	}

	return metricName, metricType
}

func generateTags() map[string]string {
	fieldsNumber := rand.Intn(20)
	tags := make(map[string]string)

	for i := 0; i < fieldsNumber; i++ {
		key := fmt.Sprintf("tag_%d", i)
		valueLength := rand.Intn(30)
		tags[key] = getRandomString(valueLength)
	}

	return tags
}

func getRandomValue(metricType pb.MetricType) float64 {
	switch metricType {
	case pb.MetricType_COUNTER:
		return float64(rand.Intn(1000000))
	case pb.MetricType_GAUGE:
		return rand.Float64() * 100
	case pb.MetricType_HISTOGRAM:
		return rand.ExpFloat64() * 100
	default:
		return rand.Float64() * 100
	}
}

func getRandomString(length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	str := make([]rune, length)
	for i := range str {
		str[i] = letters[rand.Intn(len(letters))]
	}

	return string(str)
}
