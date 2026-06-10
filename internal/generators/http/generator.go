package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sushkomihail/metric-generation-service/internal/config"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
)

const (
	FirstEndpoint  = "/first_endpoint"
	SecondEndpoint = "/second_endpoint"
	ThirdEndpoint  = "/third_endpoint"
)

var endpoints = []string{
	FirstEndpoint,
	SecondEndpoint,
	ThirdEndpoint,
}

var methods = []string{
	http.MethodGet,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
}

type result struct {
	sent    int
	success int
	failed  int
	latency time.Duration
	mu      sync.Mutex
}

func (r *result) record(latency time.Duration, success bool) {
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

func (r *result) print(workersNumber int) {
	fmt.Println("Sent: ", r.sent)
	fmt.Println("Success: ", r.success)
	fmt.Println("Failed: ", r.failed)
	fmt.Println("Latency: ", r.latency)
	avgLatency := r.latency / time.Duration(workersNumber)
	fmt.Println("Avg Latency (per worker): ", avgLatency)
	fmt.Println("Rate (requests/s): ", float64(r.success)/avgLatency.Seconds())
}

type Generator struct {
	config config.HttpGeneratorConfig
	addr   string
	client *http.Client
	result result
	log    *logger.Logger
}

func New(config config.HttpGeneratorConfig, addr string, log *logger.Logger) *Generator {
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return &Generator{
		config: config,
		addr:   addr,
		client: client,
		log:    log,
	}
}

func (g *Generator) Start() {
	var wg sync.WaitGroup
	perWorker := g.config.GenerationsNumber / g.config.WorkersNumber
	remainder := g.config.GenerationsNumber % g.config.WorkersNumber

	for i := 0; i < g.config.WorkersNumber; i++ {
		metricsCount := perWorker
		if i < remainder {
			metricsCount++
		}

		if metricsCount > 0 {
			wg.Add(1)

			go func(workerId int, metricsCount int) {
				defer wg.Done()

				pcg := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(workerId))
				r := rand.New(pcg)

				for range metricsCount {
					method := getRandomMethod(r)
					endpoint := getRandomEndpoint(r)
					body, err := generateBody(r)
					if err != nil {
						g.log.Error("Error generating request body", "error", err)
						return
					}

					g.sendRequest(method, endpoint, body)
				}
			}(i, metricsCount)
		}
	}

	wg.Wait()
	fmt.Println("HTTP metrics generation results:")
	g.result.print(g.config.WorkersNumber)
}

func (g *Generator) sendRequest(method, endpoint string, body []byte) {
	url := fmt.Sprintf("http://%s%s", g.addr, endpoint)
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		g.log.Error("Error creating http request", "error", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Trace-ID", uuid.New().String())

	start := time.Now()
	resp, err := g.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		g.log.Error("Error executing http request", "error", err)
		g.result.record(latency, false)
		return
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	success := resp.StatusCode >= 200 && resp.StatusCode < 300
	g.result.record(latency, success)
}

func generateBody(r *rand.Rand) ([]byte, error) {
	fieldsNumber := r.IntN(20) + 1
	fields := make([]string, fieldsNumber)
	for i := 0; i < fieldsNumber; i++ {
		length := r.IntN(50) + 10
		fields[i] = getRandomString(r, length)
	}
	return json.Marshal(fields)
}

func getRandomString(r *rand.Rand, length int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	str := make([]rune, length)
	for i := range str {
		str[i] = letters[r.IntN(len(letters))]
	}
	return string(str)
}

func getRandomMethod(r *rand.Rand) string {
	return methods[r.IntN(len(methods))]
}

func getRandomEndpoint(r *rand.Rand) string {
	return endpoints[r.IntN(len(endpoints))]
}
