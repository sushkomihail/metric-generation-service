package server

import (
	"net/http"

	"github.com/sushkomihail/metric-generation-service/internal/broker/kafka"
	httpgenerator "github.com/sushkomihail/metric-generation-service/internal/generators/http"
	testhendlers "github.com/sushkomihail/metric-generation-service/internal/handlers"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
	"github.com/sushkomihail/metric-generation-service/internal/middleware"
)

var handlers = map[string]func(http.ResponseWriter, *http.Request){
	httpgenerator.FirstEndpoint:  testhendlers.FirstEndpointHandler,
	httpgenerator.SecondEndpoint: testhendlers.SecondEndpointHandler,
	httpgenerator.ThirdEndpoint:  testhendlers.ThirdEndpointHandler,
}

func Listen(addr string, producer *kafka.Producer, log *logger.Logger) error {
	mux := http.NewServeMux()
	registerHandlers(mux)
	return http.ListenAndServe(addr, middleware.NewHttpMetricMiddleware(producer, log).Handler(mux))
}

func registerHandlers(mux *http.ServeMux) {
	for endpoint, handler := range handlers {
		mux.HandleFunc(endpoint, handler)
	}
}
