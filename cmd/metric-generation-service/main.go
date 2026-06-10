package main

import (
	"fmt"

	"github.com/joho/godotenv"
	"github.com/sushkomihail/metric-generation-service/internal/broker/kafka"
	"github.com/sushkomihail/metric-generation-service/internal/config"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
	"github.com/sushkomihail/metric-generation-service/internal/server"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic(fmt.Sprintf("error loading .env file: %v", err))
	}

	var cfg config.Config
	cfg.Load()

	log := logger.New(cfg.LogLevel)
	producer := kafka.NewProducer(cfg.KafkaConfig, log)
	defer func() {
		err := producer.Close()
		if err != nil {
			panic(err)
		}
	}()

	if err := server.Listen(cfg.HttpAddr, producer, log); err != nil {
		log.Error("Error starting http server", "error", err)
	}
}
