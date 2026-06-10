package main

import (
	"context"
	"fmt"

	"github.com/joho/godotenv"
	grpcclient "github.com/sushkomihail/metric-generation-service/internal/client"
	"github.com/sushkomihail/metric-generation-service/internal/config"
	grpcgenerator "github.com/sushkomihail/metric-generation-service/internal/generators/grpc"
	httpgenerator "github.com/sushkomihail/metric-generation-service/internal/generators/http"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic(fmt.Sprintf("error loading .env file: %v", err))
	}

	var cfg config.Config
	cfg.Load()

	log := logger.New(cfg.LogLevel)

	client, err := grpcclient.New(cfg.GrpcAddr, log)
	if err != nil {
		panic(err)
	}

	defer func() {
		err = client.Close()
		if err != nil {
			panic(err)
		}
	}()

	httpGenerator := httpgenerator.New(cfg.HttpGeneratorConfig, cfg.HttpAddr, log)
	httpGenerator.Start()

	grpcGenerator := grpcgenerator.New(cfg.GrpcGeneratorConfig, client, log)
	grpcGenerator.Start(context.Background())
}
