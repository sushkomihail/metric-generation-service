package config

import (
	"os"
	"strconv"
)

const (
	defaultHttpGeneratorWorkersNumber = 10
	defaultHttpGenerationsNumber      = 1000000

	defaultGrpcGeneratorWorkersNumber  = 10
	defaultGrpcUnaryGenerationsNumber  = 1000000
	defaultGrpcStreamGenerationsNumber = 1000000
	defaultGrpcStreamBatchSize         = 100
)

type Config struct {
	HttpAddr            string
	GrpcAddr            string
	HttpGeneratorConfig HttpGeneratorConfig
	GrpcGeneratorConfig GrpcGeneratorConfig
	KafkaConfig         KafkaConfig
	LogLevel            string
}

type KafkaConfig struct {
	Servers string
}

type HttpGeneratorConfig struct {
	WorkersNumber     int
	GenerationsNumber int
}

type GrpcGeneratorConfig struct {
	WorkersNumber           int
	UnaryGenerationsNumber  int
	StreamGenerationsNumber int
	StreamBatchSize         int
}

func (c *Config) Load() {
	c.HttpAddr = os.Getenv("HTTP_ADDR")
	c.GrpcAddr = os.Getenv("GRPC_ADDR")
	c.loadHttpGeneratorConfig()
	c.loadGrpcGeneratorConfig()
	c.loadKafkaConfig()
	c.LogLevel = os.Getenv("LOG_LEVEL")
}

func (c *Config) loadHttpGeneratorConfig() {
	workersNumber, err := strconv.Atoi(os.Getenv("HTTP_GENERATOR_WORKERS_NUMBER"))
	if err != nil {
		workersNumber = defaultHttpGeneratorWorkersNumber
	}

	generationsNumber, err := strconv.Atoi(os.Getenv("HTTP_GENERATIONS_NUMBER"))
	if err != nil {
		generationsNumber = defaultHttpGenerationsNumber
	}

	c.HttpGeneratorConfig.WorkersNumber = workersNumber
	c.HttpGeneratorConfig.GenerationsNumber = generationsNumber
}

func (c *Config) loadGrpcGeneratorConfig() {
	workersNumber, err := strconv.Atoi(os.Getenv("GRPC_GENERATOR_WORKERS_NUMBER"))
	if err != nil {
		workersNumber = defaultGrpcGeneratorWorkersNumber
	}

	unaryGenerationsNumber, err := strconv.Atoi(os.Getenv("GRPC_GENERATOR_UNARY_GENERATIONS_NUMBER"))
	if err != nil {
		unaryGenerationsNumber = defaultGrpcUnaryGenerationsNumber
	}

	streamGenerationsNumber, err := strconv.Atoi(os.Getenv("GRPC_GENERATOR_STREAM_GENERATIONS_NUMBER"))
	if err != nil {
		streamGenerationsNumber = defaultGrpcStreamGenerationsNumber
	}

	streamBatchSize, err := strconv.Atoi(os.Getenv("GRPC_GENERATOR_STREAM_BATCH_SIZE"))
	if err != nil {
		streamBatchSize = defaultGrpcStreamBatchSize
	}

	c.GrpcGeneratorConfig.WorkersNumber = workersNumber
	c.GrpcGeneratorConfig.UnaryGenerationsNumber = unaryGenerationsNumber
	c.GrpcGeneratorConfig.StreamGenerationsNumber = streamGenerationsNumber
	c.GrpcGeneratorConfig.StreamBatchSize = streamBatchSize
}

func (c *Config) loadKafkaConfig() {
	c.KafkaConfig.Servers = os.Getenv("KAFKA_SERVERS")
}
