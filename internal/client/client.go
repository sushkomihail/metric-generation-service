package client

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
	pb "github.com/sushkomihail/metric-aggregation-service/api/proto/generated/metrics"
	"github.com/sushkomihail/metric-generation-service/internal/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type GrpcClient struct {
	client pb.MetricsServiceClient
	conn   *grpc.ClientConn
	log    *logger.Logger
}

func New(addr string, log *logger.Logger) (*GrpcClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %w", err)
	}

	client := pb.NewMetricsServiceClient(conn)

	return &GrpcClient{
		client: client,
		conn:   conn,
		log:    log,
	}, nil
}

func (c *GrpcClient) SendMetric(ctx context.Context, req *pb.MetricRequest) (*pb.MetricResponse, error) {
	traceId := uuid.New().String()

	md := metadata.Pairs("trace-id", traceId)
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := c.client.SendMetric(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("unary metric send failed: %w", err)
	}

	return resp, nil
}

func (c *GrpcClient) StreamMetrics(ctx context.Context, metrics []*pb.MetricRequest) (*pb.StreamResponse, error) {
	stream, err := c.client.StreamMetrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	for i, metric := range metrics {
		traceId := uuid.New().String()

		if err = stream.Send(metric); err != nil {
			c.log.Error("Failed to send metric in stream", "trace_id", traceId, "error", err)
			return nil, fmt.Errorf("stream send failed at metric %d: %w", i+1, err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("stream close failed: %w", err)
	}

	if resp == nil {
		c.log.Info("Response is nil")
		return nil, nil
	}

	return resp, nil
}

func (c *GrpcClient) Close() error {
	return c.conn.Close()
}
