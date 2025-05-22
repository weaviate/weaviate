package grpc

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/weaviate/weaviate/adapters/handlers/indices/grpc/proto"
)

var (
	// Metrics for BatchPutObjects
	batchPutObjectsDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "indices_batch_put_objects_duration_seconds",
			Help:    "Duration of BatchPutObjects operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"shard"},
	)

	batchPutObjectsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "indices_batch_put_objects_total",
			Help: "Total number of BatchPutObjects operations",
		},
		[]string{"shard", "status"},
	)

	batchPutObjectsBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "indices_batch_put_objects_batch_size",
			Help:    "Size of batches in BatchPutObjects operations",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000},
		},
		[]string{"shard"},
	)
)

// IndicesClient is a client for the IndicesService
type IndicesClient struct {
	client proto.IndicesServiceClient
	conn   *grpc.ClientConn
}

// ClientOption is a function that configures the IndicesClient
type ClientOption func(*clientConfig)

type clientConfig struct {
	dialTimeout     time.Duration
	keepAliveTime   time.Duration
	keepAlivePeriod time.Duration
}

// WithDialTimeout sets the dial timeout for the client
func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.dialTimeout = timeout
	}
}

// WithKeepAlive sets the keepalive parameters for the client
func WithKeepAlive(time, period time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.keepAliveTime = time
		c.keepAlivePeriod = period
	}
}

// NewIndicesClient creates a new IndicesClient connected to the given address
func NewIndicesClient(address string, opts ...ClientOption) (*IndicesClient, error) {
	config := &clientConfig{
		dialTimeout:     5 * time.Second,
		keepAliveTime:   30 * time.Second,
		keepAlivePeriod: 10 * time.Second,
	}

	for _, opt := range opts {
		opt(config)
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                config.keepAliveTime,
			Timeout:             config.dialTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewIndicesServiceClient(conn)
	return &IndicesClient{
		client: client,
		conn:   conn,
	}, nil
}

// Close closes the client connection
func (c *IndicesClient) Close() error {
	return c.conn.Close()
}

// Object operations
func (c *IndicesClient) PutObject(ctx context.Context, req *proto.PutObjectRequest) (*proto.PutObjectResponse, error) {
	return c.client.PutObject(ctx, req)
}

// BatchPutObjects puts multiple objects into the index and records metrics
func (c *IndicesClient) BatchPutObjects(ctx context.Context, req *proto.BatchPutObjectsRequest) (*proto.BatchPutObjectsResponse, error) {
	start := time.Now()

	// Record batch size
	batchSize := float64(len(req.Objects))
	batchPutObjectsBatchSize.WithLabelValues(req.Shard).Observe(batchSize)

	// Execute the request
	resp, err := c.client.BatchPutObjects(ctx, req)

	// Record duration
	duration := time.Since(start).Seconds()
	batchPutObjectsDuration.WithLabelValues(req.Shard).Observe(duration)

	// Record success/failure
	status := "success"
	if err != nil {
		status = "error"
	}
	batchPutObjectsTotal.WithLabelValues(req.Shard, status).Inc()

	return resp, err
}

func (c *IndicesClient) GetObject(ctx context.Context, req *proto.GetObjectRequest) (*proto.GetObjectResponse, error) {
	return c.client.GetObject(ctx, req)
}

func (c *IndicesClient) DeleteObject(ctx context.Context, req *proto.DeleteObjectRequest) (*proto.DeleteObjectResponse, error) {
	return c.client.DeleteObject(ctx, req)
}

func (c *IndicesClient) BatchDeleteObjects(ctx context.Context, req *proto.BatchDeleteObjectsRequest) (*proto.BatchDeleteObjectsResponse, error) {
	return c.client.BatchDeleteObjects(ctx, req)
}

func (c *IndicesClient) MergeObject(ctx context.Context, req *proto.MergeObjectRequest) (*proto.MergeObjectResponse, error) {
	return c.client.MergeObject(ctx, req)
}

func (c *IndicesClient) MultiGetObjects(ctx context.Context, req *proto.MultiGetObjectsRequest) (*proto.MultiGetObjectsResponse, error) {
	return c.client.MultiGetObjects(ctx, req)
}

// Search operations
func (c *IndicesClient) SearchShard(ctx context.Context, req *proto.SearchShardRequest) (*proto.SearchShardResponse, error) {
	return c.client.SearchShard(ctx, req)
}

func (c *IndicesClient) Aggregate(ctx context.Context, req *proto.AggregateRequest) (*proto.AggregateResponse, error) {
	return c.client.Aggregate(ctx, req)
}

// Shard operations
func (c *IndicesClient) GetShardQueueSize(ctx context.Context, req *proto.GetShardQueueSizeRequest) (*proto.GetShardQueueSizeResponse, error) {
	return c.client.GetShardQueueSize(ctx, req)
}

func (c *IndicesClient) GetShardStatus(ctx context.Context, req *proto.GetShardStatusRequest) (*proto.GetShardStatusResponse, error) {
	return c.client.GetShardStatus(ctx, req)
}

func (c *IndicesClient) UpdateShardStatus(ctx context.Context, req *proto.UpdateShardStatusRequest) (*proto.UpdateShardStatusResponse, error) {
	return c.client.UpdateShardStatus(ctx, req)
}

func (c *IndicesClient) CreateShard(ctx context.Context, req *proto.CreateShardRequest) (*proto.CreateShardResponse, error) {
	return c.client.CreateShard(ctx, req)
}

func (c *IndicesClient) ReInitShard(ctx context.Context, req *proto.ReInitShardRequest) (*proto.ReInitShardResponse, error) {
	return c.client.ReInitShard(ctx, req)
}
