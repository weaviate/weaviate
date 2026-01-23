//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backuptest

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// S3 backend constants
	s3ContainerName = "backuptest-minio"
	s3Image         = "minio/minio"
	s3Port          = "9000/tcp"
	s3AccessKey     = "minioadmin"
	s3SecretKey     = "minioadmin"
	s3Region        = "us-east-1"
)

// S3Backend implements BackupBackend for S3-compatible storage using MinIO.
type S3Backend struct {
	*BaseBackend
	container testcontainers.Container
	network   *testcontainers.DockerNetwork
	endpoint  string
	s3Client  *s3.Client
	region    string
}

// S3Option configures the S3Backend.
type S3Option func(*S3Backend)

// WithS3Region sets the AWS region for the S3 backend.
func WithS3Region(region string) S3Option {
	return func(b *S3Backend) {
		b.region = region
	}
}

// NewS3Backend creates a new S3Backend with MinIO.
func NewS3Backend(opts ...BackendOption) *S3Backend {
	return &S3Backend{
		BaseBackend: NewBaseBackend(opts...),
		region:      s3Region,
	}
}

// NewS3BackendWithOptions creates a new S3Backend with both base and S3-specific options.
func NewS3BackendWithOptions(baseOpts []BackendOption, s3Opts ...S3Option) *S3Backend {
	backend := NewS3Backend(baseOpts...)
	for _, opt := range s3Opts {
		opt(backend)
	}
	return backend
}

// Compile-time check that S3Backend implements BackupBackend.
var _ BackupBackend = (*S3Backend)(nil)

// Name returns "s3".
func (b *S3Backend) Name() string {
	return "s3"
}

// Start provisions MinIO and creates the default bucket.
func (b *S3Backend) Start(ctx context.Context) error {
	if err := b.CheckNotStarted(); err != nil {
		return err
	}
	b.SetState(StateStarting)

	// Create network if not provided
	if b.Config().networkName == "" {
		net, err := network.New(ctx,
			network.WithDriver("bridge"),
		)
		if err != nil {
			b.SetState(StateNotStarted)
			return fmt.Errorf("failed to create network: %w", err)
		}
		b.network = net
		b.Config().networkName = net.Name
	}

	// Start MinIO container
	port := nat.Port(s3Port)
	req := testcontainers.ContainerRequest{
		Image:        s3Image,
		ExposedPorts: []string{s3Port},
		Name:         s3ContainerName,
		Hostname:     s3ContainerName,
		AutoRemove:   true,
		Networks:     []string{b.Config().networkName},
		NetworkAliases: map[string][]string{
			b.Config().networkName: {s3ContainerName},
		},
		Env: map[string]string{
			"MINIO_ROOT_USER":     s3AccessKey,
			"MINIO_ROOT_PASSWORD": s3SecretKey,
		},
		Cmd: []string{"server", "/data"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(port),
			wait.ForHTTP("/minio/health/ready").WithPort(port),
		).WithDeadline(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            b.Config().containerReuse,
	})
	if err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to start MinIO container: %w", err)
	}
	b.container = container

	// Get endpoint
	endpoint, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to get MinIO endpoint: %w", err)
	}
	b.endpoint = endpoint

	// Create S3 client
	if err := b.createS3Client(ctx); err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create default bucket
	if err := b.CreateBucketInternal(ctx, b.BucketName()); err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to create default bucket: %w", err)
	}

	b.SetState(StateRunning)
	return nil
}

// Stop terminates the MinIO container and cleans up resources.
func (b *S3Backend) Stop(ctx context.Context) error {
	if b.State() == StateStopped {
		return nil
	}
	b.SetState(StateStopping)
	b.cleanup(ctx)
	b.SetState(StateStopped)
	return nil
}

// cleanup releases all resources.
func (b *S3Backend) cleanup(ctx context.Context) {
	if b.container != nil {
		_ = b.container.Terminate(ctx)
		b.container = nil
	}
	if b.network != nil {
		_ = b.network.Remove(ctx)
		b.network = nil
	}
	b.s3Client = nil
	b.endpoint = ""
}

// GetWeaviateEnv returns environment variables for Weaviate S3 backup configuration.
func (b *S3Backend) GetWeaviateEnv() map[string]string {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	return map[string]string{
		"AWS_ACCESS_KEY_ID":   s3AccessKey,
		"AWS_SECRET_KEY":      s3SecretKey,
		"AWS_REGION":          b.region,
		"BACKUP_S3_BUCKET":    b.BucketName(),
		"BACKUP_S3_ENDPOINT":  fmt.Sprintf("%s:%s", s3ContainerName, "9000"),
		"BACKUP_S3_USE_SSL":   "false",
		"OFFLOAD_S3_ENDPOINT": fmt.Sprintf("http://%s:%s", s3ContainerName, "9000"),
	}
}

// Endpoint returns the MinIO HTTP endpoint for external access.
func (b *S3Backend) Endpoint() string {
	if err := b.CheckRunning(); err != nil {
		return ""
	}
	return b.endpoint
}

// CreateBucket creates a new S3 bucket.
func (b *S3Backend) CreateBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}
	return b.CreateBucketInternal(ctx, name)
}

// CreateBucketInternal creates a bucket without state checks (used during Start).
func (b *S3Backend) CreateBucketInternal(ctx context.Context, name string) error {
	// Check if bucket already exists
	_, err := b.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(name),
	})
	if err == nil {
		// Bucket exists - this is fine during Start, but error for CreateBucket
		if b.State() == StateRunning {
			return ErrBucketExists
		}
		return nil
	}

	// Create the bucket
	_, err = b.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", name, err)
	}

	// Wait for bucket to be ready
	waiter := s3.NewBucketExistsWaiter(b.s3Client)
	if err := waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(name),
	}, 30*time.Second); err != nil {
		return fmt.Errorf("bucket %s not ready: %w", name, err)
	}

	return nil
}

// createS3Client initializes the AWS S3 client for MinIO.
func (b *S3Backend) createS3Client(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(b.region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s3AccessKey,
			s3SecretKey,
			"",
		)),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	b.s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", b.endpoint))
		o.UsePathStyle = true // Required for MinIO
	})

	return nil
}

// S3Client returns the underlying S3 client for advanced operations.
// Returns nil if the backend is not running.
func (b *S3Backend) S3Client() *s3.Client {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	return b.s3Client
}

// NetworkName returns the Docker network name used by this backend.
func (b *S3Backend) NetworkName() string {
	return b.Config().networkName
}

// ContainerName returns the MinIO container name.
func (b *S3Backend) ContainerName() string {
	return s3ContainerName
}

// InternalEndpoint returns the endpoint for use within the Docker network.
func (b *S3Backend) InternalEndpoint() string {
	return fmt.Sprintf("%s:9000", s3ContainerName)
}

// Region returns the configured AWS region.
func (b *S3Backend) Region() string {
	return b.region
}

// ListBuckets returns all buckets in MinIO.
func (b *S3Backend) ListBuckets(ctx context.Context) ([]string, error) {
	if err := b.CheckRunning(); err != nil {
		return nil, err
	}

	output, err := b.s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}

	buckets := make([]string, len(output.Buckets))
	for i, bucket := range output.Buckets {
		buckets[i] = *bucket.Name
	}
	return buckets, nil
}

// DeleteBucket removes a bucket from MinIO.
func (b *S3Backend) DeleteBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}

	_, err := b.s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket %s: %w", name, err)
	}
	return nil
}
