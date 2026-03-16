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
	"errors"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// GCS backend constants
	gcsContainerName = "backuptest-gcs"
	gcsImage         = "oittaa/gcp-storage-emulator"
	gcsPort          = "9090/tcp"
	gcsProjectID     = "test-project"
)

// GCSBackend implements BackupBackend for Google Cloud Storage using the GCS emulator.
type GCSBackend struct {
	*BaseBackend
	container  testcontainers.Container
	network    *testcontainers.DockerNetwork
	endpoint   string
	gcsClient  *storage.Client
	projectID  string
	bucketList map[string]bool
}

// GCSOption configures the GCSBackend.
type GCSOption func(*GCSBackend)

// WithGCSProjectID sets the GCP project ID.
func WithGCSProjectID(projectID string) GCSOption {
	return func(b *GCSBackend) {
		b.projectID = projectID
	}
}

// NewGCSBackend creates a new GCSBackend with the GCS emulator.
func NewGCSBackend(opts ...BackendOption) *GCSBackend {
	return &GCSBackend{
		BaseBackend: NewBaseBackend(opts...),
		projectID:   gcsProjectID,
		bucketList:  make(map[string]bool),
	}
}

// NewGCSBackendWithOptions creates a new GCSBackend with both base and GCS-specific options.
func NewGCSBackendWithOptions(baseOpts []BackendOption, gcsOpts ...GCSOption) *GCSBackend {
	backend := NewGCSBackend(baseOpts...)
	for _, opt := range gcsOpts {
		opt(backend)
	}
	return backend
}

// Compile-time check that GCSBackend implements BackupBackend.
var _ BackupBackend = (*GCSBackend)(nil)

// Name returns "gcs".
func (b *GCSBackend) Name() string {
	return "gcs"
}

// Start provisions the GCS emulator and creates the default bucket.
func (b *GCSBackend) Start(ctx context.Context) error {
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

	// Start GCS emulator container
	port := nat.Port(gcsPort)
	req := testcontainers.ContainerRequest{
		Image:        gcsImage,
		ExposedPorts: []string{gcsPort},
		Name:         gcsContainerName,
		Hostname:     gcsContainerName,
		AutoRemove:   true,
		Networks:     []string{b.Config().networkName},
		NetworkAliases: map[string][]string{
			b.Config().networkName: {gcsContainerName},
		},
		Env: map[string]string{
			"PORT": "9090",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(port),
			wait.ForHTTP("/").WithPort(port),
		).WithStartupTimeoutDefault(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            b.Config().containerReuse,
	})
	if err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to start GCS emulator container: %w", err)
	}
	b.container = container

	// Get endpoint
	endpoint, err := container.PortEndpoint(ctx, port, "")
	if err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to get GCS emulator endpoint: %w", err)
	}
	b.endpoint = endpoint

	// Create GCS client
	if err := b.createGCSClient(ctx); err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to create GCS client: %w", err)
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

// Stop terminates the GCS emulator container and cleans up resources.
func (b *GCSBackend) Stop(ctx context.Context) error {
	if b.State() == StateStopped {
		return nil
	}
	b.SetState(StateStopping)
	b.cleanup(ctx)
	b.SetState(StateStopped)
	return nil
}

// cleanup releases all resources.
func (b *GCSBackend) cleanup(ctx context.Context) {
	if b.gcsClient != nil {
		_ = b.gcsClient.Close()
		b.gcsClient = nil
	}
	if b.container != nil {
		_ = b.container.Terminate(ctx)
		b.container = nil
	}
	if b.network != nil {
		_ = b.network.Remove(ctx)
		b.network = nil
	}
	b.endpoint = ""
	b.bucketList = make(map[string]bool)
}

// GetWeaviateEnv returns environment variables for Weaviate GCS backup configuration.
func (b *GCSBackend) GetWeaviateEnv() map[string]string {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	return map[string]string{
		"GOOGLE_CLOUD_PROJECT":           b.projectID,
		"STORAGE_EMULATOR_HOST":          fmt.Sprintf("http://%s:%s", gcsContainerName, "9090"),
		"BACKUP_GCS_BUCKET":              b.BucketName(),
		"BACKUP_GCS_USE_AUTH":            "false",
		"GOOGLE_APPLICATION_CREDENTIALS": "", // Empty for emulator
	}
}

// Endpoint returns the GCS emulator HTTP endpoint for external access.
func (b *GCSBackend) Endpoint() string {
	if err := b.CheckRunning(); err != nil {
		return ""
	}
	return b.endpoint
}

// CreateBucket creates a new GCS bucket.
func (b *GCSBackend) CreateBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}
	if b.bucketList[name] {
		return ErrBucketExists
	}
	return b.CreateBucketInternal(ctx, name)
}

// CreateBucketInternal creates a bucket without state checks (used during Start).
func (b *GCSBackend) CreateBucketInternal(ctx context.Context, name string) error {
	// Check if bucket already exists in our tracking
	if b.bucketList[name] {
		if b.State() == StateRunning {
			return ErrBucketExists
		}
		return nil
	}

	// Create the bucket
	bucket := b.gcsClient.Bucket(name)
	if err := bucket.Create(ctx, b.projectID, nil); err != nil {
		// Check if it already exists (from a previous run with container reuse)
		if b.bucketExists(ctx, name) {
			b.bucketList[name] = true
			return nil
		}
		return fmt.Errorf("failed to create bucket %s: %w", name, err)
	}

	b.bucketList[name] = true
	return nil
}

// bucketExists checks if a bucket exists.
func (b *GCSBackend) bucketExists(ctx context.Context, name string) bool {
	bucket := b.gcsClient.Bucket(name)
	_, err := bucket.Attrs(ctx)
	return err == nil
}

// createGCSClient initializes the GCS client for the emulator.
func (b *GCSBackend) createGCSClient(ctx context.Context) error {
	// The GCS emulator is accessed via the STORAGE_EMULATOR_HOST env var.
	// We set it temporarily for client creation, then the client will use it.
	os.Setenv("STORAGE_EMULATOR_HOST", b.endpoint)

	client, err := storage.NewClient(ctx,
		option.WithoutAuthentication(),
	)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	b.gcsClient = client
	return nil
}

// GCSClient returns the underlying GCS client for advanced operations.
// Returns nil if the backend is not running.
func (b *GCSBackend) GCSClient() *storage.Client {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	return b.gcsClient
}

// NetworkName returns the Docker network name used by this backend.
func (b *GCSBackend) NetworkName() string {
	return b.Config().networkName
}

// ContainerName returns the GCS emulator container name.
func (b *GCSBackend) ContainerName() string {
	return gcsContainerName
}

// InternalEndpoint returns the endpoint for use within the Docker network.
func (b *GCSBackend) InternalEndpoint() string {
	return fmt.Sprintf("%s:9090", gcsContainerName)
}

// ProjectID returns the configured GCP project ID.
func (b *GCSBackend) ProjectID() string {
	return b.projectID
}

// ListBuckets returns all buckets in the GCS emulator.
func (b *GCSBackend) ListBuckets(ctx context.Context) ([]string, error) {
	if err := b.CheckRunning(); err != nil {
		return nil, err
	}

	var buckets []string
	it := b.gcsClient.Buckets(ctx, b.projectID)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list buckets: %w", err)
		}
		buckets = append(buckets, attrs.Name)
	}
	return buckets, nil
}

// DeleteBucket removes a bucket from the GCS emulator.
// The bucket must be empty.
func (b *GCSBackend) DeleteBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}

	bucket := b.gcsClient.Bucket(name)

	// First, delete all objects in the bucket
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects in bucket %s: %w", name, err)
		}
		if err := bucket.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", objAttrs.Name, err)
		}
	}

	// Delete the bucket
	if err := bucket.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete bucket %s: %w", name, err)
	}

	delete(b.bucketList, name)
	return nil
}
