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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// Azure backend constants
	azureContainerName = "backuptest-azurite"
	azureImage         = "mcr.microsoft.com/azure-storage/azurite"
	azureBlobPort      = "10000/tcp"
	azureQueuePort     = "10001/tcp"
	azureTablePort     = "10002/tcp"

	// Azurite well-known development credentials
	azureAccountName = "devstoreaccount1"
	azureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// AzureBackend implements BackupBackend for Azure Blob Storage using Azurite.
type AzureBackend struct {
	*BaseBackend
	container     testcontainers.Container
	network       *testcontainers.DockerNetwork
	endpoint      string
	azureClient   *azblob.Client
	containerList map[string]bool // Azure calls buckets "containers"
}

// NewAzureBackend creates a new AzureBackend with Azurite.
func NewAzureBackend(opts ...BackendOption) *AzureBackend {
	return &AzureBackend{
		BaseBackend:   NewBaseBackend(opts...),
		containerList: make(map[string]bool),
	}
}

// Compile-time check that AzureBackend implements BackupBackend.
var _ BackupBackend = (*AzureBackend)(nil)

// Name returns "azure".
func (b *AzureBackend) Name() string {
	return "azure"
}

// Start provisions Azurite and creates the default container.
func (b *AzureBackend) Start(ctx context.Context) error {
	if err := b.CheckNotStarted(); err != nil {
		return err
	}
	b.SetState(StateStarting)

	// Create network if not provided
	if b.Config().networkName == "" {
		net, err := network.New(ctx,
			network.WithCheckDuplicate(),
			network.WithDriver("bridge"),
		)
		if err != nil {
			b.SetState(StateNotStarted)
			return fmt.Errorf("failed to create network: %w", err)
		}
		b.network = net
		b.Config().networkName = net.Name
	}

	// Start Azurite container
	blobPort := nat.Port(azureBlobPort)
	req := testcontainers.ContainerRequest{
		Image:        azureImage,
		ExposedPorts: []string{azureBlobPort, azureQueuePort, azureTablePort},
		Name:         azureContainerName,
		Hostname:     azureContainerName,
		AutoRemove:   true,
		Networks:     []string{b.Config().networkName},
		NetworkAliases: map[string][]string{
			b.Config().networkName: {azureContainerName},
		},
		Cmd: []string{"azurite", "--blobHost", "0.0.0.0", "--queueHost", "0.0.0.0", "--tableHost", "0.0.0.0", "--skipApiVersionCheck"},
		WaitingFor: wait.ForAll(
			wait.ForLog("Azurite Blob service is successfully listening at http://0.0.0.0:10000"),
			wait.ForLog("Azurite Queue service is successfully listening at http://0.0.0.0:10001"),
			wait.ForLog("Azurite Table service is successfully listening at http://0.0.0.0:10002"),
			wait.ForListeningPort(blobPort),
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
		return fmt.Errorf("failed to start Azurite container: %w", err)
	}
	b.container = container

	// Get endpoint
	endpoint, err := container.PortEndpoint(ctx, blobPort, "")
	if err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to get Azurite endpoint: %w", err)
	}
	b.endpoint = endpoint

	// Create Azure client
	if err := b.createAzureClient(ctx); err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to create Azure client: %w", err)
	}

	// Create default container (Azure's term for bucket)
	if err := b.CreateBucketInternal(ctx, b.BucketName()); err != nil {
		b.cleanup(ctx)
		b.SetState(StateNotStarted)
		return fmt.Errorf("failed to create default container: %w", err)
	}

	b.SetState(StateRunning)
	return nil
}

// Stop terminates the Azurite container and cleans up resources.
func (b *AzureBackend) Stop(ctx context.Context) error {
	if b.State() == StateStopped {
		return nil
	}
	b.SetState(StateStopping)
	b.cleanup(ctx)
	b.SetState(StateStopped)
	return nil
}

// cleanup releases all resources.
func (b *AzureBackend) cleanup(ctx context.Context) {
	b.azureClient = nil
	if b.container != nil {
		_ = b.container.Terminate(ctx)
		b.container = nil
	}
	if b.network != nil {
		_ = b.network.Remove(ctx)
		b.network = nil
	}
	b.endpoint = ""
	b.containerList = make(map[string]bool)
}

// GetWeaviateEnv returns environment variables for Weaviate Azure backup configuration.
func (b *AzureBackend) GetWeaviateEnv() map[string]string {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	// Build connection string for internal Docker network access
	internalEndpoint := fmt.Sprintf("%s:10000", azureContainerName)
	connectionString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s/%s;",
		azureAccountName, azureAccountKey, internalEndpoint, azureAccountName,
	)
	return map[string]string{
		"AZURE_STORAGE_CONNECTION_STRING": connectionString,
		"BACKUP_AZURE_CONTAINER":          b.BucketName(),
	}
}

// Endpoint returns the Azurite blob HTTP endpoint for external access.
func (b *AzureBackend) Endpoint() string {
	if err := b.CheckRunning(); err != nil {
		return ""
	}
	return b.endpoint
}

// CreateBucket creates a new Azure container (Azure's term for bucket).
func (b *AzureBackend) CreateBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}
	if b.containerList[name] {
		return ErrBucketExists
	}
	return b.CreateBucketInternal(ctx, name)
}

// CreateBucketInternal creates a container without state checks (used during Start).
func (b *AzureBackend) CreateBucketInternal(ctx context.Context, name string) error {
	// Check if container already exists in our tracking
	if b.containerList[name] {
		if b.State() == StateRunning {
			return ErrBucketExists
		}
		return nil
	}

	// Create the container
	_, err := b.azureClient.CreateContainer(ctx, name, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerAlreadyExists" {
			// Container already exists (from previous run with container reuse)
			b.containerList[name] = true
			return nil
		}
		return fmt.Errorf("failed to create container %s: %w", name, err)
	}

	b.containerList[name] = true
	return nil
}

// createAzureClient initializes the Azure Blob client for Azurite.
func (b *AzureBackend) createAzureClient(ctx context.Context) error {
	connectionString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s/%s;",
		azureAccountName, azureAccountKey, b.endpoint, azureAccountName,
	)

	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure client: %w", err)
	}

	// Verify connection by listing containers
	pager := client.NewListContainersPager(nil)
	_, err = pager.NextPage(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Azurite: %w", err)
	}

	b.azureClient = client
	return nil
}

// AzureClient returns the underlying Azure Blob client for advanced operations.
// Returns nil if the backend is not running.
func (b *AzureBackend) AzureClient() *azblob.Client {
	if err := b.CheckRunning(); err != nil {
		return nil
	}
	return b.azureClient
}

// NetworkName returns the Docker network name used by this backend.
func (b *AzureBackend) NetworkName() string {
	return b.Config().networkName
}

// ContainerName returns the Azurite container name (Docker container, not Azure container).
func (b *AzureBackend) ContainerName() string {
	return azureContainerName
}

// InternalEndpoint returns the endpoint for use within the Docker network.
func (b *AzureBackend) InternalEndpoint() string {
	return fmt.Sprintf("%s:10000", azureContainerName)
}

// ListBuckets returns all Azure containers (buckets) in Azurite.
func (b *AzureBackend) ListBuckets(ctx context.Context) ([]string, error) {
	if err := b.CheckRunning(); err != nil {
		return nil, err
	}

	var containers []string
	pager := b.azureClient.NewListContainersPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list containers: %w", err)
		}
		for _, container := range page.ContainerItems {
			if container.Name != nil {
				containers = append(containers, *container.Name)
			}
		}
	}
	return containers, nil
}

// DeleteBucket removes an Azure container from Azurite.
func (b *AzureBackend) DeleteBucket(ctx context.Context, name string) error {
	if err := b.CheckRunning(); err != nil {
		return err
	}

	_, err := b.azureClient.DeleteContainer(ctx, name, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerNotFound" {
			// Already deleted
			delete(b.containerList, name)
			return nil
		}
		return fmt.Errorf("failed to delete container %s: %w", name, err)
	}

	delete(b.containerList, name)
	return nil
}

// ConnectionString returns the Azure connection string for external access.
func (b *AzureBackend) ConnectionString() string {
	if err := b.CheckRunning(); err != nil {
		return ""
	}
	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s/%s;",
		azureAccountName, azureAccountKey, b.endpoint, azureAccountName,
	)
}
