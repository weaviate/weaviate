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

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	// Environment variable names for sharing cluster endpoints
	envSharedWeaviateEndpoint = "SHARED_WEAVIATE_ENDPOINT"
	envSharedAzuriteEndpoint  = "SHARED_AZURITE_ENDPOINT"

	// Azure/Azurite configuration
	azureContainerName = "backups"
	// Azurite default connection string template
	connectionStringTemplate = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s/devstoreaccount1;"
)

// sharedCompose holds the Docker compose instance for all tests in this package.
// This is set up once in TestMain and shared across all tests.
var sharedCompose *docker.DockerCompose

// TestMain sets up a shared Weaviate cluster with Azurite
// that is reused across all tests in this package. This significantly reduces
// test execution time compared to spinning up a new cluster for each test.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var err error
	sharedCompose, err = setupSharedCluster(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to start shared cluster"))
	}

	// Log container URIs for debugging
	weaviateURI := sharedCompose.GetWeaviate().URI()
	azuriteURI := sharedCompose.GetAzurite().URI()

	// Verify contextionary is available
	contextionary := sharedCompose.GetText2VecContextionary()
	if contextionary != nil {
		// Log contextionary URI for debugging
		_ = contextionary.URI() // This ensures the container is accessible
	}

	// Set up environment variables for tests to use
	os.Setenv(envSharedWeaviateEndpoint, weaviateURI)
	os.Setenv(envSharedAzuriteEndpoint, azuriteURI)

	// Set up environment for Azurite
	connectionString := fmt.Sprintf(connectionStringTemplate, azuriteURI)
	os.Setenv("AZURE_STORAGE_CONNECTION_STRING", connectionString)

	// Create the default container (without *testing.T since we're in TestMain)
	if err := createAzureContainerWithRetry(ctx, azuriteURI, azureContainerName); err != nil {
		panic(errors.Wrap(err, "failed to create Azure container"))
	}

	// Set up the helper client to point to the shared cluster
	helper.SetupClient(weaviateURI)

	// Run all tests
	code := m.Run()

	// Clean up container (ignore errors during cleanup)
	_ = deleteAzureContainer(ctx, azuriteURI, azureContainerName)

	// Tear down the shared cluster
	if err := sharedCompose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate shared cluster"))
	}

	os.Exit(code)
}

// createAzureContainerWithRetry creates an Azure container with retry logic for TestMain.
func createAzureContainerWithRetry(ctx context.Context, endpoint, containerName string) error {
	connectionString := fmt.Sprintf(connectionStringTemplate, endpoint)

	var lastErr error
	for i := 0; i < 20; i++ { // Retry for up to 10 seconds
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		_, err = client.CreateContainer(ctx, containerName, nil)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}

// deleteAzureContainer deletes an Azure container and its contents.
func deleteAzureContainer(ctx context.Context, endpoint, containerName string) error {
	connectionString := fmt.Sprintf(connectionStringTemplate, endpoint)

	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return err
	}

	_, err = client.DeleteContainer(ctx, containerName, nil)
	return err
}

// setupSharedCluster creates a Weaviate cluster with Azurite for backup testing.
// The cluster includes:
// - 3-node Weaviate cluster (to test cluster backup scenarios)
// - Azurite Azure storage emulator
// - text2vec-contextionary vectorizer (for tests that need vectorization)
func setupSharedCluster(ctx context.Context) (*docker.DockerCompose, error) {
	compose, err := docker.New().
		WithBackendAzure(azureContainerName).
		WithText2VecContextionary().
		WithWeaviateCluster(3).
		Start(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start docker compose")
	}

	// Verify contextionary container is running
	contextionary := compose.GetText2VecContextionary()
	if contextionary == nil {
		return nil, errors.New("text2vec-contextionary container not found - vectorizer may not be available")
	}

	return compose, nil
}

// GetSharedCompose returns the shared Docker compose instance.
// This should only be called after TestMain has completed setup.
func GetSharedCompose() *docker.DockerCompose {
	return sharedCompose
}

// GetWeaviateURI returns the URI of the primary Weaviate node.
func GetWeaviateURI() string {
	if sharedCompose == nil {
		return os.Getenv(envSharedWeaviateEndpoint)
	}
	return sharedCompose.GetWeaviate().URI()
}

// GetAzuriteEndpoint returns the URI of the Azurite emulator.
func GetAzuriteEndpoint() string {
	if sharedCompose == nil {
		return os.Getenv(envSharedAzuriteEndpoint)
	}
	return sharedCompose.GetAzurite().URI()
}
