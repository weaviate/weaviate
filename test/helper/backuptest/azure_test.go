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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureBackend_ImplementsInterface(t *testing.T) {
	// Compile-time check is in azure.go, but this makes it explicit in tests
	var backend BackupBackend = NewAzureBackend()
	assert.NotNil(t, backend)
	assert.Equal(t, "azure", backend.Name())
}

func TestAzureBackend_StartsAzurite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.Equal(t, StateNotStarted, backend.State())

	// Start backend - NO env vars set beforehand
	err := backend.Start(ctx)
	require.NoError(t, err, "Start should succeed with no pre-configuration")
	defer backend.Stop(ctx)

	// Verify state
	assert.Equal(t, StateRunning, backend.State())

	// Verify Azurite is accessible via HTTP
	endpoint := backend.Endpoint()
	require.NotEmpty(t, endpoint, "Endpoint should not be empty after Start")

	// Azurite blob endpoint should respond (may return 400 for root but proves it's running)
	resp, err := http.Get("http://" + endpoint + "/")
	require.NoError(t, err, "Azurite should be accessible")
	defer resp.Body.Close()
	// Azurite returns 400 for root path, but that's fine - it means it's running
	assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusBadRequest,
		"Azurite should respond (got %d)", resp.StatusCode)
}

func TestAzureBackend_CreatesContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify default container exists
	containers, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, containers, "backups", "default container should exist")

	// Create additional container
	err = backend.CreateBucket(ctx, "override-container")
	require.NoError(t, err)

	// Verify it exists
	containers, err = backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, containers, "override-container", "additional container should exist")

	// Creating same container again should return ErrBucketExists
	err = backend.CreateBucket(ctx, "override-container")
	assert.ErrorIs(t, err, ErrBucketExists)
}

func TestAzureBackend_GetWeaviateEnv(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	env := backend.GetWeaviateEnv()
	require.NotNil(t, env)

	// Verify all required keys are present
	requiredKeys := []string{
		"AZURE_STORAGE_CONNECTION_STRING",
		"BACKUP_AZURE_CONTAINER",
	}

	for _, key := range requiredKeys {
		assert.Contains(t, env, key, "env should contain %s", key)
		assert.NotEmpty(t, env[key], "%s should not be empty", key)
	}

	// Verify specific values
	assert.Equal(t, "backups", env["BACKUP_AZURE_CONTAINER"])
	assert.Contains(t, env["AZURE_STORAGE_CONNECTION_STRING"], "devstoreaccount1")
	assert.Contains(t, env["AZURE_STORAGE_CONNECTION_STRING"], "backuptest-azurite")
}

func TestAzureBackend_Cleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend(WithContainerReuse(false))
	require.NoError(t, backend.Start(ctx))

	endpoint := backend.Endpoint()
	require.NotEmpty(t, endpoint)

	// Stop backend
	err := backend.Stop(ctx)
	require.NoError(t, err)
	assert.Equal(t, StateStopped, backend.State())

	// Verify endpoint is cleared
	assert.Empty(t, backend.Endpoint())
	assert.Nil(t, backend.GetWeaviateEnv())
	assert.Nil(t, backend.AzureClient())
}

func TestAzureBackend_CustomContainerName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend(WithBucketName("my-custom-container"))
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify custom container exists
	containers, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, containers, "my-custom-container", "custom container should exist")

	// Verify env vars use custom container
	env := backend.GetWeaviateEnv()
	assert.Equal(t, "my-custom-container", env["BACKUP_AZURE_CONTAINER"])
}

func TestAzureBackend_ContractViolations(t *testing.T) {
	ctx := context.Background()

	t.Run("GetWeaviateEnv before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		assert.Nil(t, backend.GetWeaviateEnv())
	})

	t.Run("CreateBucket before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		err := backend.CreateBucket(ctx, "test")
		assert.ErrorIs(t, err, ErrNotStarted)
	})

	t.Run("Endpoint before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		assert.Empty(t, backend.Endpoint())
	})

	t.Run("AzureClient before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		assert.Nil(t, backend.AzureClient())
	})

	t.Run("ListBuckets before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		_, err := backend.ListBuckets(ctx)
		assert.ErrorIs(t, err, ErrNotStarted)
	})

	t.Run("ConnectionString before Start", func(t *testing.T) {
		backend := NewAzureBackend()
		assert.Empty(t, backend.ConnectionString())
	})
}

func TestAzureBackend_IdempotentStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	require.NoError(t, backend.Stop(ctx))

	// Second stop should not error
	err := backend.Stop(ctx)
	assert.NoError(t, err)
	assert.Equal(t, StateStopped, backend.State())
}

func TestAzureBackend_NetworkName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Network should be auto-generated
	networkName := backend.NetworkName()
	assert.NotEmpty(t, networkName, "network name should be set after Start")
}

func TestAzureBackend_InternalEndpoint(t *testing.T) {
	backend := NewAzureBackend()
	// InternalEndpoint is always the container name + port
	assert.Equal(t, "backuptest-azurite:10000", backend.InternalEndpoint())
}

func TestAzureBackend_ContainerName(t *testing.T) {
	backend := NewAzureBackend()
	assert.Equal(t, "backuptest-azurite", backend.ContainerName())
}

func TestAzureBackend_DeleteContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Create a test container
	err := backend.CreateBucket(ctx, "test-delete-container")
	require.NoError(t, err)

	// Verify it exists
	containers, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, containers, "test-delete-container")

	// Delete it
	err = backend.DeleteBucket(ctx, "test-delete-container")
	require.NoError(t, err)

	// Verify it's gone
	containers, err = backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.NotContains(t, containers, "test-delete-container")
}

func TestAzureBackend_ConnectionString(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewAzureBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	connStr := backend.ConnectionString()
	assert.NotEmpty(t, connStr)
	assert.Contains(t, connStr, "devstoreaccount1")
	assert.Contains(t, connStr, "AccountKey=")
	assert.Contains(t, connStr, "BlobEndpoint=")
}
