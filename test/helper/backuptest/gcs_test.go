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

func TestGCSBackend_ImplementsInterface(t *testing.T) {
	// Compile-time check is in gcs.go, but this makes it explicit in tests
	var backend BackupBackend = NewGCSBackend()
	assert.NotNil(t, backend)
	assert.Equal(t, "gcs", backend.Name())
}

func TestGCSBackend_StartsEmulator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.Equal(t, StateNotStarted, backend.State())

	// Start backend - NO env vars set beforehand
	err := backend.Start(ctx)
	require.NoError(t, err, "Start should succeed with no pre-configuration")
	defer backend.Stop(ctx)

	// Verify state
	assert.Equal(t, StateRunning, backend.State())

	// Verify GCS emulator is accessible via HTTP
	endpoint := backend.Endpoint()
	require.NotEmpty(t, endpoint, "Endpoint should not be empty after Start")

	resp, err := http.Get("http://" + endpoint + "/")
	require.NoError(t, err, "GCS emulator should be accessible")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "GCS emulator should respond")
}

func TestGCSBackend_CreatesBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify default bucket exists
	buckets, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, buckets, "backups", "default bucket should exist")

	// Create additional bucket
	err = backend.CreateBucket(ctx, "override-bucket")
	require.NoError(t, err)

	// Verify it exists
	buckets, err = backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, buckets, "override-bucket", "additional bucket should exist")

	// Creating same bucket again should return ErrBucketExists
	err = backend.CreateBucket(ctx, "override-bucket")
	assert.ErrorIs(t, err, ErrBucketExists)
}

func TestGCSBackend_GetWeaviateEnv(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	env := backend.GetWeaviateEnv()
	require.NotNil(t, env)

	// Verify all required keys are present
	requiredKeys := []string{
		"GOOGLE_CLOUD_PROJECT",
		"STORAGE_EMULATOR_HOST",
		"BACKUP_GCS_BUCKET",
		"BACKUP_GCS_USE_AUTH",
	}

	for _, key := range requiredKeys {
		assert.Contains(t, env, key, "env should contain %s", key)
	}

	// Verify specific values
	assert.Equal(t, "backups", env["BACKUP_GCS_BUCKET"])
	assert.Equal(t, "false", env["BACKUP_GCS_USE_AUTH"])
	assert.Equal(t, "test-project", env["GOOGLE_CLOUD_PROJECT"])
	assert.NotEmpty(t, env["STORAGE_EMULATOR_HOST"])
}

func TestGCSBackend_Cleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend(WithContainerReuse(false))
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
	assert.Nil(t, backend.GCSClient())
}

func TestGCSBackend_CustomBucketName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend(WithBucketName("my-custom-bucket"))
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify custom bucket exists
	buckets, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, buckets, "my-custom-bucket", "custom bucket should exist")

	// Verify env vars use custom bucket
	env := backend.GetWeaviateEnv()
	assert.Equal(t, "my-custom-bucket", env["BACKUP_GCS_BUCKET"])
}

func TestGCSBackend_CustomProjectID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackendWithOptions(
		[]BackendOption{},
		WithGCSProjectID("my-project-id"),
	)
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify project ID in env vars
	env := backend.GetWeaviateEnv()
	assert.Equal(t, "my-project-id", env["GOOGLE_CLOUD_PROJECT"])
	assert.Equal(t, "my-project-id", backend.ProjectID())
}

func TestGCSBackend_ContractViolations(t *testing.T) {
	ctx := context.Background()

	t.Run("GetWeaviateEnv before Start", func(t *testing.T) {
		backend := NewGCSBackend()
		assert.Nil(t, backend.GetWeaviateEnv())
	})

	t.Run("CreateBucket before Start", func(t *testing.T) {
		backend := NewGCSBackend()
		err := backend.CreateBucket(ctx, "test")
		assert.ErrorIs(t, err, ErrNotStarted)
	})

	t.Run("Endpoint before Start", func(t *testing.T) {
		backend := NewGCSBackend()
		assert.Empty(t, backend.Endpoint())
	})

	t.Run("GCSClient before Start", func(t *testing.T) {
		backend := NewGCSBackend()
		assert.Nil(t, backend.GCSClient())
	})

	t.Run("ListBuckets before Start", func(t *testing.T) {
		backend := NewGCSBackend()
		_, err := backend.ListBuckets(ctx)
		assert.ErrorIs(t, err, ErrNotStarted)
	})
}

func TestGCSBackend_IdempotentStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.NoError(t, backend.Start(ctx))
	require.NoError(t, backend.Stop(ctx))

	// Second stop should not error
	err := backend.Stop(ctx)
	assert.NoError(t, err)
	assert.Equal(t, StateStopped, backend.State())
}

func TestGCSBackend_NetworkName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Network should be auto-generated
	networkName := backend.NetworkName()
	assert.NotEmpty(t, networkName, "network name should be set after Start")
}

func TestGCSBackend_InternalEndpoint(t *testing.T) {
	backend := NewGCSBackend()
	// InternalEndpoint is always the container name + port
	assert.Equal(t, "backuptest-gcs:9090", backend.InternalEndpoint())
}

func TestGCSBackend_ContainerName(t *testing.T) {
	backend := NewGCSBackend()
	assert.Equal(t, "backuptest-gcs", backend.ContainerName())
}

func TestGCSBackend_DeleteBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewGCSBackend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Create a test bucket
	err := backend.CreateBucket(ctx, "test-delete-bucket")
	require.NoError(t, err)

	// Verify it exists
	buckets, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, buckets, "test-delete-bucket")

	// Delete it
	err = backend.DeleteBucket(ctx, "test-delete-bucket")
	require.NoError(t, err)

	// Verify it's gone
	buckets, err = backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.NotContains(t, buckets, "test-delete-bucket")
}
