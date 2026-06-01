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

func TestS3Backend_ImplementsInterface(t *testing.T) {
	// Compile-time check is in s3.go, but this makes it explicit in tests
	var backend BackupBackend = NewS3Backend()
	assert.NotNil(t, backend)
	assert.Equal(t, "s3", backend.Name())
}

func TestS3Backend_StartsMinIO(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend()
	require.Equal(t, StateNotStarted, backend.State())

	// Start backend - NO env vars set beforehand
	err := backend.Start(ctx)
	require.NoError(t, err, "Start should succeed with no pre-configuration")
	defer backend.Stop(ctx)

	// Verify state
	assert.Equal(t, StateRunning, backend.State())

	// Verify MinIO is accessible via HTTP
	endpoint := backend.Endpoint()
	require.NotEmpty(t, endpoint, "Endpoint should not be empty after Start")

	resp, err := http.Get("http://" + endpoint + "/minio/health/ready")
	require.NoError(t, err, "MinIO health check should be accessible")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "MinIO should be healthy")
}

func TestS3Backend_CreatesBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend()
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

func TestS3Backend_GetWeaviateEnv(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	env := backend.GetWeaviateEnv()
	require.NotNil(t, env)

	// Verify all required keys are present
	requiredKeys := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_KEY",
		"AWS_REGION",
		"BACKUP_S3_BUCKET",
		"BACKUP_S3_ENDPOINT",
		"BACKUP_S3_USE_SSL",
	}

	for _, key := range requiredKeys {
		assert.Contains(t, env, key, "env should contain %s", key)
		assert.NotEmpty(t, env[key], "%s should not be empty", key)
	}

	// Verify specific values
	assert.Equal(t, "backups", env["BACKUP_S3_BUCKET"])
	assert.Equal(t, "false", env["BACKUP_S3_USE_SSL"])
	assert.Equal(t, "us-east-1", env["AWS_REGION"])
}

func TestS3Backend_Cleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend(WithContainerReuse(false))
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
	assert.Nil(t, backend.S3Client())

	// Note: With container reuse disabled, we can't easily verify the container
	// is gone without additional docker inspection, but the state should be correct
}

func TestS3Backend_CustomBucketName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend(WithBucketName("my-custom-bucket"))
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify custom bucket exists
	buckets, err := backend.ListBuckets(ctx)
	require.NoError(t, err)
	assert.Contains(t, buckets, "my-custom-bucket", "custom bucket should exist")

	// Verify env vars use custom bucket
	env := backend.GetWeaviateEnv()
	assert.Equal(t, "my-custom-bucket", env["BACKUP_S3_BUCKET"])
}

func TestS3Backend_CustomRegion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3BackendWithOptions(
		[]BackendOption{},
		WithS3Region("eu-west-1"),
	)
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Verify region in env vars
	env := backend.GetWeaviateEnv()
	assert.Equal(t, "eu-west-1", env["AWS_REGION"])
	assert.Equal(t, "eu-west-1", backend.Region())
}

func TestS3Backend_ContractViolations(t *testing.T) {
	ctx := context.Background()

	t.Run("GetWeaviateEnv before Start", func(t *testing.T) {
		backend := NewS3Backend()
		assert.Nil(t, backend.GetWeaviateEnv())
	})

	t.Run("CreateBucket before Start", func(t *testing.T) {
		backend := NewS3Backend()
		err := backend.CreateBucket(ctx, "test")
		assert.ErrorIs(t, err, ErrNotStarted)
	})

	t.Run("Endpoint before Start", func(t *testing.T) {
		backend := NewS3Backend()
		assert.Empty(t, backend.Endpoint())
	})

	t.Run("S3Client before Start", func(t *testing.T) {
		backend := NewS3Backend()
		assert.Nil(t, backend.S3Client())
	})

	t.Run("ListBuckets before Start", func(t *testing.T) {
		backend := NewS3Backend()
		_, err := backend.ListBuckets(ctx)
		assert.ErrorIs(t, err, ErrNotStarted)
	})
}

func TestS3Backend_IdempotentStop(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend()
	require.NoError(t, backend.Start(ctx))
	require.NoError(t, backend.Stop(ctx))

	// Second stop should not error
	err := backend.Stop(ctx)
	assert.NoError(t, err)
	assert.Equal(t, StateStopped, backend.State())
}

func TestS3Backend_NetworkName(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	backend := NewS3Backend()
	require.NoError(t, backend.Start(ctx))
	defer backend.Stop(ctx)

	// Network should be auto-generated
	networkName := backend.NetworkName()
	assert.NotEmpty(t, networkName, "network name should be set after Start")
}

func TestS3Backend_InternalEndpoint(t *testing.T) {
	backend := NewS3Backend()
	// InternalEndpoint is always the container name + port
	assert.Equal(t, "backuptest-minio:9000", backend.InternalEndpoint())
}

func TestS3Backend_ContainerName(t *testing.T) {
	backend := NewS3Backend()
	assert.Equal(t, "backuptest-minio", backend.ContainerName())
}
