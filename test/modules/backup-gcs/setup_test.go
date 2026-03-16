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
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	// Environment variable names for sharing cluster endpoints
	envSharedWeaviateEndpoint = "SHARED_WEAVIATE_ENDPOINT"
	envSharedGCSEndpoint      = "SHARED_GCS_ENDPOINT"

	// GCS emulator configuration
	gcsProjectID  = "backup-test-project"
	gcsBucketName = "backups"
)

// sharedCompose holds the Docker compose instance for all tests in this package.
// This is set up once in TestMain and shared across all tests.
var sharedCompose *docker.DockerCompose

// TestMain sets up a shared Weaviate cluster with GCS emulator
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
	gcsURI := sharedCompose.GetGCS().URI()

	// Verify contextionary is available
	contextionary := sharedCompose.GetText2VecContextionary()
	if contextionary != nil {
		// Log contextionary URI for debugging
		_ = contextionary.URI() // This ensures the container is accessible
	}

	// Set up environment variables for tests to use
	os.Setenv(envSharedWeaviateEndpoint, weaviateURI)
	os.Setenv(envSharedGCSEndpoint, gcsURI)

	// Set up environment for GCS emulator
	os.Setenv("STORAGE_EMULATOR_HOST", gcsURI)
	os.Setenv("GOOGLE_CLOUD_PROJECT", gcsProjectID)

	// Create the default bucket (without *testing.T since we're in TestMain)
	if err := createGCSBucketWithRetry(ctx, gcsProjectID, gcsBucketName); err != nil {
		panic(errors.Wrap(err, "failed to create GCS bucket"))
	}

	// Set up the helper client to point to the shared cluster
	helper.SetupClient(weaviateURI)

	// Run all tests
	code := m.Run()

	// Clean up bucket (ignore errors during cleanup)
	_ = deleteGCSBucket(ctx, gcsBucketName)

	// Tear down the shared cluster
	if err := sharedCompose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate shared cluster"))
	}

	os.Exit(code)
}

// createGCSBucketWithRetry creates a GCS bucket with retry logic for TestMain.
// It uses the STORAGE_EMULATOR_HOST environment variable which must be set before calling.
func createGCSBucketWithRetry(ctx context.Context, projectID, bucketName string) error {
	var lastErr error
	for i := 0; i < 20; i++ { // Retry for up to 10 seconds
		opts := []option.ClientOption{option.WithoutAuthentication()}
		if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
			opts = append(opts, option.WithEndpoint(emulatorHost))
		}

		client, err := storage.NewClient(ctx, opts...)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		err = client.Bucket(bucketName).Create(ctx, projectID, nil)
		client.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}

// deleteGCSBucket deletes a GCS bucket and its contents.
// It uses the STORAGE_EMULATOR_HOST environment variable which must be set before calling.
func deleteGCSBucket(ctx context.Context, bucketName string) error {
	opts := []option.ClientOption{option.WithoutAuthentication()}
	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		opts = append(opts, option.WithEndpoint(emulatorHost))
	}

	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)

	// Delete all objects in the bucket first
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if err != nil {
			break // No more objects or error
		}
		_ = bucket.Object(objAttrs.Name).Delete(ctx)
	}

	return bucket.Delete(ctx)
}

// setupSharedCluster creates a Weaviate cluster with GCS emulator for backup testing.
// The cluster includes:
// - 3-node Weaviate cluster (to test cluster backup scenarios)
// - GCS emulator storage
// - text2vec-contextionary vectorizer (for tests that need vectorization)
func setupSharedCluster(ctx context.Context) (*docker.DockerCompose, error) {
	compose, err := docker.New().
		WithBackendGCS(gcsBucketName).
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

// GetGCSEndpoint returns the URI of the GCS emulator.
func GetGCSEndpoint() string {
	if sharedCompose == nil {
		return os.Getenv(envSharedGCSEndpoint)
	}
	return sharedCompose.GetGCS().URI()
}
