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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	// Environment variable names for sharing cluster endpoints
	envSharedWeaviateEndpoint = "SHARED_WEAVIATE_ENDPOINT"
	envSharedMinioEndpoint    = "SHARED_MINIO_ENDPOINT"
	envSharedS3Region         = "SHARED_AWS_REGION"
)

// sharedCompose holds the Docker compose instance for all tests in this package.
// This is set up once in TestMain and shared across all tests.
var sharedCompose *docker.DockerCompose

// defaultS3Region is the region used for S3 tests.
const defaultS3Region = "eu-west-1"

// TestMain sets up a shared Weaviate cluster with MinIO (S3-compatible storage)
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
	minioURI := sharedCompose.GetMinIO().URI()

	// Verify contextionary is available
	contextionary := sharedCompose.GetText2VecContextionary()
	if contextionary != nil {
		// Log contextionary URI for debugging
		_ = contextionary.URI() // This ensures the container is accessible
	}

	// Set up environment variables for tests to use
	os.Setenv(envSharedWeaviateEndpoint, weaviateURI)
	os.Setenv(envSharedMinioEndpoint, minioURI)
	os.Setenv(envSharedS3Region, defaultS3Region)

	// Set up the helper client to point to the shared cluster
	helper.SetupClient(weaviateURI)

	// Run all tests
	code := m.Run()

	// Tear down the shared cluster
	if err := sharedCompose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate shared cluster"))
	}

	os.Exit(code)
}

// setupSharedCluster creates a Weaviate cluster with MinIO for S3 backup testing.
// The cluster includes:
// - 3-node Weaviate cluster (to test cluster backup scenarios)
// - MinIO S3-compatible storage
// - text2vec-contextionary vectorizer (for tests that need vectorization)
func setupSharedCluster(ctx context.Context) (*docker.DockerCompose, error) {
	compose, err := docker.New().
		WithBackendS3("backups", defaultS3Region).
		WithWeaviateEnv("AWS_REGION", defaultS3Region).
		WithText2VecContextionary().
		WithWeaviateCluster(3).
		WithWeaviateEnv("BACKUP_MIN_CHUNK_SIZE", "1024").            // allow incremental backups in tests
		WithWeaviateEnv("PERSISTENCE_LSM_MAX_SEGMENT_SIZE", "1024"). // avoid compactions so incremental backups have unchanged files
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

// GetMinioURI returns the URI of the MinIO server.
func GetMinioURI() string {
	if sharedCompose == nil {
		return os.Getenv(envSharedMinioEndpoint)
	}
	return sharedCompose.GetMinIO().URI()
}

// GetS3Region returns the S3 region used for tests.
func GetS3Region() string {
	return defaultS3Region
}
