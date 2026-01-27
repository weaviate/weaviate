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
)

// sharedCompose holds the Docker compose instance for all tests in this package.
// This is set up once in TestMain and shared across all tests.
var sharedCompose *docker.DockerCompose

// TestMain sets up a shared single-node Weaviate cluster with filesystem backup enabled.
// Filesystem backup only works on single-node clusters, so we use With1NodeCluster().
// This is reused across all tests in this package to reduce test execution time.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var err error
	sharedCompose, err = setupSharedCluster(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to start shared cluster"))
	}

	// Log container URI for debugging
	weaviateURI := sharedCompose.GetWeaviate().URI()

	// Verify contextionary is available
	contextionary := sharedCompose.GetText2VecContextionary()
	if contextionary != nil {
		// Log contextionary URI for debugging
		_ = contextionary.URI() // This ensures the container is accessible
	}

	// Set up environment variables for tests to use
	os.Setenv(envSharedWeaviateEndpoint, weaviateURI)

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

// setupSharedCluster creates a single-node Weaviate cluster with filesystem backup enabled.
// The cluster includes:
// - Single-node Weaviate (filesystem backup only works on single-node clusters)
// - Filesystem backup module with BACKUP_FILESYSTEM_PATH=/tmp/backups
// - text2vec-contextionary vectorizer (for tests that need vectorization)
func setupSharedCluster(ctx context.Context) (*docker.DockerCompose, error) {
	compose, err := docker.New().
		WithBackendFilesystem().
		WithText2VecContextionary().
		With1NodeCluster().
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

// GetWeaviateURI returns the URI of the Weaviate node.
func GetWeaviateURI() string {
	if sharedCompose == nil {
		return os.Getenv(envSharedWeaviateEndpoint)
	}
	return sharedCompose.GetWeaviate().URI()
}
