//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package named_vectors_cluster_test

import (
	"context"
	"testing"

	nvutil "acceptance_tests_with_client/named_vectors"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestNamedVectors_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := createClusterEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", nvutil.AllTests(endpoint))
	t.Run("legacy tests", nvutil.AllLegacyTests(endpoint))
}

func TestNamedVectors_Cluster_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := createClusterEnvironmentAsyncIndexing(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", nvutil.AllTests(endpoint))
	t.Run("legacy tests", nvutil.AllLegacyTests(endpoint))
}

func createClusterEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = nvutil.ComposeModules().
		WithWeaviateCluster(3).
		Start(ctx)
	return
}

func createClusterEnvironmentAsyncIndexing(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = nvutil.ComposeModules().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateCluster(3).
		Start(ctx)
	return
}
