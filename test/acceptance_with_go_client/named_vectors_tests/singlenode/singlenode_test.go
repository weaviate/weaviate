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

package singlenode

import (
	"context"
	"testing"

	test_suits "acceptance_tests_with_client/named_vectors_tests/test_suits"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestNamedVectors_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", test_suits.AllTests(endpoint))
	t.Run("legacy tests", test_suits.AllLegacyTests(endpoint))
}

func TestNamedVectors_SingleNode_AsyncIndexing(t *testing.T) {
	// ctx := context.Background()
	// compose, err := createSingleNodeEnvironmentAsyncIndexing(ctx)
	// require.NoError(t, err)
	// defer func() {
	// 	require.NoError(t, compose.Terminate(ctx))
	// }()
	// endpoint := compose.GetWeaviate().URI()
	endpoint := "localhost:8080"
	t.Run("tests", test_suits.AllTests(endpoint))
	// t.Run("legacy tests", test_suits.AllLegacyTests(endpoint))
}

func TestNamedVectors_SingleNode_Restart(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	t.Run("restart", test_suits.TestRestart(compose))
}

func createSingleNodeEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = test_suits.ComposeModules().
		WithWeaviate().
		Start(ctx)
	return
}

func createSingleNodeEnvironmentAsyncIndexing(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = test_suits.ComposeModules().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "1s").
		WithWeaviate().
		Start(ctx)
	return
}
