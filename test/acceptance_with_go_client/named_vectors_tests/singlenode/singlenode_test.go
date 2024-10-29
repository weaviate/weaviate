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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	named_vectors_tests_util "acceptance_tests_with_client/named_vectors_tests"
)


func TestNamedVectors_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", named_vectors_tests_util.AllTests(endpoint))
	t.Run("legacy tests", named_vectors_tests_util.AllLegacyTests(endpoint))
}

func TestNamedVectors_SingleNode_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironmentAsyncIndexing(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", named_vectors_tests_util.AllTests(endpoint))
	t.Run("legacy tests", named_vectors_tests_util.AllLegacyTests(endpoint))
}

func TestNamedVectors_SingleNode_Restart(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	t.Run("restart", named_vectors_tests_util.TestRestart(compose))
}

func createSingleNodeEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = named_vectors_tests_util.ComposeModules().
		WithWeaviate().
		Start(ctx)
	return
}

func createSingleNodeEnvironmentAsyncIndexing(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = named_vectors_tests_util.ComposeModules().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviate().
		Start(ctx)
	return
}