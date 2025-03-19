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

	"acceptance_tests_with_client/named_vectors_tests/test_suits"

	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestNamedVectors_SingleNode(t *testing.T) {
	//ctx := context.Background()
	//compose, err := createSingleNodeEnvironment(ctx)
	//require.NoError(t, err)
	//defer func() {
	//	require.NoError(t, compose.Terminate(ctx))
	//}()
	//endpoint := compose.GetWeaviate().URI()
	//t.Run("tests", test_suits.AllTests(endpoint))
	//t.Run("legacy tests", test_suits.AllLegacyTests(endpoint))
	endpoint := "localhost:8080"
	t.Run("mixed vector tests", test_suits.AllMixedVectorsTests(endpoint))
}

func TestNamedVectors_SingleNode_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironmentAsyncIndexing(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", test_suits.AllTests(endpoint))
	t.Run("legacy tests", test_suits.AllLegacyTests(endpoint))
	t.Run("mixed vector tests", test_suits.AllMixedVectorsTests(endpoint))
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

func TestNamedVectors_VerifyMixedSchemaIsRejectedWithoutEnvFlag(t *testing.T) {
	compose, err := test_suits.ComposeModules().
		WithWeaviate().
		Start(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: compose.GetWeaviate().URI()})
	require.Nil(t, err)

	err = client.Schema().ClassCreator().
		WithClass(&models.Class{
			Class: "MixedVectors",
			VectorConfig: map[string]models.VectorConfig{
				"contextionary": {
					Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
					VectorIndexType: "hnsw",
				},
			},
			VectorIndexType: "hnsw",
			Vectorizer:      "text2vec-contextionary",
		}).
		Do(ctx)
	require.ErrorContains(t, err, "class MixedVectors has configuration for both class level and named vectors which is currently not supported.")
}

func createSingleNodeEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = test_suits.ComposeModules().
		WithWeaviate().
		WithWeaviateEnv("EXPERIMENTAL_BACKWARDS_COMPATIBLE_NAMED_VECTORS", "true").
		Start(ctx)
	return
}

func createSingleNodeEnvironmentAsyncIndexing(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = test_suits.ComposeModules().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "1s").
		WithWeaviateEnv("EXPERIMENTAL_BACKWARDS_COMPATIBLE_NAMED_VECTORS", "true").
		WithWeaviate().
		Start(ctx)
	return
}
