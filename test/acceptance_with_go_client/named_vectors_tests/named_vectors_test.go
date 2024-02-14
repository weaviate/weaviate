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

package named_vectors_tests

import (
	"context"
	"testing"

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
	t.Run("tests", allTests(t, endpoint))
}

func TestNamedVectors_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := createClusterEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("tests", allTests(t, endpoint))
}

func allTests(t *testing.T, endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("schema", testCreateSchemaWithNoneVectorizer(t, endpoint))
		t.Run("object", testCreateObject(t, endpoint))
		t.Run("batch", testBatchObject(t, endpoint))
		t.Run("none vectorizer", testCreateSchemaWithNoneVectorizer(t, endpoint))
		t.Run("mixed objects with none and vectorizer", testCreateSchemaWithMixedVectorizers(t, endpoint))
		t.Run("classes with properties setting", testCreateWithModulePropertiesObject(t, endpoint))
	}
}

func createSingleNodeEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviate().
		Start(ctx)
	return
}

func createClusterEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviateCluster().
		Start(ctx)
	return
}

func composeModules() (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecContextionary().
		WithText2VecTransformers().
		WithText2VecOpenAI().
		WithText2VecCohere()
	return
}
