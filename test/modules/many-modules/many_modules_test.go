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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func Test_ManyModules_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("many modules", manyModulesTests(endpoint))
	t.Run("create schema with specific text2vec-openai settings", createSchemaOpenAISanityChecks(endpoint))
}

func Test_ManyModules_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := createClusterEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("many modules", manyModulesTests(endpoint))
	t.Run("create schema with specific text2vec-openai settings", createSchemaOpenAISanityChecks(endpoint))
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
		WithText2VecCohere().
		WithText2VecVoyageAI().
		WithText2VecPaLM().
		WithText2VecHuggingFace().
		WithText2VecAWS().
		WithGenerativeOpenAI().
		WithGenerativeCohere().
		WithGenerativePaLM().
		WithGenerativeAWS().
		WithGenerativeAnyscale().
		WithQnAOpenAI().
		WithRerankerCohere()
	return
}
