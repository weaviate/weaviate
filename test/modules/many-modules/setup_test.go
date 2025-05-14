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
	"os"
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

func Test_ManyModules_SingleNode_Enabled_API_Based_Modules(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironmentWithEnabledApiBasedModules(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	t.Run("api based modules", apiBasedModulesTests(endpoint))
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

func createSingleNodeEnvironmentWithEnabledApiBasedModules(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviateEnv("ENABLE_API_BASED_MODULES", "true").
		WithWeaviate().
		Start(ctx)
	return
}

func createClusterEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviateCluster(3).
		Start(ctx)
	return
}

func composeModules() (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecContextionary().
		WithText2VecTransformers().
		WithText2VecOpenAI(os.Getenv("OPENAI_APIKEY"), os.Getenv("OPENAI_ORGANIZATION"), os.Getenv("AZURE_APIKEY")).
		WithText2VecCohere(os.Getenv("COHERE_APIKEY")).
		WithText2VecVoyageAI(os.Getenv("VOYAGEAI_APIKEY")).
		WithText2VecGoogle(os.Getenv("GOOGLE_APIKEY")).
		WithText2VecHuggingFace(os.Getenv("HUGGINGFACE_APIKEY")).
		WithText2VecAWS(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")).
		WithGenerativeOpenAI(os.Getenv("OPENAI_APIKEY"), os.Getenv("OPENAI_ORGANIZATION"), os.Getenv("AZURE_APIKEY")).
		WithGenerativeCohere(os.Getenv("COHERE_APIKEY")).
		WithGenerativeGoogle(os.Getenv("GOOGLE_APIKEY")).
		WithGenerativeAWS(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")).
		WithGenerativeAnyscale().
		WithGenerativeAnthropic(os.Getenv("ANTHROPIC_APIKEY")).
		WithGenerativeFriendliAI(os.Getenv("FRIENDLI_TOKEN")).
		WithQnAOpenAI().
		WithRerankerCohere().
		WithRerankerVoyageAI()
	return
}
