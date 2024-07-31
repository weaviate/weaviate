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

package tests

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGenerativeOpenAI_SingleNode(t *testing.T) {
	apiKey := os.Getenv("OPENAI_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, OPENAI_APIKEY environment variable not present")
	}
	organization := os.Getenv("OPENAI_ORGANIZATION")
	if apiKey == "" {
		t.Skip("skipping, OPENAI_ORGANIZATION environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, apiKey, organization)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("tests", testGenerativeOpenAI(endpoint))
}

func createSingleNodeEnvironment(ctx context.Context, apiKey, organization string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(apiKey, organization).
		WithWeaviate().
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_DYNAMIC_RAG_SYNTAX", "true").
		Start(ctx)
	return
}

func composeModules(apiKey, organization string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecTransformers().
		WithGenerativeOpenAI(apiKey, organization, "")
	return
}
