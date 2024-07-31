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

func TestGenerativeAnthropic_SingleNode(t *testing.T) {
	apiKey := os.Getenv("ANTHROPIC_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, ANTHROPIC_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, apiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("tests", testGenerativeAnthropic(endpoint))
}

func createSingleNodeEnvironment(ctx context.Context, apiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(apiKey).
		WithWeaviate().
		WithWeaviateEnv("EXPERIMENTAL_DYNAMIC_RAG_SYNTAX", "true").
		Start(ctx)
	return
}

func composeModules(apiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecTransformers().
		WithGenerativeAnthropic(apiKey)
	return
}
