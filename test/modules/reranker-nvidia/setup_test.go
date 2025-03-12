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

func TestRerankerNvidia(t *testing.T) {
	apiApiKey := os.Getenv("NVIDIA_APIKEY")
	if apiApiKey == "" {
		t.Skip("skipping, NVIDIA_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, apiApiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("reranker-nvidia", testRerankerNvidia(endpoint))
}

func createSingleNodeEnvironment(ctx context.Context, apiApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(apiApiKey).
		WithWeaviate().
		Start(ctx)
	return
}

func composeModules(apiApiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecContextionary().
		WithRerankerNvidia(apiApiKey)
	return
}
