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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGenerativeOllama_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpointREST := compose.GetWeaviate().URI()
	endpointGRPC := compose.GetWeaviate().GrpcURI()
	ollamaApiEndpoint := compose.GetOllamaGenerative().GetEndpoint("apiEndpoint")

	t.Run("tests", testGenerativeOllama(endpointREST, endpointGRPC, ollamaApiEndpoint))
}

func createSingleNodeEnvironment(ctx context.Context,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("MODULES_CLIENT_TIMEOUT", "120s").
		Start(ctx)
	return
}

func composeModules() (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecTransformers().
		WithGenerativeOllama()
	return
}
