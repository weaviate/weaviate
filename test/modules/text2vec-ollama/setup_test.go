//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

func TestText2VecOllama_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()
	ollamaApiEndpoint := compose.GetOllamaVectorizer().GetEndpoint("apiEndpoint")

	t.Run("tests", testText2VecOllama(rest, grpc, ollamaApiEndpoint))
}

func TestText2VecOllama_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := createClusterEnvironment(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()
	ollamaApiEndpoint := compose.GetOllamaVectorizer().GetEndpoint("apiEndpoint")

	t.Run("tests", testText2VecOllama(rest, grpc, ollamaApiEndpoint))
}

func createSingleNodeEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviate().
		WithWeaviateWithGRPC().
		Start(ctx)
	return compose, err
}

func createClusterEnvironment(ctx context.Context) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	return compose, err
}

func composeModules() (composeModules *docker.Compose) {
	composeModules = docker.New().WithText2VecOllama()
	return composeModules
}
