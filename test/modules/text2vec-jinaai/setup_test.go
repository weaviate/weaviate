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

func TestText2VecJinaAI(t *testing.T) {
	jinaApiKey := os.Getenv("JINAAI_APIKEY")
	if jinaApiKey == "" {
		t.Skip("skipping, JINAAI_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, jinaApiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()

	t.Run("tests", testText2VecJinaAI(rest, grpc))
}

func createSingleNodeEnvironment(ctx context.Context, jinaApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(jinaApiKey).
		WithWeaviate().
		WithWeaviateWithGRPC().
		Start(ctx)
	return
}

func composeModules(jinaApiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecJinaAI(jinaApiKey)
	return
}
