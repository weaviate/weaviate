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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func TestText2VecMorph(t *testing.T) {
	apiKey := os.Getenv("MORPH_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, MORPH_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, apiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()

	t.Run("text2vec-morph", testText2VecMorph(rest, grpc))
}

func createSingleNodeEnvironment(ctx context.Context, apiKey string,
) (*docker.DockerCompose, error) {
	compose, err := composeModules(apiKey).
		WithWeaviate().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("MODULES_CLIENT_TIMEOUT", fmt.Sprintf("%.0fs", companies.DefaultTimeout.Seconds())).
		Start(ctx)
	return compose, err
}

func composeModules(apiKey string) *docker.Compose {
	return docker.New().
		WithText2VecMorph(apiKey)
}
