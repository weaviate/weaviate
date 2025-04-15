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

func TestGenerativeGoogle_VertexAI_SingleNode(t *testing.T) {
	gcpProject := os.Getenv("GCP_PROJECT")
	if gcpProject == "" {
		t.Skip("skipping, GCP_PROJECT environment variable not present")
	}
	googleApiKey := os.Getenv("GOOGLE_APIKEY")
	if googleApiKey == "" {
		t.Skip("skipping, GOOGLE_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, googleApiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpointREST := compose.GetWeaviate().URI()
	endpointGRPC := compose.GetWeaviate().GrpcURI()

	t.Run("generative-google", testGenerativeGoogle(endpointREST, endpointGRPC, gcpProject, "generative-google"))
	t.Run("generative-palm", testGenerativeGoogle(endpointREST, endpointGRPC, gcpProject, "generative-palm"))
}

func createSingleNodeEnvironment(ctx context.Context, googleApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(googleApiKey).
		WithWeaviateWithGRPC().
		WithWeaviateEnv("MODULES_CLIENT_TIMEOUT", "120s").
		Start(ctx)
	return
}

func composeModules(googleApiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecGoogle(googleApiKey).
		WithGenerativeGoogle(googleApiKey)
	return
}
