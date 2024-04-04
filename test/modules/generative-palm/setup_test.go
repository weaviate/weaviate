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

package generative_palm_tests

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGenerativePaLM_VertexAI_SingleNode(t *testing.T) {
	gcpProject := os.Getenv("GCP_PROJECT")
	if gcpProject == "" {
		t.Skip("skipping, GCP_PROJECT environment variable not present")
	}
	palmApiKey := os.Getenv("PALM_APIKEY")
	if palmApiKey == "" {
		t.Skip("skipping, PALM_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, palmApiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("tests", testGenerativePaLM(endpoint, gcpProject))
}

func createSingleNodeEnvironment(ctx context.Context, palmApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(palmApiKey).
		WithWeaviate().
		Start(ctx)
	return
}

func composeModules(palmApiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecPaLM(palmApiKey).
		WithGenerativePaLM(palmApiKey)
	return
}
