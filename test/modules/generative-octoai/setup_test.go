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

package generative_octoai_tests

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGenerativeOctoAI_SingleNode(t *testing.T) {
	octoAIApiKey := os.Getenv("OCTOAI_APIKEY")
	if octoAIApiKey == "" {
		t.Skip("skipping, OCTOAI_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, octoAIApiKey)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("tests", testGenerativeOctoAI(endpoint, "https://text.octoai.run"))
}

func createSingleNodeEnvironment(ctx context.Context, octoAIApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(octoAIApiKey).
		WithWeaviate().
		Start(ctx)
	return
}

func composeModules(octoAIApiKey string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecOctoAI(octoAIApiKey).
		WithGenerativeOctoAI(octoAIApiKey)
	return
}
