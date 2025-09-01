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

func TestText2VecOpenAI(t *testing.T) {
	apiKey := os.Getenv("OPENAI_APIKEY")
	if apiKey == "" {
		t.Skip("skipping, OPENAI_APIKEY environment variable not present")
	}
	organization := os.Getenv("OPENAI_ORGANIZATION")
	if organization == "" {
		t.Skip("skipping, OPENAI_ORGANIZATION environment variable not present")
	}

	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, apiKey, organization)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()

	t.Run("tests", testText2VecOpenAI(rest, grpc))
}

func createSingleNodeEnvironment(ctx context.Context, apiKey, organization string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(apiKey, organization).
		WithWeaviate().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("MODULES_CLIENT_TIMEOUT", fmt.Sprintf("%.0fs", companies.DefaultTimeout.Seconds())).
		Start(ctx)
	return
}

func composeModules(apiKey, organization string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecOpenAI(apiKey, organization, "")
	return
}
