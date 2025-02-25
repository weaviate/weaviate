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

func TestText2VecAWS_SingleNode(t *testing.T) {
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKey == "" {
			t.Skip("skipping, AWS_ACCESS_KEY or AWS_ACCESS_KEY_ID environment variable not present")
		}
	}
	secretKey := os.Getenv("AWS_SECRET_KEY")
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			t.Skip("skipping, AWS_SECRET_KEY or AWS_SECRET_ACCESS_KEY environment variable not present")
		}
	}
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")
	if sessionToken == "" {
		t.Skip("skipping, AWS_SESSION_TOKEN environment variable not present")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		t.Skip("skipping, AWS_REGION environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, accessKey, secretKey, sessionToken)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	rest := compose.GetWeaviate().URI()
	grpc := compose.GetWeaviate().GrpcURI()

	t.Run("tests", testText2VecAWS(rest, grpc, region))
}

func createSingleNodeEnvironment(ctx context.Context, accessKey, secretKey, sessionToken string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(accessKey, secretKey, sessionToken).
		WithWeaviate().
		WithWeaviateWithGRPC().
		Start(ctx)
	return
}

func composeModules(accessKey, secretKey, sessionToken string) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecAWS(accessKey, secretKey, sessionToken)
	return
}
