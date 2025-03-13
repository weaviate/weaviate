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

func TestGenerativeManyModules_SingleNode(t *testing.T) {
	// AWS
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKey == "" {
			t.Skip("skipping, AWS_ACCESS_KEY environment variable not present")
		}
	}
	secretKey := os.Getenv("AWS_SECRET_KEY")
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			t.Skip("skipping, AWS_SECRET_KEY environment variable not present")
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
	// Google
	gcpProject := os.Getenv("GCP_PROJECT")
	if gcpProject == "" {
		t.Skip("skipping, GCP_PROJECT environment variable not present")
	}
	googleApiKey := os.Getenv("GOOGLE_APIKEY")
	if googleApiKey == "" {
		t.Skip("skipping, GOOGLE_APIKEY environment variable not present")
	}
	// OpenAI
	openAIApiKey := os.Getenv("OPENAI_APIKEY")
	openAIOrganization := os.Getenv("OPENAI_ORGANIZATION")
	// Cohere
	cohereApiKey := os.Getenv("COHERE_APIKEY")
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, accessKey, secretKey, sessionToken,
		openAIApiKey, openAIOrganization, googleApiKey, cohereApiKey,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()
	ollamaApiEndpoint := compose.GetOllamaGenerative().GetEndpoint("apiEndpoint")

	t.Run("tests", testGenerativeManyModules(endpoint, ollamaApiEndpoint, region, gcpProject))
}

func createSingleNodeEnvironment(ctx context.Context,
	accessKey, secretKey, sessionToken string,
	openAIApiKey, openAIOrganization string,
	googleApiKey, cohereApiKey string,
) (compose *docker.DockerCompose, err error) {
	compose, err = composeModules(accessKey, secretKey, sessionToken,
		openAIApiKey, openAIOrganization, googleApiKey, cohereApiKey,
	).
		WithWeaviate().
		Start(ctx)
	return
}

func composeModules(accessKey, secretKey, sessionToken string,
	openAIApiKey, openAIOrganization string,
	googleApiKey, cohereApiKey string,
) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecTransformers().
		WithGenerativeOllama().
		WithGenerativeAWS(accessKey, secretKey, sessionToken).
		WithGenerativeGoogle(googleApiKey).
		WithGenerativeOpenAI(openAIApiKey, openAIOrganization, "").
		WithGenerativeCohere(cohereApiKey)
	return
}
