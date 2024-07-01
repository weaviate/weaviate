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
	googleApiKey := os.Getenv("PALM_APIKEY")
	if googleApiKey == "" {
		t.Skip("skipping, PALM_APIKEY environment variable not present")
	}
	// OpenAI
	openAIApiKey := os.Getenv("OPENAI_APIKEY")
	if openAIApiKey == "" {
		t.Skip("skipping, OPENAI_APIKEY environment variable not present")
	}
	openAIOrganization := os.Getenv("OPENAI_ORGANIZATION")
	if openAIApiKey == "" {
		t.Skip("skipping, OPENAI_ORGANIZATION environment variable not present")
	}
	// Cohere
	cohereApiKey := os.Getenv("COHERE_APIKEY")
	if cohereApiKey == "" {
		t.Skip("skipping, COHERE_APIKEY environment variable not present")
	}
	ctx := context.Background()
	compose, err := createSingleNodeEnvironment(ctx, accessKey, secretKey, sessionToken,
		openAIApiKey, openAIOrganization, googleApiKey, cohereApiKey,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	endpoint := compose.GetWeaviate().URI()

	t.Run("tests", testGenerativeManyModules(endpoint, region, gcpProject))
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
		WithWeaviateEnv("EXPERIMENTAL_DYNAMIC_RAG_SYNTAX", "true").
		Start(ctx)
	return
}

func composeModules(accessKey, secretKey, sessionToken string,
	openAIApiKey, openAIOrganization string,
	googleApiKey, cohereApiKey string,
) (composeModules *docker.Compose) {
	composeModules = docker.New().
		WithText2VecTransformers().
		WithText2VecAWS(accessKey, secretKey, sessionToken).
		WithGenerativeAWS(accessKey, secretKey, sessionToken).
		WithGenerativeOpenAI(openAIApiKey, openAIOrganization, "").
		WithGenerativePaLM(googleApiKey).
		WithGenerativeCohere(cohereApiKey).
		WithGenerativeOllama()
	return
}
