package acceptance_v2

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/acceptance_v2/shared/cluster_api_auth"
	"github.com/weaviate/weaviate/test/acceptance_v2/shared/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAcceptanceSharedCluster(t *testing.T) {
	t.Parallel()

	clusterSetup := docker.New().With3NodeCluster().WithText2VecOpenAI("", "", "").WithWeaviateBasicAuth("user", "pass").WithText2VecContextionary()
	if os.Getenv("CI") != "" {
		image := os.Getenv("WEAVIATE_IMAGE")
		if image == "" {
			t.Fatalf("WEAVIATE_IMAGE is not set but CI is set")
		}
		clusterSetup = clusterSetup.WithWeaviateImage(image)
	}

	compose, err := clusterSetup.Start(t.Context())
	require.Nil(t, err)
	require.NotNil(t, compose)

	t.Cleanup(func() {
		// Can't reuse test context here as it's already expired
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	})
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("SharedClusterAPI", func(t *testing.T) { cluster_api_auth.ClusterAPIAuthTest(t, compose) })
	t.Run("SharedSchema", func(t *testing.T) { schema.TestSharedSchema(t, compose) })
}
