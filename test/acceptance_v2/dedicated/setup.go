package dedicated

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

func SetupDedicated(t *testing.T, setup func(*testing.T) *docker.Compose) *docker.DockerCompose {
	t.Helper()

	clusterSetup := setup(t)
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
	return compose
}
