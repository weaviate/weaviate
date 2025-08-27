package acceptance_with_go_client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
)

func createClientWithClassName(t *testing.T) (*client.Client, string) {
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	ctx := context.Background()
	className := t.Name()
	require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

	return c, className
}
