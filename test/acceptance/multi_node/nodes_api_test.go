package multi_node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestNodesMultiNode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	// helper.SetupClient("127.0.0.1:8080")
	paragraphClass := articles.ParagraphsClass()
	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)
	articleClass := articles.ArticlesClass()
	helper.DeleteClass(t, articleClass.Class)
	helper.CreateClass(t, articleClass)

	for i := 0; i < 10; i++ {
		require.NoError(t, helper.CreateObject(t, articles.NewArticle().Object()))
		require.NoError(t, helper.CreateObject(t, articles.NewParagraph().Object()))
	}

	minimal, verbose := verbosity.OutputMinimal, verbosity.OutputVerbose

	t.Run("output without class minimal", func(t *testing.T) {
		payload := getNodesPayload(t, minimal, "")
		for _, node := range payload.Nodes {
			require.Nil(t, node.Shards)
		}
	})

	t.Run("output without class verbose", func(t *testing.T) {
		payload := getNodesPayload(t, verbose, "")
		for _, node := range payload.Nodes {
			require.NotNil(t, node.Shards)
			require.Len(t, node.Shards, 2)
		}
	})

	t.Run("output with class minimal", func(t *testing.T) {
		payload := getNodesPayload(t, minimal, articleClass.Class)
		for _, node := range payload.Nodes {
			require.Nil(t, node.Shards)
		}
	})

	t.Run("output with class verbose", func(t *testing.T) {
		payload := getNodesPayload(t, verbose, articleClass.Class)
		for _, node := range payload.Nodes {
			require.NotNil(t, node.Shards)
			require.Len(t, node.Shards, 1)
		}
	})
}

func getNodesPayload(t *testing.T, verbosity string, class string) *models.NodesStatusResponse {
	params := nodes.NewNodesGetClassParams().WithOutput(&verbosity)
	if class != "" {
		params.WithClassName(class)
	}
	body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
	require.NoError(t, clientErr)
	payload, err := body.Payload, clientErr
	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Len(t, payload.Nodes, 3)
	return payload
}
