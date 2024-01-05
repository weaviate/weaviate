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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestClusterAPIAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateClusterWithBasicAuth("user", "pass").
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("sanity checks", func(t *testing.T) {
		t.Run("check nodes", func(t *testing.T) {
			resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 2)
		})

		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("import data", func(t *testing.T) {
			helper.CreateObjectsBatch(t, books.Objects())
		})

		t.Run("nearText query", func(t *testing.T) {
			query := `
			{
				Get {
					Books(
						nearText: {
							concepts: ["Frank Herbert"]
						}
					){
						title
					}
				}
			}`
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			books := result.Get("Get", "Books").AsSlice()
			require.True(t, len(books) > 0)
			results, ok := books[0].(map[string]interface{})
			require.True(t, ok)
			assert.True(t, results["title"] != nil)
		})
	})
}
