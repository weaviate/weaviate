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

package cluster_api_auth

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func ClusterAPIAuthTest(t *testing.T, compose *docker.DockerCompose) {
	t.Parallel()

	t.Run("sanity checks", func(t *testing.T) {
		t.Run("check nodes", func(t *testing.T) {
			resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 3)
		})

		className := "BooksApiAuthTest"
		booksClass := books.ClassContextionaryVectorizerWithName(className)
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("import data", func(t *testing.T) {
			client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
			helper.CreateObjectsBatch(t, client, books.ObjectsWithName(className))
		})

		t.Run("nearText query", func(t *testing.T) {
			query := fmt.Sprintf(`
			{
				Get {
					%s(
						nearText: {
							concepts: ["Frank Herbert"]
						}
					){
						title
					}
				}
			}`, booksClass.Class)
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			books := result.Get("Get", booksClass.Class).AsSlice()
			require.True(t, len(books) > 0)
			results, ok := books[0].(map[string]interface{})
			require.True(t, ok)
			require.True(t, results["title"] != nil)
		})
	})
}
