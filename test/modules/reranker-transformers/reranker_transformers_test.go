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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func TestRerankerTransformers(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("import data", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("rerank", func(t *testing.T) {
		query := `
			{
				Get {
					Books{
						title
						_additional{
							id
							rerank(property:"description", query: "Who is the author of Dune?") {
								score
							}
						}
					}
				}
			}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		booksResponse := result.Get("Get", "Books").AsSlice()
		require.True(t, len(booksResponse) > 0)
		results, ok := booksResponse[0].(map[string]interface{})
		require.True(t, ok)
		assert.True(t, results["title"] != nil)
		assert.NotNil(t, results["_additional"])
		additional, ok := results["_additional"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, books.Dune.String(), additional["id"])
		assert.NotNil(t, additional["rerank"])
		rerank, ok := additional["rerank"].([]interface{})
		require.True(t, ok)
		score, ok := rerank[0].(map[string]interface{})
		require.True(t, ok)
		require.NotNil(t, score)
		assert.NotNil(t, score["score"])
	})
}
