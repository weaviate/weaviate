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
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func Test_ManyModules(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))

	t.Run("check enabled modules", func(t *testing.T) {
		meta := helper.GetMeta(t)
		require.NotNil(t, meta)

		expectedModuleNames := []string{
			"generative-cohere", "generative-palm", "generative-openai", "generative-aws", "generative-anyscale",
			"text2vec-cohere", "text2vec-contextionary", "text2vec-openai", "text2vec-huggingface",
			"text2vec-palm", "text2vec-aws", "text2vec-transformers", "qna-openai", "reranker-cohere",
		}

		modules, ok := meta.Modules.(map[string]interface{})
		require.True(t, ok)
		assert.Len(t, modules, len(expectedModuleNames))

		moduleNames := []string{}
		for name := range modules {
			moduleNames = append(moduleNames, name)
		}
		assert.ElementsMatch(t, expectedModuleNames, moduleNames)
	})

	booksClass := books.ClassContextionaryVectorizer()
	multiShardClass := multishard.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	helper.CreateClass(t, multiShardClass)
	defer helper.DeleteClass(t, booksClass.Class)
	defer helper.DeleteClass(t, multiShardClass.Class)

	t.Run("import data", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
		for _, multishard := range multishard.Objects() {
			helper.CreateObject(t, multishard)
			helper.AssertGetObjectEventually(t, multishard.Class, multishard.ID)
		}
	})

	t.Run("sanity checks", func(t *testing.T) {
		concepts := []string{
			"Frank", "Herbert", "Dune", "Book", "Project", "Hail", "Mary",
			"The Lord of the Ice Garden", "Ice Garden", "science", "fiction",
			"fantasy novel", "novelist",
		}
		checkResults := func(query string) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			books := result.Get("Get", "Books").AsSlice()
			require.True(t, len(books) > 0)
			results, ok := books[0].(map[string]interface{})
			require.True(t, ok)
			assert.True(t, results["title"] != nil)
		}

		t.Run("nearText queries", func(t *testing.T) {
			queryTemplate := `
			{
				Get {
					Books(
						nearText: {
							concepts: ["%s"]
						}
					){
						title
					}
				}
			}`
			for _, concept := range concepts {
				checkResults(fmt.Sprintf(queryTemplate, concept))
			}
		})
		t.Run("nearObject queries", func(t *testing.T) {
			queryTemplate := `
			{
				Get {
					Books(
						nearObject: {
							id: "%s"
						}
					){
						title
					}
				}
			}`
			ids := []strfmt.UUID{books.Dune, books.ProjectHailMary, books.TheLordOfTheIceGarden}
			for _, id := range ids {
				checkResults(fmt.Sprintf(queryTemplate, id))
			}
		})
		t.Run("nearVector queries", func(t *testing.T) {
			getVectors := func() []string {
				query := `
				{
					Get {
						Books(limit: 3){ _additional{ vector } }
					}
				}`
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				books := result.Get("Get", "Books").AsSlice()
				require.True(t, len(books) == 3)
				vectors := make([]string, 3)
				for i := 0; i < 3; i++ {
					results, ok := books[i].(map[string]interface{})
					require.True(t, ok)
					vector, ok := results["_additional"].(map[string]interface{})["vector"].([]interface{})
					require.True(t, ok)
					vec, err := json.Marshal(vector)
					require.Nil(t, err)
					vectors[i] = string(vec)
				}
				return vectors
			}
			vectors := getVectors()
			queryTemplate := `
			{
				Get {
					Books(
						nearVector: {
							vector: %s
						}
					){
						title
					}
				}
			}`
			for _, vector := range vectors {
				checkResults(fmt.Sprintf(queryTemplate, vector))
			}
		})
		t.Run("hybrid queries", func(t *testing.T) {
			queryTemplate := `
			{
				Get {
					Books(
						hybrid: {
							query: "%s"
						}
					){
						title
					}
				}
			}`
			for _, concept := range concepts {
				checkResults(fmt.Sprintf(queryTemplate, concept))
			}
		})
		t.Run("bm25 queries", func(t *testing.T) {
			queryTemplate := `
			{
				Get {
					Books(
						bm25:{
							query: "%s"
						}
					){
						title
					}
				}
			}`
			for _, concept := range []string{"Frank", "Project Hail Mary", "Dune", "Project", "Hail", "Mary"} {
				checkResults(fmt.Sprintf(queryTemplate, concept))
			}
		})
	})
}
