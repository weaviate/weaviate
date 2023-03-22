//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func Test_CLIP(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))
	booksClass := books.ClassCLIPVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("add data to Books schema", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("query Books data with nearText", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, `
			{
				Get {
					Books(
						limit: 1
						nearText: {
							concepts: ["Dune"]
							distance: 0.5
						}
					){
						title
						_additional {
							distance
						}
					}
				}
			}
		`)
		books := result.Get("Get", "Books").AsSlice()
		require.Len(t, books, 1)
		title := books[0].(map[string]interface{})["title"]
		assert.Equal(t, "Dune", title)
		distance := books[0].(map[string]interface{})["_additional"].(map[string]interface{})["distance"].(json.Number)
		assert.NotNil(t, distance)
		dist, err := distance.Float64()
		require.Nil(t, err)
		assert.Greater(t, dist, 0.0)
		assert.LessOrEqual(t, dist, 0.03)
	})
}
