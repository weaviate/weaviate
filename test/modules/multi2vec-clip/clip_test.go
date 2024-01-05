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

	tests := []struct {
		concept       string
		expectedTitle string
	}{
		{
			concept:       "Dune",
			expectedTitle: "Dune",
		},
		{
			concept:       "three",
			expectedTitle: "The Lord of the Ice Garden",
		},
	}
	for _, tt := range tests {
		t.Run("query Books data with nearText", func(t *testing.T) {
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(`
				{
					Get {
						Books(
							limit: 1
							nearText: {
								concepts: ["%v"]
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
			`, tt.concept))
			books := result.Get("Get", "Books").AsSlice()
			require.Len(t, books, 1)
			title := books[0].(map[string]interface{})["title"]
			assert.Equal(t, tt.expectedTitle, title)
			distance := books[0].(map[string]interface{})["_additional"].(map[string]interface{})["distance"].(json.Number)
			assert.NotNil(t, distance)
			dist, err := distance.Float64()
			require.Nil(t, err)
			assert.Greater(t, dist, 0.0)
			assert.LessOrEqual(t, dist, 0.03)
		})
	}
}
