//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/books"
	"github.com/stretchr/testify/assert"
)

func Test_T2VTransformers(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))
	booksClass := books.ClassTransformersVectorizer()
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
						nearText: {
							concepts: ["Frank Herbert"]
							distance: 0.7
						}
					){
						title
					}
				}
			}
		`)
		books := result.Get("Get", "Books").AsSlice()
		expected := []interface{}{
			map[string]interface{}{"title": "Dune"},
		}
		assert.ElementsMatch(t, expected, books)
	})
}
