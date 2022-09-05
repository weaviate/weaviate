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

func Test_SUMTransformers(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))
	booksClass := books.ClassContextionaryVectorizer()
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
					Books{
						title
						_additional {
							summary (properties:["description"]) {
								property
								result
							}
						}
					}
				}
			}
		`)
		books := result.Get("Get", "Books").AsSlice()
		expected := []interface{}{
			map[string]interface{}{
				"title": "Dune",
				"_additional": map[string]interface{}{
					"summary": map[string]interface{}{
						"property": "description",
						"result":   "Dune is a 1965 epic science fiction novel by American author Frank Herbert.It is the first novel in the Dune series by Frank Herbert, and the first in the \"Dune\" series of books.It was published in the United States by Simon & Schuster in 1965.",
					},
				},
			},
		}
		assert.ElementsMatch(t, expected, books)
	})
}
