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
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/books"
	"github.com/stretchr/testify/assert"
)

func Test_QnATransformers(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateEndpoint))
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)
	// Text2VecTransformers
	booksTransformers := "BooksTransformers"
	booksTransformersClass := books.ClassTransformersVectorizerWithName(booksTransformers)
	helper.CreateClass(t, booksTransformersClass)
	defer helper.DeleteClass(t, booksTransformersClass.Class)

	t.Run("add data to Books schema", func(t *testing.T) {
		bookObjects := []*models.Object{}
		bookObjects = append(bookObjects, books.Objects()...)
		bookObjects = append(bookObjects, books.ObjectsWithName(booksTransformers)...)
		for _, book := range bookObjects {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
	})

	t.Run("ask", func(t *testing.T) {
		for _, class := range []*models.Class{booksClass, booksTransformersClass} {
			t.Run(class.Class, func(t *testing.T) {
				query := `
					{
						Get {
							%s(
								ask: {
									question: "Who is Dune's author?"
								}
								limit: 1
							){
								title
								_additional {
									answer {
										hasAnswer
										property
										result
										startPosition
										endPosition
									}
								}
							}
						}
					}
				`
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, class.Class))
				books := result.Get("Get", class.Class).AsSlice()
				expected := []interface{}{
					map[string]interface{}{
						"title": "Dune",
						"_additional": map[string]interface{}{
							"answer": map[string]interface{}{
								"endPosition":   json.Number("74"),
								"hasAnswer":     true,
								"property":      "description",
								"result":        "frank herbert",
								"startPosition": json.Number("61"),
							},
						},
					},
				}
				assert.ElementsMatch(t, expected, books)
			})
		}
	})
}
