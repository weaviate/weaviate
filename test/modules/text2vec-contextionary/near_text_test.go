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
	"reflect"
	"testing"

	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func Test_Text2Vec_NearText(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateNode1Endpoint))
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
		for _, multi := range multishard.Objects() {
			helper.CreateObject(t, multi)
			helper.AssertGetObjectEventually(t, multi.Class, multi.ID)
		}
	})

	t.Run("nearText with sorting, asc", func(t *testing.T) {
		query := `
		{
			Get {
				Books(
					nearText: {concepts: ["novel"]}
					sort: [{path: ["title"], order: asc}]
				) {
					title
				}
			}
		}`

		expected := []interface{}{
			map[string]interface{}{"title": "Dune"},
			map[string]interface{}{"title": "Project Hail Mary"},
			map[string]interface{}{"title": "The Lord of the Ice Garden"},
		}
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		received := result.Get("Get", "Books").AsSlice()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf("sort objects expected = %v, received %v", expected, received)
		}
	})

	t.Run("nearText with sorting, desc", func(t *testing.T) {
		query := `
		{
			Get {
				Books(
					nearText: {concepts: ["novel"]}
					sort: [{path: ["title"], order: desc}]
				) {
					title
				}
			}
		}`

		expected := []interface{}{
			map[string]interface{}{"title": "The Lord of the Ice Garden"},
			map[string]interface{}{"title": "Project Hail Mary"},
			map[string]interface{}{"title": "Dune"},
		}
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		received := result.Get("Get", "Books").AsSlice()
		if !reflect.DeepEqual(expected, received) {
			t.Errorf("sort objects expected = %v, received %v", expected, received)
		}
	})
}
