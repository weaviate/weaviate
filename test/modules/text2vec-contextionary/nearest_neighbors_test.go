//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func Test_Text2Vec_NearestNeighbors(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateNode1Endpoint))
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	for _, book := range books.Objects() {
		helper.CreateObject(t, book)
		helper.AssertGetObjectEventually(t, book.Class, book.ID)
	}

	for _, book := range books.Objects() {
		// the list endpoint is not scoped to a class, it only resolves module
		// props if a single vectorizer is enabled, so get the objects one by one
		params := objects.NewObjectsClassGetParams().
			WithClassName(book.Class).
			WithID(book.ID).
			WithInclude(ptString("nearestNeighbors"))
		res, err := helper.Client(t).Objects.ObjectsClassGet(params, nil)
		require.NoError(t, err)

		// marshalling to JSON and back into an untyped map to make sure we assert
		// on the actual JSON structure. This way if we accidentally change the
		// goswagger generation so it affects both the client and the server in the
		// same way, this test should catch it
		b, err := json.Marshal(res.Payload)
		require.NoError(t, err)

		var untyped map[string]interface{}
		require.NoError(t, json.Unmarshal(b, &untyped))

		neighbors := untyped["additional"].(map[string]interface{})["nearestNeighbors"].(map[string]interface{})["neighbors"].([]interface{})
		require.NotEmpty(t, neighbors)
		for _, neighbor := range neighbors {
			require.NotEmpty(t, neighbor.(map[string]interface{})["concept"])
		}
	}
}

func ptString(in string) *string {
	return &in
}
