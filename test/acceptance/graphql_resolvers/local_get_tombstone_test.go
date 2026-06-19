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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func gettingObjectsWithTombstones(t *testing.T) {
	className := "TombstoneTestClass"
	
	// Create class
	createObjectClass(t, &models.Class{
		Class: className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	})
	defer deleteObjectClass(t, className)

	// Add objects
	ids := []string{
		"10000000-0000-0000-0000-000000000001",
		"10000000-0000-0000-0000-000000000002",
		"10000000-0000-0000-0000-000000000003",
	}
	
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}

	for i, id := range ids {
		createObject(t, &models.Object{
			Class: className,
			ID:    strfmt.UUID(id),
			Vector: vectors[i],
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("Object %d", i),
			},
		})
	}

	// Verify all exist
	query := fmt.Sprintf(`
	{
		Get {
			%s(
				nearVector: {
					vector: [1.0, 0.0, 0.0]
					distance: 10.0
				}
			) {
				_additional { id }
				name
			}
		}
	}
	`, className)
	
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	objs := result.Get("Get", className).AsSlice()
	require.Equal(t, 3, len(objs))

	// Delete the first object (which is closest to the query vector)
	deleteObject(t, className, ids[0])

	// Immediate search - should exclude the deleted object
	// The fix in HNSW should ensure this even before tombstone cleanup
	result = graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	objs = result.Get("Get", className).AsSlice()
	require.Equal(t, 2, len(objs))
	
	for _, obj := range objs {
		item := obj.(map[string]interface{})
		additional := item["_additional"].(map[string]interface{})
		id := additional["id"].(string)
		require.NotEqual(t, ids[0], id)
	}
}
