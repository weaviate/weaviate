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
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/go-openapi/strfmt"
)

// TestObjects_PatchKeepsPropertyLengthFilterCurrent proves that a PATCH whose
// analyzed terms are unchanged but whose raw value length changes still
// updates the len() where-filter, rather than leaving it pointing at the
// pre-patch value. It also proves that an object lacking a property matches
// len() == 0 with only IndexPropertyLength on, through a PATCH that adds the
// property and a PUT that drops it.
func TestObjects_PatchKeepsPropertyLengthFilterCurrent(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithWeaviate().Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	properties := []*models.Property{
		{
			Name:         "text",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		},
		{
			Name:     "counts",
			DataType: schema.DataTypeIntArray.PropString(),
		},
	}

	className := "PatchLengthFilter"
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexNullState:      true,
			IndexPropertyLength: true,
		},
		Properties: properties,
	})
	defer helper.DeleteClass(t, className)

	// Without IndexNullState, so a missing property gets its length-0 entry
	// from IndexPropertyLength alone.
	missingClassName := "MissingLengthFilter"
	helper.CreateClass(t, &models.Class{
		Class:      missingClassName,
		Vectorizer: "none",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexPropertyLength: true,
		},
		Properties: properties,
	})
	defer helper.DeleteClass(t, missingClassName)

	tests := []struct {
		name      string
		property  string
		before    interface{}
		after     interface{}
		beforeLen int
		afterLen  int
	}{
		{
			name:      "punctuation-only edit keeps the token but grows the raw length",
			property:  "text",
			before:    "alpha",
			after:     "alpha!",
			beforeLen: 5,
			afterLen:  6,
		},
		{
			name:      "whitespace-only edit keeps zero tokens but grows the raw length from empty",
			property:  "text",
			before:    "",
			after:     " ",
			beforeLen: 0,
			afterLen:  1,
		},
		{
			name:      "duplicate-collapsing array edit keeps the term set but grows the element count",
			property:  "counts",
			before:    []int{1, 2},
			after:     []int{1, 2, 2},
			beforeLen: 2,
			afterLen:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := strfmt.UUID(uuid.New().String())
			require.NoError(t, helper.CreateObject(t, &models.Object{
				Class:      className,
				ID:         id,
				Properties: map[string]interface{}{tc.property: tc.before},
			}))
			defer helper.DeleteObject(t, &models.Object{Class: className, ID: id})

			assertLenFilterCount(t, className, tc.property, tc.beforeLen, 1)
			assertLenFilterCount(t, className, tc.property, tc.afterLen, 0)

			require.NoError(t, helper.PatchObject(t, &models.Object{
				Class:      className,
				ID:         id,
				Properties: map[string]interface{}{tc.property: tc.after},
			}))

			assertLenFilterCount(t, className, tc.property, tc.afterLen, 1)
			assertLenFilterCount(t, className, tc.property, tc.beforeLen, 0)
		})
	}

	t.Run("missing property matches len() == 0 with only IndexPropertyLength", func(t *testing.T) {
		missingID := strfmt.UUID(uuid.New().String())
		emptyID := strfmt.UUID(uuid.New().String())
		presentID := strfmt.UUID(uuid.New().String())

		create := func(id strfmt.UUID, props map[string]interface{}) {
			t.Helper()
			require.NoError(t, helper.CreateObject(t, &models.Object{
				Class:      missingClassName,
				ID:         id,
				Properties: props,
			}))
			t.Cleanup(func() { helper.DeleteObject(t, &models.Object{Class: missingClassName, ID: id}) })
		}
		create(missingID, map[string]interface{}{"text": "missing"})
		create(emptyID, map[string]interface{}{"text": "empty", "counts": []int{}})
		create(presentID, map[string]interface{}{"text": "present", "counts": []int{1}})

		require.ElementsMatch(t, []strfmt.UUID{missingID, emptyID},
			lenFilterIDs(t, missingClassName, "counts", 0), "after insert")
		require.ElementsMatch(t, []strfmt.UUID{presentID},
			lenFilterIDs(t, missingClassName, "counts", 1), "after insert")

		// PATCH merges, so it can add counts but not drop it.
		require.NoError(t, helper.PatchObject(t, &models.Object{
			Class:      missingClassName,
			ID:         missingID,
			Properties: map[string]interface{}{"counts": []int{2}},
		}))
		// PUT replaces, so leaving counts out drops it.
		require.NoError(t, helper.UpdateObject(t, &models.Object{
			Class:      missingClassName,
			ID:         presentID,
			Properties: map[string]interface{}{"text": "present"},
		}))

		require.ElementsMatch(t, []strfmt.UUID{presentID, emptyID},
			lenFilterIDs(t, missingClassName, "counts", 0),
			"after a PATCH adds counts and a PUT drops it")
		require.ElementsMatch(t, []strfmt.UUID{missingID},
			lenFilterIDs(t, missingClassName, "counts", 1),
			"after a PATCH adds counts and a PUT drops it")
	})
}

func assertLenFilterCount(t *testing.T, className, property string, length, expectedCount int) {
	t.Helper()
	require.Len(t, lenFilterIDs(t, className, property, length), expectedCount,
		"len(%s) = %d should match %d object(s)", property, length, expectedCount)
}

func lenFilterIDs(t *testing.T, className, property string, length int) []strfmt.UUID {
	t.Helper()
	query := fmt.Sprintf(`{
		Get {
			%s(where: {path: ["len(%s)"], operator: Equal, valueInt: %d}) {
				_additional { id }
			}
		}
	}`, className, property, length)

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	objs := result.Get("Get", className).AsSlice()
	ids := make([]strfmt.UUID, len(objs))
	for i, obj := range objs {
		ids[i] = strfmt.UUID(obj.(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string))
	}
	return ids
}
