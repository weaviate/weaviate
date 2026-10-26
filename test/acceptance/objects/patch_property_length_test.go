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
// pre-patch value.
func TestObjects_PatchKeepsPropertyLengthFilterCurrent(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithWeaviate().Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	className := "PatchLengthFilter"
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			IndexNullState:      true,
			IndexPropertyLength: true,
		},
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "counts",
				DataType: schema.DataTypeIntArray.PropString(),
			},
		},
	})
	defer helper.DeleteClass(t, className)

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
}

func assertLenFilterCount(t *testing.T, className, property string, length, expectedCount int) {
	t.Helper()
	query := fmt.Sprintf(`{
		Get {
			%s(where: {path: ["len(%s)"], operator: Equal, valueInt: %d}) {
				_additional { id }
			}
		}
	}`, className, property, length)

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	require.Len(t, result.Get("Get", className).AsSlice(), expectedCount,
		"len(%s) = %d should match %d object(s)", property, length, expectedCount)
}
