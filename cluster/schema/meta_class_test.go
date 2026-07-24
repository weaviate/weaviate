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

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestMergeProps_Tokenization pins weaviate/0-weaviate-issues#238: a downgraded
// v1.37 node replaying a v1.38 tokenization migration must apply it, not
// silently keep the stale value and return 0 hits.
func TestMergeProps_Tokenization(t *testing.T) {
	tests := []struct {
		name               string
		old                []*models.Property
		new                []*models.Property
		expectTokenization string
		expectNestedNames  []string
	}{
		{
			// #238 repro: replaying a v1.38 migration must flip the tokenization.
			name: "migration flips tokenization",
			old: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			}},
			new: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationField,
			}},
			expectTokenization: models.PropertyTokenizationField,
		},
		{
			// An index-only update (no tokenization) must not wipe the stored value.
			name: "empty incoming tokenization keeps stored value",
			old: []*models.Property{{
				Name:         "title",
				Tokenization: models.PropertyTokenizationWord,
			}},
			new:                []*models.Property{{Name: "title"}},
			expectTokenization: models.PropertyTokenizationWord,
		},
		{
			// Invariant the backport relies on: native v1.37 never changes
			// tokenization, so this merge is always a no-op.
			name: "same incoming tokenization is a no-op",
			old: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			}},
			new: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			}},
			expectTokenization: models.PropertyTokenizationWord,
		},
		{
			// A differently-cased incoming name must still merge, not duplicate.
			name: "case-insensitive name match merges tokenization",
			old: []*models.Property{{
				Name:         "Title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationWord,
			}},
			new: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: models.PropertyTokenizationField,
			}},
			expectTokenization: models.PropertyTokenizationField,
		},
		{
			// The tokenization flip must survive the nested-merge copy path.
			name: "tokenization flip survives copy-on-nested-merge",
			old: []*models.Property{{
				Name:         "info",
				DataType:     []string{"object"},
				Tokenization: models.PropertyTokenizationWord,
				NestedProperties: []*models.NestedProperty{
					{Name: "street", DataType: []string{"text"}},
				},
			}},
			new: []*models.Property{{
				Name:         "info",
				DataType:     []string{"object"},
				Tokenization: models.PropertyTokenizationField,
				NestedProperties: []*models.NestedProperty{
					{Name: "zip", DataType: []string{"text"}},
				},
			}},
			expectTokenization: models.PropertyTokenizationField,
			expectNestedNames:  []string{"street", "zip"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := MergeProps(tt.old, tt.new)

			require.Len(t, merged, 1, "matching names must merge, not append")
			require.Equal(t, tt.expectTokenization, merged[0].Tokenization)

			if tt.expectNestedNames != nil {
				nestedNames := make([]string, 0, len(merged[0].NestedProperties))
				for _, np := range merged[0].NestedProperties {
					nestedNames = append(nestedNames, np.Name)
				}
				require.ElementsMatch(t, tt.expectNestedNames, nestedNames)
			}
		})
	}
}
