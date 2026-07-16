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

// TestMergeProps_Tokenization pins the Tokenization-merge behavior backported
// for weaviate/0-weaviate-issues#238: a v1.38 runtime-reindex
// change-tokenization migration commits the flip as an UpdateProperty carrying
// the new tokenization. If a node is downgraded to this (patched) v1.37 and
// replays that log entry, MergeProps must apply the new tokenization —
// otherwise the property keeps its AddClass-time value and every query is
// tokenized against a mismatched index, silently returning 0 hits.
func TestMergeProps_Tokenization(t *testing.T) {
	tests := []struct {
		name               string
		old                []*models.Property
		new                []*models.Property
		expectTokenization string
		expectNestedNames  []string
	}{
		{
			// The #238 repro: replaying a v1.38-written migration must apply
			// the flipped tokenization. Red without the backport.
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
			// The no-op guard: an UpdateProperty carrying no tokenization
			// (e.g. a plain index-only update) must not wipe the stored value.
			name: "empty incoming tokenization keeps stored value",
			old: []*models.Property{{
				Name:         "title",
				Tokenization: models.PropertyTokenizationWord,
			}},
			new:                []*models.Property{{Name: "title"}},
			expectTokenization: models.PropertyTokenizationWord,
		},
		{
			// The native-v1.37 neutrality invariant the backport's safety
			// argument rests on: native v1.37 never mutates an existing
			// property's tokenization, so the incoming value always equals
			// the stored one and the merge must be a no-op. Green with and
			// without the backport by design — it pins the invariant rather
			// than reproducing #238.
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
			// MergeProps keys on strings.ToLower(Name): a differently-cased
			// incoming name must still merge into the existing property
			// instead of appending a duplicate.
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
			// The nested-merge branch replaces the merged property with a
			// copy (propCopy := *mergedProps[oldIdx]) after the tokenization
			// assignment; the flip must survive that copy.
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
