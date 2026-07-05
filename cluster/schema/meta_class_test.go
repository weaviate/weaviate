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

// TestMergeProps_MergesTokenization pins weaviate/0-weaviate-issues#238: a
// v1.38 runtime-reindex change-tokenization migration commits the flip as an
// UpdateProperty carrying the new tokenization. If a node is downgraded to this
// (patched) v1.37 and replays that log entry, MergeProps must apply the new
// tokenization — otherwise the property keeps its AddClass-time value and every
// query is tokenized against a mismatched index, silently returning 0 hits.
func TestMergeProps_MergesTokenization(t *testing.T) {
	old := []*models.Property{{
		Name:         "title",
		DataType:     []string{"text"},
		Tokenization: models.PropertyTokenizationWord,
	}}
	updated := []*models.Property{{
		Name:         "title",
		DataType:     []string{"text"},
		Tokenization: models.PropertyTokenizationField,
	}}

	merged := MergeProps(old, updated)

	require.Len(t, merged, 1)
	require.Equal(t, models.PropertyTokenizationField, merged[0].Tokenization,
		"the migrated tokenization must be merged so a downgraded node reads it")
}

// TestMergeProps_KeepsTokenizationWhenIncomingEmpty guards the backport's
// no-op-for-native-v1.37 property: an UpdateProperty that carries no
// tokenization (e.g. a plain index-only update) must not wipe the stored value.
func TestMergeProps_KeepsTokenizationWhenIncomingEmpty(t *testing.T) {
	old := []*models.Property{{
		Name:         "title",
		Tokenization: models.PropertyTokenizationWord,
	}}
	updated := []*models.Property{{Name: "title"}}

	merged := MergeProps(old, updated)

	require.Len(t, merged, 1)
	require.Equal(t, models.PropertyTokenizationWord, merged[0].Tokenization,
		"an UpdateProperty with no tokenization must not clear the existing one")
}
