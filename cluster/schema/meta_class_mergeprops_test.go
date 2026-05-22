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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

// TestMergePropsMasked covers the field-mask path on the inner property
// merge: with no mask we get the legacy "replace every field" behavior,
// with a mask we only overwrite the listed fields, and two disjoint masked
// merges in sequence (which is what the FSM-serialised apply path produces
// from two concurrent UpdateProperty submissions) do not clobber each
// other's flag. This is the core invariant the fieldmask was added for.
func TestMergePropsMasked(t *testing.T) {
	mkBool := func(b bool) *bool { return &b }

	t.Run("nil mask is identical to MergeProps (replace all)", func(t *testing.T) {
		old := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(true),
			IndexSearchable: mkBool(true),
			Tokenization:    "word",
		}}
		next := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(false),
			IndexSearchable: mkBool(false),
			Tokenization:    "field",
		}}

		merged := MergePropsMasked(old, next, nil)
		require.Len(t, merged, 1)
		assert.False(t, *merged[0].IndexFilterable, "nil mask must replace IndexFilterable")
		assert.False(t, *merged[0].IndexSearchable, "nil mask must replace IndexSearchable")
		assert.Equal(t, "field", merged[0].Tokenization, "nil mask must replace Tokenization")
	})

	t.Run("mask restricts overwrite to listed fields only", func(t *testing.T) {
		old := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(true),
			IndexSearchable: mkBool(true),
			Tokenization:    "word",
		}}
		// Caller intends to flip IndexFilterable false and nothing else.
		// They populate the request property with the new value AND with
		// stale zeroes for the fields they did not intend to change.
		next := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(false),
			IndexSearchable: nil, // stale view; must NOT win
			Tokenization:    "",  // stale view; must NOT win
		}}

		merged := MergePropsMasked(old, next, []string{api.PropertyFieldIndexFilterable})
		require.Len(t, merged, 1)
		assert.False(t, *merged[0].IndexFilterable, "masked field must be overwritten")
		require.NotNil(t, merged[0].IndexSearchable, "unmasked field must not be cleared")
		assert.True(t, *merged[0].IndexSearchable, "unmasked field must keep its old value")
		assert.Equal(t, "word", merged[0].Tokenization, "unmasked Tokenization must keep its old value")
	})

	t.Run("two disjoint masked merges in sequence do not clobber", func(t *testing.T) {
		// Initial: both flags false. Two callers, each reading the same
		// stale view, want to flip exactly one flag. The FSM applies them
		// in sequence (serialised). Without the mask, the second apply
		// would silently reset the first one's flag back to false.
		current := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(false),
			IndexSearchable: mkBool(false),
			Tokenization:    "word",
		}}

		// Caller A flips IndexFilterable; its req carries the stale
		// IndexSearchable=false.
		reqA := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(true),
			IndexSearchable: mkBool(false),
			Tokenization:    "word",
		}}
		afterA := MergePropsMasked(current, reqA, []string{api.PropertyFieldIndexFilterable})
		require.Len(t, afterA, 1)
		assert.True(t, *afterA[0].IndexFilterable)
		assert.False(t, *afterA[0].IndexSearchable)

		// Caller B reads the SAME stale snapshot (IndexFilterable=false in
		// its mind) and wants to flip IndexSearchable. Its req carries the
		// stale IndexFilterable=false. With the mask, only IndexSearchable
		// is touched on apply, so caller A's flip survives.
		reqB := []*models.Property{{
			Name:            "title",
			DataType:        []string{"text"},
			IndexFilterable: mkBool(false), // stale
			IndexSearchable: mkBool(true),
			Tokenization:    "word",
		}}
		afterB := MergePropsMasked(afterA, reqB, []string{api.PropertyFieldIndexSearchable})
		require.Len(t, afterB, 1)
		assert.True(t, *afterB[0].IndexFilterable, "caller A's flip must NOT be clobbered by caller B")
		assert.True(t, *afterB[0].IndexSearchable, "caller B's flip must apply")
	})

	t.Run("mask never affects insertion of brand-new properties", func(t *testing.T) {
		// AddProperty path: a property that does not exist yet must be
		// added in full, regardless of mask. The mask only restricts the
		// overwrite of existing properties.
		old := []*models.Property{{
			Name:     "title",
			DataType: []string{"text"},
		}}
		next := []*models.Property{{
			Name:            "score",
			DataType:        []string{"int"},
			IndexFilterable: mkBool(true),
			Tokenization:    "word",
		}}
		// Even with a mask that does NOT include IndexFilterable, the
		// new property is inserted with its full set of fields.
		merged := MergePropsMasked(old, next, []string{api.PropertyFieldTokenization})
		require.Len(t, merged, 2)
		var added *models.Property
		for _, p := range merged {
			if p.Name == "score" {
				added = p
			}
		}
		require.NotNil(t, added)
		require.NotNil(t, added.IndexFilterable)
		assert.True(t, *added.IndexFilterable, "new property must be inserted in full, mask must not strip fields")
	})

	t.Run("empty (non-nil) mask matches the nil-mask semantics", func(t *testing.T) {
		// Defense-in-depth: an explicitly empty slice should not silently
		// turn into a no-op merge. The contract is: empty or nil means
		// "merge everything".
		old := []*models.Property{{Name: "title", DataType: []string{"text"}, IndexFilterable: mkBool(true)}}
		next := []*models.Property{{Name: "title", DataType: []string{"text"}, IndexFilterable: mkBool(false)}}

		merged := MergePropsMasked(old, next, []string{})
		require.Len(t, merged, 1)
		require.NotNil(t, merged[0].IndexFilterable)
		assert.False(t, *merged[0].IndexFilterable, "empty mask must replace every field")
	})

	t.Run("unknown mask field is ignored (forward-compat)", func(t *testing.T) {
		// A future Weaviate may introduce a new PropertyField* constant
		// that an older node doesn't recognise. With the current per-
		// field switch, an unknown tag simply doesn't match any branch
		// and the unknown field is silently skipped — the safe
		// "do nothing on this field" fallback.
		old := []*models.Property{{Name: "title", DataType: []string{"text"}, IndexFilterable: mkBool(true)}}
		next := []*models.Property{{Name: "title", DataType: []string{"text"}, IndexFilterable: mkBool(false)}}

		merged := MergePropsMasked(old, next, []string{"some-future-field-we-do-not-know-about"})
		require.Len(t, merged, 1)
		require.NotNil(t, merged[0].IndexFilterable)
		assert.True(t, *merged[0].IndexFilterable, "unknown mask tag must not match any field; old values must survive")
	})
}
