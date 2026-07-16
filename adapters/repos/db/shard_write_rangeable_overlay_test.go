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

package db

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRangeableForceIndexOverlay pins the write-path overlay that closes
// the #12189-introduced write-loss window (weaviate/0-weaviate-issues#319,
// rangeable instance): once a shard is locally ready for a rangeable prop
// but the live schema flag hasn't flipped yet, writes to that prop must be
// forced to analyze as rangeable so they land in the already-canonical
// bucket. See [Shard.rangeableForceIndexOverlay] for the mechanism.
func TestRangeableForceIndexOverlay(t *testing.T) {
	const propName = "score"
	ctx := testCtx()
	className := "RangeableForceOverlay_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	falseVal, trueVal := false, true
	preFlipProp := &models.Property{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &falseVal}
	postFlipProp := &models.Property{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &trueVal}

	t.Run("not locally ready, flag false: no overlay (still mid-migration, double-write covers it)", func(t *testing.T) {
		shard.setRangeableLocallyReady(propName, false)
		overlay := shard.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
		require.Nil(t, overlay, "no overlay should fire before this shard's local swap")
	})

	t.Run("locally ready, flag still false: overlay forces rangeable (the write-loss window)", func(t *testing.T) {
		shard.setRangeableLocallyReady(propName, true)
		overlay := shard.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
		require.NotNil(t, overlay)
		require.True(t, overlay[propName].ForceRangeable,
			"a write in the post-swap pre-flip window must be forced to analyze as rangeable")
	})

	t.Run("locally ready, flag already true: no overlay (cluster flip landed, ordinary path covers it)", func(t *testing.T) {
		shard.setRangeableLocallyReady(propName, true)
		overlay := shard.rangeableForceIndexOverlay([]*models.Property{postFlipProp})
		require.Nil(t, overlay,
			"once the live schema already says rangeable, the overlay must self-limit and stop firing")
	})

	t.Run("repair-rangeable analog: flag true from the start, never fires regardless of local-ready", func(t *testing.T) {
		// repair-rangeable's submit-time validator enforces the flag is
		// already true before the migration starts, so this is the same
		// case as "flag already true" above, exercised for both
		// local-ready states to document the invariant explicitly.
		shard.setRangeableLocallyReady(propName, false)
		require.Nil(t, shard.rangeableForceIndexOverlay([]*models.Property{postFlipProp}))
		shard.setRangeableLocallyReady(propName, true)
		require.Nil(t, shard.rangeableForceIndexOverlay([]*models.Property{postFlipProp}))
	})

	t.Run("never-migrated property: bucket-existence fallback keeps it false, no overlay", func(t *testing.T) {
		other := &models.Property{Name: "untouched", DataType: []string{"int"}, IndexRangeFilters: &falseVal}
		overlay := shard.rangeableForceIndexOverlay([]*models.Property{other})
		require.Nil(t, overlay, "a property with no rangeable bucket and no explicit ready entry must default to not-ready")
	})

	t.Run("nil props are skipped without panicking", func(t *testing.T) {
		require.NotPanics(t, func() {
			shard.rangeableForceIndexOverlay([]*models.Property{nil, preFlipProp})
		})
	})
}

// TestWriteAnalyzerOverlayMergesTokenizationAndRangeable pins that
// [Shard.writeAnalyzerOverlay] unions the tokenization overlay (text
// props, weaviate/0-weaviate-issues#240) and the rangeable force overlay
// (numeric props, weaviate/0-weaviate-issues#319) without either
// clobbering the other, since a real write can hit both simultaneously
// during two concurrent-but-disjoint migrations on the same class.
func TestWriteAnalyzerOverlayMergesTokenizationAndRangeable(t *testing.T) {
	const (
		textProp    = "title"
		rangeProp   = "score"
		tokenTarget = "word"
	)
	ctx := testCtx()
	className := "WriteOverlayMerge_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	class.Properties = append(class.Properties, &models.Property{
		Name:     textProp,
		DataType: []string{"text"},
	})

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	falseVal := false
	rangeableProp := &models.Property{Name: rangeProp, DataType: []string{"int"}, IndexRangeFilters: &falseVal}
	textPropLive := &models.Property{Name: textProp, DataType: []string{"text"}, Tokenization: "whitespace"}

	shard.setRangeableLocallyReady(rangeProp, true)
	shard.SetTokenizationOverlay(textProp, tokenTarget)

	merged := shard.writeAnalyzerOverlay([]*models.Property{rangeableProp, textPropLive})
	require.Len(t, merged, 2)
	require.True(t, merged[rangeProp].ForceRangeable)
	require.Equal(t, tokenTarget, merged[textProp].Tokenization)
	require.False(t, merged[textProp].ForceRangeable,
		"the tokenization overlay entry must not pick up the rangeable Force flag")
}
