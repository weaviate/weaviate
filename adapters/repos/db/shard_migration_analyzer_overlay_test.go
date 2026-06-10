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
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestShard_MigrationAnalyzerOverlay_CallbackVisibility is the shard-layer
// regression test for weaviate/weaviate#11688.
//
// Setup: an int property with indexFilterable=false, indexRangeFilters=false,
// i.e. NO enabled inverted index — the exact property state of an
// enable-rangeable migration on a filterable-disabled property. A counting
// add-callback is registered on the property-value-index write path (the
// same registration mechanism the runtime reindex double-write callbacks
// use).
//
// Three phases:
//
//  1. Baseline (no migration overlay): a put NEVER surfaces the property to
//     the callback — the analyzer drops it (no enabled index). This holds
//     pre-fix AND post-fix and pins why the pre-fix double-write callbacks
//     were dead code: they only see what the analyzer emits.
//  2. With the migration analyzer overlay registered (the fix): a put
//     surfaces the property to the callback with HasRangeableIndex=true,
//     exactly what the rangeable double-write callback needs. Pre-fix this
//     test does not compile — registerMigrationAnalyzerOverlay is the new
//     API introduced by the fix.
//  3. After unregistering: puts stop surfacing the property (lifecycle is
//     tied to the double-write window, not the shard's lifetime).
//
// The class enables IndexNullState and IndexPropertyLength so phase 2 also
// proves the ForcedViaOverlay aux-skip: without it, the put would fail with
// "no bucket for prop 'vint' null found" because null/length buckets are
// never created for a property with no enabled index.
func TestShard_MigrationAnalyzerOverlay_CallbackVisibility(t *testing.T) {
	const propName = "vint"

	ctx := context.Background()
	className := "MigrationOverlayCallback_" + uuid.NewString()[:8]
	vFalse := false
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
		},
		Properties: []*models.Property{
			{
				Name:              propName,
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &vFalse,
				IndexSearchable:   &vFalse,
			},
			{
				// A second, regularly indexed property proves the overlay
				// does not disturb normal analysis.
				Name:     "grp",
				DataType: schema.DataTypeInt.PropString(),
			},
		},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	// Counting callback: record every (propName → snapshot of flags) the
	// write path surfaces. Same registration the double-write callbacks use.
	type seenProp struct {
		hasRangeable     bool
		forcedViaOverlay bool
	}
	seen := map[string][]seenProp{}
	disableCounting := shard.registerAddToPropertyValueIndex(
		func(_ *Shard, _ uint64, property *inverted.Property) error {
			seen[property.Name] = append(seen[property.Name], seenProp{
				hasRangeable:     property.HasRangeableIndex,
				forcedViaOverlay: property.ForcedViaOverlay,
			})
			return nil
		})
	t.Cleanup(disableCounting)

	put := func(val int64) {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					propName: val,
					"grp":    int64(1),
				},
				CreationTimeUnix:   time.Now().UnixMilli(),
				LastUpdateTimeUnix: time.Now().UnixMilli(),
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// --- Phase 1: no migration overlay — the analyzer drops the prop, the
	// callback never sees it. This is the pre-fix live-path behavior for
	// the whole migration window.
	put(100)
	require.Empty(t, seen[propName],
		"without a migration overlay the analyzer must drop the no-index prop — "+
			"the callback can never see it (this is the #11688 loss mechanism)")
	require.NotEmpty(t, seen["grp"], "regularly indexed prop must reach the callback")

	// --- Phase 2: register the migration overlay (what
	// registerDoubleWriteCallbacks now does for the task's props) and put
	// again — the callback must see the prop with HasRangeableIndex=true.
	disableOverlay := shard.registerMigrationAnalyzerOverlay(map[string]inverted.PropertyOverlay{
		propName: {ForceRangeable: true},
	})
	put(200)
	require.Len(t, seen[propName], 1,
		"with the migration overlay active the callback MUST see the migrating prop")
	require.True(t, seen[propName][0].hasRangeable,
		"the surfaced prop must carry HasRangeableIndex=true so the rangeable "+
			"double-write callback routes it to the ingest bucket")
	require.True(t, seen[propName][0].forcedViaOverlay,
		"the surfaced prop must be marked ForcedViaOverlay (no live-enabled index)")

	// --- Phase 3: unregister (disableCallbacks teardown) — puts stop
	// surfacing the prop.
	disableOverlay()
	put(300)
	require.Len(t, seen[propName], 1,
		"after unregistering, the analyzer must drop the prop again — the overlay "+
			"lifecycle is tied to the double-write window")

	// Idempotent disable must not panic or corrupt the registry.
	disableOverlay()

	// Other props were never disturbed: one entry per put.
	require.Len(t, seen["grp"], 3)
}

// TestShard_MigrationAnalyzerOverlay_MergeAndConcurrentRegistrations pins
// the registry semantics the fix relies on:
//
//   - two concurrent registrations on the same prop OR their Force* flags;
//   - unregistering one keeps the other's flags active;
//   - a Tokenization override is dropped at registration (live-path
//     tokenization is owned by the per-prop tokenization overlay, which
//     flips atomically with the bucket pointer — forcing it for the whole
//     double-write window would corrupt the still-live old buckets);
//   - an overlay with no Force* flags is a no-op registration.
func TestShard_MigrationAnalyzerOverlay_MergeAndConcurrentRegistrations(t *testing.T) {
	ctx := context.Background()
	className := "MigrationOverlayMerge_" + uuid.NewString()[:8]
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
		},
		Properties: []*models.Property{
			{Name: "vint", DataType: schema.DataTypeInt.PropString()},
		},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	require.Nil(t, shard.migrationAnalyzerOverlaySnapshot(), "steady state: no overlay")

	disableA := shard.registerMigrationAnalyzerOverlay(map[string]inverted.PropertyOverlay{
		"vint": {ForceRangeable: true},
	})
	disableB := shard.registerMigrationAnalyzerOverlay(map[string]inverted.PropertyOverlay{
		"vint": {ForceFilterable: true, Tokenization: "word"}, // Tokenization must be dropped
	})

	merged := shard.migrationAnalyzerOverlaySnapshot()
	require.Len(t, merged, 1)
	require.True(t, merged["vint"].ForceRangeable, "flags from registration A")
	require.True(t, merged["vint"].ForceFilterable, "flags from registration B OR-ed in")
	require.Empty(t, merged["vint"].Tokenization,
		"Tokenization must be stripped — live-path tokenization is owned by the "+
			"per-prop tokenization overlay, not the migration Force* registry")

	disableA()
	merged = shard.migrationAnalyzerOverlaySnapshot()
	require.False(t, merged["vint"].ForceRangeable, "A's flag gone after A unregisters")
	require.True(t, merged["vint"].ForceFilterable, "B's flag must survive A's unregister")

	disableB()
	require.Empty(t, shard.migrationAnalyzerOverlaySnapshot(), "empty after all unregister")

	// No-op registration: no Force* flags set.
	disableC := shard.registerMigrationAnalyzerOverlay(map[string]inverted.PropertyOverlay{
		"vint": {Tokenization: "field"},
	})
	require.Empty(t, shard.migrationAnalyzerOverlaySnapshot(),
		"a Tokenization-only overlay must not create a registration")
	disableC()
}
