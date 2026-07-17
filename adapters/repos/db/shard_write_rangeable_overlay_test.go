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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// filterableToRangeableRecoveryPayloadJSON builds the payload.mig content
// SaveRecoveryPayload writes in production; tests that drive migrations
// inline bypass ReindexProvider and must write it explicitly for a
// realistic on-disk shape.
func filterableToRangeableRecoveryPayloadJSON(t *testing.T, propNames ...string) []byte {
	t.Helper()
	rec := struct {
		Payload struct {
			Properties []string `json:"properties"`
		} `json:"payload"`
	}{}
	rec.Payload.Properties = propNames
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	return encoded
}

// newLiveFilterableToRangeableTask uses the real, unwrapped
// FilterableToRangeableStrategy (unlike newFilterableToRangeableTask's
// testFilterableToRangeableStrategyWrapper), so OnMigrationComplete's
// setRangeableLocallyReady side effect actually runs.
func newLiveFilterableToRangeableTask(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
	t.Helper()
	strategy := &FilterableToRangeableStrategy{
		propNames:  []string{propName},
		generation: 1,
	}
	return NewShardReindexTaskGeneric(
		"FilterableToRangeable", idx.logger, strategy, filterableToRangeableTaskConfig(className, propName),
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}

// driveFilterableToRangeableMigrationToLocalTidy drives a migration to
// full local completion (tidied.mig on disk) without touching the
// cluster-wide schema flag - that flip is RAFT-level and out of scope
// for a single-shard test (weaviate/weaviate#12189). Callers must shut
// down the returned shard.
func driveFilterableToRangeableMigrationToLocalTidy(
	t *testing.T, ctx context.Context, class *models.Class, propName string, numObjects int,
) (shard *Shard, idx *Index) {
	t.Helper()
	className := class.Class

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard = shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task := newLiveFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.NoError(t, task.SaveRecoveryPayload(shard.pathLSM(),
		filterableToRangeableRecoveryPayloadJSON(t, propName)))

	return shard, idx
}

// restartShardAfterLocalTidy simulates a real restart: a fresh *Shard
// with an empty rangeableLocalReady map, after FinalizeCompletedMigrations
// has deleted the tidied tracker dir. Deliberately skips wiring a live
// reindex task, or it would re-run the migration and mask the gap under test.
func restartShardAfterLocalTidy(t *testing.T, ctx context.Context, idx *Index, shard *Shard, class *models.Class) *Shard {
	t.Helper()
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "restarted shard init must succeed")
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)
	return shard2
}

// TestRangeableForceIndexOverlay pins that once a shard is locally ready
// for a rangeable prop but the schema flag hasn't flipped yet, writes
// must be forced to analyze as rangeable (weaviate/weaviate#12189).
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
		// repair-rangeable's submit-time validator requires the flag already
		// true, so this exercises the same case for both local-ready states.
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
// writeAnalyzerOverlay unions the tokenization and rangeable-force
// overlays without either clobbering the other, since a real write can
// hit both during concurrent, disjoint migrations on the same class.
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

// TestRangeableForceIndexOverlay_FastExit_FreshShard pins that a shard
// with no migration history takes the zero-alloc fast exit without
// calling IsRangeableLocallyReady; the allocation claim itself is pinned
// by BenchmarkRangeableForceIndexOverlay_SteadyState.
func TestRangeableForceIndexOverlay_FastExit_FreshShard(t *testing.T) {
	ctx := testCtx()
	className := "RangeableOverlayFastExit_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	require.Empty(t, shard.rangeableLocalReady,
		"sanity: a fresh shard with no migration history must have an empty rangeableLocalReady map")
	require.False(t, shard.rangeableLocalReadyHistoryUnknown.Load())

	falseVal := false
	untouched := &models.Property{Name: "score", DataType: []string{"int"}, IndexRangeFilters: &falseVal}
	require.Nil(t, shard.rangeableForceIndexOverlay([]*models.Property{untouched}))
}

// TestRangeableForceIndexOverlay_SurvivesRestartAfterLocalTidyBeforeSchemaFlip
// pins that a restart between local-tidy completion and the schema flip
// (weaviate/weaviate#12189) must not reopen the write-loss window: the
// on-disk tracker is gone by then, so a naive `len(rangeableLocalReady)==0`
// fast exit would need seedRangeableLocalReadyFromMigrationHistory to have
// already seeded `true`.
func TestRangeableForceIndexOverlay_SurvivesRestartAfterLocalTidyBeforeSchemaFlip(t *testing.T) {
	ctx := testCtx()
	className := "RangeableOverlayRestart_" + uuid.NewString()[:8]
	const propName = filterableToRangeablePropName
	class := newFilterableToRangeableTestClass(className)

	shard, idx := driveFilterableToRangeableMigrationToLocalTidy(t, ctx, class, propName, 10)

	falseVal := false
	preFlipProp := &models.Property{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &falseVal}

	overlay := shard.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
	require.NotNil(t, overlay, "sanity: pre-restart overlay should already force rangeable")
	require.True(t, overlay[propName].ForceRangeable)

	shard2 := restartShardAfterLocalTidy(t, ctx, idx, shard, class)
	defer shard2.Shutdown(ctx)

	overlay2 := shard2.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
	require.NotNilf(t, overlay2, "restart must not reopen the post-swap pre-flip write-loss window: "+
		"this shard's migration history says property %q's rangeable bucket is tidied, so the write "+
		"overlay must still force rangeable even though the in-memory map was wiped by the restart",
		propName)
	require.True(t, overlay2[propName].ForceRangeable)
}

// newSeedHistoryTestShardWithTracker leaves exactly one tracker dir on
// disk with a parseable payload.mig (written explicitly since driving the
// migration inline bypasses ReindexProvider).
func newSeedHistoryTestShardWithTracker(t *testing.T, ctx context.Context, namePrefix, propName string) *Shard {
	t.Helper()
	className := namePrefix + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	task := newLiveFilterableToRangeableTask(t, shard.index, className, propName)
	require.NoError(t, task.SaveRecoveryPayload(shard.pathLSM(),
		filterableToRangeableRecoveryPayloadJSON(t, propName)))
	return shard
}

// TestSeedRangeableLocalReadyFromMigrationHistory unit-tests
// seedRangeableLocalReadyFromMigrationHistory against a hand-built
// on-disk tracker, isolated from full migration/restart integration.
func TestSeedRangeableLocalReadyFromMigrationHistory(t *testing.T) {
	ctx := testCtx()

	t.Run("tidied tracker with parseable payload seeds true", func(t *testing.T) {
		const propName = "score"
		shard := newSeedHistoryTestShardWithTracker(t, ctx, "SeedHistoryTidied_", propName)
		dirs := rangeableMigrationTrackerDirs(shard)
		require.Len(t, dirs, 1)
		require.NoError(t, os.WriteFile(filepath.Join(dirs[0], "tidied.mig"), nil, 0o644))

		require.Empty(t, shard.rangeableLocalReady, "sanity: nothing seeded yet")

		seedRangeableLocalReadyFromMigrationHistory(shard)

		require.Equal(t, map[string]bool{propName: true}, shard.rangeableLocalReady)
		require.False(t, shard.rangeableLocalReadyHistoryUnknown.Load())
	})

	t.Run("in-flight (non-tidied) tracker with parseable payload also seeds true (markInFlightRangeableMigrationsNotReady corrects it later)", func(t *testing.T) {
		const propName = "score"
		shard := newSeedHistoryTestShardWithTracker(t, ctx, "SeedHistoryInFlight_", propName)

		seedRangeableLocalReadyFromMigrationHistory(shard)
		require.Equal(t, map[string]bool{propName: true}, shard.rangeableLocalReady,
			"seed is optimistic by design; markInFlightRangeableMigrationsNotReady (which runs "+
				"AFTER FinalizeCompletedMigrations in NewShard) is the authoritative pass that "+
				"corrects still-in-flight properties back to false")

		markInFlightRangeableMigrationsNotReady(shard)
		require.Equal(t, map[string]bool{propName: false}, shard.rangeableLocalReady)
	})

	t.Run("unparseable payload disables the fast-exit flag instead of guessing property names", func(t *testing.T) {
		className := "SeedHistoryUnparseable_" + uuid.NewString()[:8]
		const propName = "score"
		class := newFilterableToRangeableTestClass(className)
		shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
			false, false, false)
		shard := shd.(*Shard)
		defer shard.Shutdown(ctx)

		task := newLiveFilterableToRangeableTask(t, shard.index, className, propName)
		require.NoError(t, task.SaveRecoveryPayload(shard.pathLSM(), []byte("not valid json")))
		dirs := rangeableMigrationTrackerDirs(shard)
		require.Len(t, dirs, 1)
		require.NoError(t, os.WriteFile(filepath.Join(dirs[0], "tidied.mig"), nil, 0o644))

		seedRangeableLocalReadyFromMigrationHistory(shard)

		require.Empty(t, shard.rangeableLocalReady,
			"no property name could be recovered, so nothing should be guessed into the map")
		require.True(t, shard.rangeableLocalReadyHistoryUnknown.Load())

		// Disabled shard-wide, not just for the affected property:
		// rangeableForceIndexOverlay can't know which properties an
		// unparseable tracker might have named.
		require.False(t, len(shard.rangeableLocalReady) == 0 && !shard.rangeableLocalReadyHistoryUnknown.Load(),
			"fast-exit precondition must be false so rangeableForceIndexOverlay takes the slow path")

		falseVal := false
		other := &models.Property{Name: "unrelated_prop", DataType: []string{"int"}, IndexRangeFilters: &falseVal}
		require.Nil(t, shard.rangeableForceIndexOverlay([]*models.Property{other}),
			"the slow path still correctly answers 'not ready' for a genuinely unrelated property")
	})

	t.Run("no .migrations dir: no-op", func(t *testing.T) {
		className := "SeedHistoryNone_" + uuid.NewString()[:8]
		class := newFilterableToRangeableTestClass(className)
		shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
			false, false, false)
		shard := shd.(*Shard)
		defer shard.Shutdown(ctx)

		require.NotPanics(t, func() { seedRangeableLocalReadyFromMigrationHistory(shard) })
		require.Empty(t, shard.rangeableLocalReady)
		require.False(t, shard.rangeableLocalReadyHistoryUnknown.Load())
	})
}
