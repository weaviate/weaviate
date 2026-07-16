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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// filterableToRangeableRecoveryPayloadJSON builds the payload.mig
// content [ShardReindexTaskGeneric.SaveRecoveryPayload] writes in
// production (via ReindexProvider.persistRecoveryRecord, the
// RAFT/DTM-driven submission path), the shape
// [readRecoveryPropertyNames] parses. Driving a migration inline via
// direct OnAfterLsmInit/OnAfterLsmInitAsync calls (as the recovery-
// convergence tests and newLiveFilterableToRangeableTask do) never
// goes through ReindexProvider, so it never writes payload.mig on its
// own; tests that need a realistic on-disk payload.mig must write it
// explicitly, same as this helper.
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

// newLiveFilterableToRangeableTask is the same wiring as
// newFilterableToRangeableTask (inverted_reindex_recovery_filterable_to_rangeable_test.go)
// EXCEPT it uses the real, unwrapped FilterableToRangeableStrategy
// instead of testFilterableToRangeableStrategyWrapper. That wrapper
// intentionally skips the production OnMigrationComplete's
// setRangeableLocallyReady side effect (by design; see its own
// godoc, it exists for bucket-content fingerprint tests that don't
// care about the query-path optimization flag). This helper is for
// tests that specifically DO care about that flag, so it must run
// the real hook.
func newLiveFilterableToRangeableTask(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
	t.Helper()
	strategy := &FilterableToRangeableStrategy{
		propNames:  []string{propName},
		generation: 1,
	}
	selectedProps := map[string]struct{}{propName: {}}
	cfg := reindexTaskConfig{
		swapBuckets:                   true,
		tidyBuckets:                   true,
		concurrency:                   2,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,

		selectionEnabled: true,
		selectedPropsByCollection: map[string]map[string]struct{}{
			className: selectedProps,
		},
		selectedShardsByCollection: map[string]map[string]struct{}{
			className: nil, // nil = all shards
		},
	}
	return NewShardReindexTaskGeneric(
		"FilterableToRangeable", idx.logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}

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

// TestRangeableForceIndexOverlay_FastExit_FreshShard pins the
// steady-state fast exit added to close the +15-allocs/op regression
// (rangeableForceIndexOverlay running the per-prop
// IsRangeableLocallyReady bucket-existence lookup unconditionally on
// every write, even for a shard that has never had a rangeable
// migration). A brand-new shard's rangeableLocalReady map is empty
// and rangeableLocalReadyHistoryUnknown is false, so the fast exit
// must return nil without ever calling IsRangeableLocallyReady; this
// test only pins the observable output (nil overlay), the allocation
// claim itself is pinned by BenchmarkRangeableForceIndexOverlay_SteadyState.
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
// is the regression test for the fast-exit safety edge QA flagged: the
// bucket-existence fallback in IsRangeableLocallyReady. It reproduces
// the exact restart timing that makes a naive
// `len(rangeableLocalReady)==0` fast exit unsafe.
//
// Sequence: drive an enable-rangeable migration to full local
// completion (tidied.mig written) WITHOUT ever touching the
// cluster-wide schema flag (that flip is RAFT-level, out of scope for
// a single-shard test, and is exactly the window GH
// weaviate/weaviate#12189 defers). Then simulate a process restart via
// idx.initShard, which builds a brand-new *Shard with an empty
// in-memory rangeableLocalReady map, and, critically,
// FinalizeCompletedMigrations deletes the tidied tracker directory as
// part of that same restart, so the only on-disk evidence of the
// completed migration is gone by the time rangeableForceIndexOverlay
// could be asked to fast-exit.
//
// This test catches the write-loss regression a naive
// `len(rangeableLocalReady)==0` fast exit (without
// seedRangeableLocalReadyFromMigrationHistory seeding an explicit
// `true` entry before FinalizeCompletedMigrations runs) would
// reintroduce, because it exercises the exact input shape that trips
// the bug: a restarted shard whose migration history lives only in a
// tracker directory that gets deleted during the same restart that
// wipes the in-memory map. Verified red on the pre-fix shape (see the
// handoff log's stash-revert record); on the shipped code, the
// explicit `true` seeded before FinalizeCompletedMigrations survives
// the restart and this test is green.
func TestRangeableForceIndexOverlay_SurvivesRestartAfterLocalTidyBeforeSchemaFlip(t *testing.T) {
	ctx := testCtx()
	className := "RangeableOverlayRestart_" + uuid.NewString()[:8]
	const propName = filterableToRangeablePropName
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 10, className) {
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

	// Driving the migration inline (above) never goes through
	// ReindexProvider, so it never writes payload.mig on its own; do
	// it explicitly so the on-disk shape matches what a real
	// RAFT/DTM-driven migration leaves behind (see
	// filterableToRangeableRecoveryPayloadJSON's godoc).
	require.NoError(t, task.SaveRecoveryPayload(shard.pathLSM(),
		filterableToRangeableRecoveryPayloadJSON(t, propName)))

	falseVal := false
	preFlipProp := &models.Property{Name: propName, DataType: []string{"int"}, IndexRangeFilters: &falseVal}

	// Sanity: migration fully tidied locally (in-process
	// OnMigrationComplete already ran), cluster-wide schema flag still
	// false. The overlay must already be forcing rangeable.
	overlay := shard.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
	require.NotNil(t, overlay, "sanity: pre-restart overlay should already force rangeable")
	require.True(t, overlay[propName].ForceRangeable)

	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	// Deliberately do NOT wire idx.shardReindexer to a task that knows
	// about this migration (it stays the default shardReindexerV3Noop
	// testShardWithSettings set up, see helper_for_test.go). Real node
	// startup initializes shards before RAFT/DTM has necessarily
	// re-synced the active-task list to this node's reindexer, so a
	// write can land on a freshly-initialized shard with NO reindex
	// task driving recovery yet. Wiring a live, matching task here (as
	// the sibling recovery-convergence tests do) would let its
	// OnAfterLsmInit re-discover "not started" (the tracker dir is
	// gone) and re-run the WHOLE migration from scratch, which
	// re-converges regardless of whether the seed fix under test even
	// exists, masking the exact gap this test is for.
	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "restarted shard init must succeed")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	overlay2 := shard2.rangeableForceIndexOverlay([]*models.Property{preFlipProp})
	require.NotNilf(t, overlay2, "restart must not reopen the post-swap pre-flip write-loss window: "+
		"this shard's migration history says property %q's rangeable bucket is tidied, so the write "+
		"overlay must still force rangeable even though the in-memory map was wiped by the restart",
		propName)
	require.True(t, overlay2[propName].ForceRangeable)
}

// newSeedHistoryTestShardWithTracker builds a fresh shard for a
// TestSeedRangeableLocalReadyFromMigrationHistory sub-test and drives a
// single rangeable migration far enough to leave exactly one tracker
// dir on disk with a parseable payload.mig (SaveRecoveryPayload'd
// explicitly, since driving the migration inline never goes through
// ReindexProvider - see filterableToRangeableRecoveryPayloadJSON's
// godoc). namePrefix keeps each sub-test's class name distinct;
// t.Cleanup shuts the shard down when the sub-test returns, so callers
// don't need their own defer.
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
// [seedRangeableLocalReadyFromMigrationHistory] directly against a
// hand-built on-disk tracker, isolating it from the full
// migration/restart/task-machinery integration (covered end-to-end by
// TestRangeableForceIndexOverlay_SurvivesRestartAfterLocalTidyBeforeSchemaFlip).
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
		// No tidied.mig written: this tracker is still in-flight.

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

		// The fast exit must be disabled for the WHOLE shard by this,
		// not just the affected property: rangeableForceIndexOverlay
		// has no way to know in advance which properties an
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
