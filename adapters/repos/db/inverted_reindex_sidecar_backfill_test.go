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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// weaviate/0-weaviate-issues#322: enable-* migrations never backfill the
// null/length sidecar buckets (or the BM25 prop-length tally) for objects
// that existed BEFORE the migration started. The backfill loop
// (OnAfterLsmInitAsync -> uuidObjectsIteratorAsync) already computes both
// return values of AnalyzeObjectForMigrationWithOverlay (props AND
// nilProps), but the write loop only threads `props` into
// strategy.WriteToReindexBucket for the migrating properties' VALUE index -
// it never writes the null/length sidecar buckets or updates the BM25 prop
// length tracker, both of which a normal PutObject write would populate via
// extendInvertedIndicesLSM / SetPropertyLengths.
//
// These tests build a "migrated" shard (property starts unindexed, enable-*
// runs after the objects already exist) and a "control" shard (identical
// objects, property indexed with the FINAL schema from the start) and
// assert the sidecar bucket / tally content converges. Pre-fix, the
// migrated shard's sidecar buckets are empty for the pre-existing objects
// (RED); post-fix they are bit-equal to the control (GREEN).
// -----------------------------------------------------------------------------

const sidecarBackfillTextProp = "title"

// sidecarBackfillTextObjects builds n objects on a text property: the
// first nilCount have NO title property at all (nil path, isNull=true);
// the rest cycle through word-counts 1..5 so the length bucket has
// multiple distinct keys. Deterministic UUIDs aren't required - DocID
// assignment order is what must match between the two shards, and both
// shards receive the objects in the same slice order.
func sidecarBackfillTextObjects(className string, n, nilCount int) []*storobj.Object {
	out := make([]*storobj.Object, n)
	words := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for i := 0; i < n; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
		}
		if i >= nilCount {
			wordCount := (i % 5) + 1 // 1..5 words -> 5 distinct lengths
			text := ""
			for w := 0; w < wordCount; w++ {
				if w > 0 {
					text += " "
				}
				text += words[w]
			}
			obj.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: text}
		}
		// i < nilCount: Properties stays nil -> title is absent -> null path.
		out[i] = obj
	}
	return out
}

// newSidecarBackfillTextClass builds a class with a single text property.
// indexFilterable/indexSearchable control which index (if any) the
// property starts with - nil means "not explicitly set" (defaults apply).
func newSidecarBackfillTextClass(className string, indexFilterable, indexSearchable *bool) *models.Class {
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{
			{
				Name:            sidecarBackfillTextProp,
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: indexFilterable,
				IndexSearchable: indexSearchable,
			},
		},
	}
}

// sidecarNullFingerprint / sidecarLengthFingerprint read the live null /
// length sidecar buckets directly (bypassing the query layer, which is
// gated on the RAFT schema flag this test never flips) so the assertions
// pin the exact on-disk backfill content the migration produced.
func sidecarNullFingerprint(t *testing.T, shard *Shard, propName string) map[string][]uint64 {
	t.Helper()
	return fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameNullLSM(propName)))
}

func sidecarLengthFingerprint(t *testing.T, shard *Shard, propName string) map[string][]uint64 {
	t.Helper()
	return fingerprintRoaringSetBucket(t, shard.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName)))
}

// TestSidecarBackfill_EnableFilterable_NullAndLength is the RED repro for
// the null/length half of gh#322 on the enable-filterable migration path.
//
// Causal link: this test catches the bug because it drives the REAL
// production backfill loop (ShardReindexTaskGeneric.OnAfterLsmInitAsync ->
// uuidObjectsIteratorAsync -> Shard.AnalyzeObjectForMigrationWithOverlay)
// against pre-existing objects, then reads the null/length buckets that
// loop is supposed to populate. On the unfixed tree those buckets are
// either missing entirely (no bucket -> nil bucket -> empty fingerprint)
// or contain zero entries for the backfilled objects, because the backfill
// write loop only calls strategy.WriteToReindexBucket for the value index
// and discards AnalyzeObjectForMigrationWithOverlay's nilProps return
// value. The control shard (property indexed from class creation) proves
// what the CORRECT fingerprint should be for the identical object set.
func TestSidecarBackfill_EnableFilterable_NullAndLength(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	migratedClassName := "SidecarBackfillFilterableMigrated_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillTextClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(migratedClassName, numObjects, nilCount)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	// Pre-migration: null/length buckets must be absent (HasAnyInvertedIndex
	// is false for an unindexed property, so shard init skips creating
	// them - see initPropertyBuckets, shard_init_properties.go:92).
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillTextProp)),
		"pre-migration null bucket must be absent for an unindexed property")
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameLengthLSM(sidecarBackfillTextProp)),
		"pre-migration length bucket must be absent for an unindexed property")

	task, wrapped := newEnableFilterableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	// Control: identical objects, property filterable from class creation.
	controlClassName := "SidecarBackfillFilterableControl_" + uuid.NewString()[:8]
	vTrue := true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vTrue, &vFalse)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)

	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, nilCount)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}

	controlNull := sidecarNullFingerprint(t, controlShard, sidecarBackfillTextProp)
	controlLength := sidecarLengthFingerprint(t, controlShard, sidecarBackfillTextProp)
	require.NotEmpty(t, controlNull, "control null fingerprint must be non-empty (sanity check on the control shard itself)")
	require.NotEmpty(t, controlLength, "control length fingerprint must be non-empty (sanity check on the control shard itself)")

	migratedNull := sidecarNullFingerprint(t, migratedShard, sidecarBackfillTextProp)
	migratedLength := sidecarLengthFingerprint(t, migratedShard, sidecarBackfillTextProp)

	assert.Equal(t, controlNull, migratedNull,
		"null-state sidecar for pre-existing objects must match a freshly-indexed control after enable-filterable")
	assert.Equal(t, controlLength, migratedLength,
		"length sidecar for pre-existing objects must match a freshly-indexed control after enable-filterable")
}

// TestSidecarBackfill_EnableSearchable_NullLengthAndTally is the RED repro
// for gh#322 on the enable-searchable path, additionally covering the BM25
// prop-length tally (SetPropertyLengths / JsonShardMetaData), which is only
// fed by properties with HasSearchableIndex=true.
func TestSidecarBackfill_EnableSearchable_NullLengthAndTally(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	migratedClassName := "SidecarBackfillSearchableMigrated_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillTextClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(migratedClassName, numObjects, nilCount)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillTextProp)))
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameLengthLSM(sidecarBackfillTextProp)))

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	controlClassName := "SidecarBackfillSearchableControl_" + uuid.NewString()[:8]
	vTrue := true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)

	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, nilCount)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}

	controlNull := sidecarNullFingerprint(t, controlShard, sidecarBackfillTextProp)
	controlLength := sidecarLengthFingerprint(t, controlShard, sidecarBackfillTextProp)
	require.NotEmpty(t, controlNull)
	require.NotEmpty(t, controlLength)

	migratedNull := sidecarNullFingerprint(t, migratedShard, sidecarBackfillTextProp)
	migratedLength := sidecarLengthFingerprint(t, migratedShard, sidecarBackfillTextProp)
	assert.Equal(t, controlNull, migratedNull,
		"null-state sidecar for pre-existing objects must match a freshly-indexed control after enable-searchable")
	assert.Equal(t, controlLength, migratedLength,
		"length sidecar for pre-existing objects must match a freshly-indexed control after enable-searchable")

	controlSum, controlCount, controlMean, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.NotZero(t, controlCount, "control tally must have counted at least one searchable write (sanity check)")

	migratedSum, migratedCount, migratedMean, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, controlCount, migratedCount,
		"BM25 prop-length tally COUNT for pre-existing objects must match control after enable-searchable")
	assert.Equal(t, controlSum, migratedSum,
		"BM25 prop-length tally SUM for pre-existing objects must match control after enable-searchable")
	assert.InDelta(t, controlMean, migratedMean, 0.001,
		"BM25 avgdl (prop-length mean) for pre-existing objects must match control after enable-searchable")
}

const sidecarBackfillNumProp = "score"

func sidecarBackfillNumObjects(className string, n int) []*storobj.Object {
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		out[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					sidecarBackfillNumProp: int64(i),
				},
			},
		}
	}
	return out
}

// newSidecarBackfillNumClass builds a class with a single numeric property
// that starts with ZERO inverted index (filterable and rangeable both
// explicitly false), matching the from-scratch enable-rangeable scenario
// where initPropertyBuckets never creates a null/length bucket pre-migration.
func newSidecarBackfillNumClass(className string, indexFilterable, indexRangeFilters *bool) *models.Class {
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{
			{
				Name:              sidecarBackfillNumProp,
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   indexFilterable,
				IndexRangeFilters: indexRangeFilters,
			},
		},
	}
}

// TestSidecarBackfill_EnableRangeable_NullAndLength is the RED repro for
// gh#322 on the enable-rangeable path, starting from a property with NO
// prior index at all (IndexFilterable=false, IndexRangeFilters=false) so
// the null/length bucket-creation gap (shared with #12211's adjacent
// finding) is exercised too.
//
// Numeric properties always report Length=-1 (length doesn't apply - see
// inverted_reindexer.go:360 "properties where defining a length does not
// make sense (floats etc.) have a negative entry as length"), so only the
// null-state sidecar is meaningful here; length assertions are N/A by
// construction (documented in the adversarial self-check, not silently
// skipped).
func TestSidecarBackfill_EnableRangeable_Null(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	migratedClassName := "SidecarBackfillRangeableMigrated_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillNumClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillNumObjects(migratedClassName, numObjects)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillNumProp)),
		"pre-migration null bucket must be absent for an unindexed numeric property")

	task, wrapped := newFilterableToRangeableTask(t, migratedIdx, migratedClassName, sidecarBackfillNumProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	controlClassName := "SidecarBackfillRangeableControl_" + uuid.NewString()[:8]
	vTrue := true
	controlClass := newSidecarBackfillNumClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)

	controlObjects := sidecarBackfillNumObjects(controlClassName, numObjects)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}

	controlNull := sidecarNullFingerprint(t, controlShard, sidecarBackfillNumProp)
	require.NotEmpty(t, controlNull, "control null fingerprint must be non-empty (sanity check)")

	migratedNull := sidecarNullFingerprint(t, migratedShard, sidecarBackfillNumProp)
	assert.Equal(t, controlNull, migratedNull,
		"null-state sidecar for pre-existing objects must match a freshly-indexed control after enable-rangeable")
}

// TestSidecarBackfill_ScopedToMigratingPropOnly guards against a naive fix
// that calls the full extendInvertedIndicesLSM (or re-tallies every analyzed
// property) instead of filtering to just the migrating property set: doing
// so would double-count the BM25 tally / duplicate-but-idempotent the
// null/length buckets for a property that was NOT part of this migration
// and was already correctly indexed at original write time.
//
// Causal link: this test catches a "backfill touches non-migrating props"
// regression because it puts a SECOND, already-searchable property
// ("summary") on the same objects, migrates only "title", and asserts the
// untouched property's tally is byte-identical to its own pre-migration
// value - a naive whole-object backfill would inflate summary's Sum/Count.
func TestSidecarBackfill_ScopedToMigratingPropOnly(t *testing.T) {
	const numObjects = 10
	ctx := testCtx()

	className := "SidecarBackfillScoped_" + uuid.NewString()[:8]
	vFalse := false
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{
			{
				Name:            sidecarBackfillTextProp, // migration target, starts unindexed
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexSearchable: &vFalse,
			},
			{
				// bystander: already searchable from class creation, NOT
				// part of this migration.
				Name:         "summary",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
	}
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for i := 0; i < numObjects; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					sidecarBackfillTextProp: fmt.Sprintf("alpha bravo %d", i),
					"summary":               "gamma delta epsilon",
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally("summary")
	require.NoError(t, err)
	require.Equal(t, numObjects, preCount, "sanity: summary should have one tally entry per object pre-migration")

	task, wrapped := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted)

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally("summary")
	require.NoError(t, err)
	assert.Equal(t, preCount, postCount, "migrating a different property must not change the bystander's tally COUNT")
	assert.Equal(t, preSum, postSum, "migrating a different property must not change the bystander's tally SUM")
}

// -----------------------------------------------------------------------------
// The BM25 tally is recomputed once, from scratch, per migrating property
// whose AnalyzerOverlay forces IndexSearchable on - see
// ShardReindexTaskGeneric.completeMigrationOnShard and
// Shard.recomputeSearchableTallyForProp - rather than incrementally inside
// backfillSidecarsForMigration's per-object loop. The tests below pin the
// three regressions an incremental per-object tally write is prone to:
//
//  1. TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally and
//     TestSidecarBackfill_RebuildSearchable_DoesNotDoubleCountTally: a
//     migration on an ALREADY-searchable property must not re-tally
//     objects that were already tallied at original PutObject time.
//  2. TestSidecarBackfill_EnableFilterableOverSearchableProp_DoesNotDoubleCountTally:
//     the same regression, but for enable-filterable (a different
//     strategy) run over a property that happens to already be
//     searchable - proving the fix isn't scoped to one strategy.
//  3. TestSidecarBackfill_EnableSearchable_TallyDurableAcrossRestart: the
//     recompute must Flush() to disk, so a process restart immediately
//     after a completed enable-searchable migration does not lose the
//     tally.
// -----------------------------------------------------------------------------

// TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally is the RED
// repro for gh#322 finding 1: OnAfterLsmInitAsync's backfill loop
// runs for every generic migration, not just enable-*. Before this rework,
// backfillSidecarsForMigration gated the BM25 tally write on
// prop.HasSearchableIndex, which is true for a property that was ALREADY
// searchable before a map->blockmax migration started (map->blockmax only
// changes the on-disk bucket strategy, not searchable-ness) - so every
// pre-existing object got re-tallied on top of the tally its original
// PutObject already contributed, doubling Sum/Count.
//
// Causal link: this test catches the bug because it drives the REAL
// production migration lifecycle (task.OnAfterLsmInitAsync, looped to
// completion, same pattern TestMapToBlockmaxMigration_RuntimeSwap uses)
// against a property that is searchable from class creation, then compares
// the tally before and after the migration. On the unfixed tree (which
// still calls TrackProperty per object inside backfillSidecarsForMigration,
// gated on prop.HasSearchableIndex with no distinction for "already
// searchable before this migration") the post-migration tally is exactly
// double the pre-migration tally: postCount == 2*preCount,
// postSum == 2*preSum. Post-fix, MapToBlockmaxStrategy's AnalyzerOverlay
// returns nil (noAnalyzerOverlay), so completeMigrationOnShard's
// ForceSearchable gate never fires for it and the tally is left untouched.
func TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	className := "SidecarBackfillMapToBlockmax_" + uuid.NewString()[:8]
	// newTestClassWithProps leaves IndexSearchable nil (defaults to true)
	// and UsingBlockMaxWAND=false, so the property starts fully searchable
	// via a MapCollection bucket - exactly the map->blockmax source state.
	class := newTestClassWithProps(className, []string{sidecarBackfillTextProp})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Equal(t, numObjects, preCount, "sanity: every PutObject must have tallied (property is searchable from class creation)")
	require.NotZero(t, preSum, "sanity: non-empty text must contribute a non-zero sum")

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, strategy.migrationCompleted)

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, preCount, postCount,
		"BUG regression check (gh#322): map->blockmax on an already-searchable "+
			"property must not double the BM25 tally COUNT - got %d, want %d (pre-migration value, unchanged)", postCount, preCount)
	assert.Equal(t, preSum, postSum,
		"BUG regression check (gh#322): map->blockmax on an already-searchable "+
			"property must not double the BM25 tally SUM - got %d, want %d (pre-migration value, unchanged)", postSum, preSum)
}

// TestSidecarBackfill_RebuildSearchable_DoesNotDoubleCountTally is the same
// regression as TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally,
// exercised against RebuildSearchableStrategy instead - a second,
// independent strategy whose whole point is to rebuild an ALREADY-searchable
// property's bucket without changing its searchable-ness. RebuildSearchable
// also returns a nil AnalyzerOverlay (explicitly - see its own godoc: "MUST
// NOT change tokenization"), so it's a second confirmation that the
// ForceSearchable gate generalizes correctly across strategies rather than
// being coupled to EnableSearchableStrategy by name.
func TestSidecarBackfill_RebuildSearchable_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	className := "SidecarBackfillRebuildSearchable_" + uuid.NewString()[:8]
	class := newRebuildSearchableTestClass(className, []string{sidecarBackfillTextProp})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Equal(t, numObjects, preCount, "sanity: every PutObject must have tallied (property is searchable from class creation)")

	task, wrapped := newRebuildSearchableTask(t, idx, className, sidecarBackfillTextProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted)

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, preCount, postCount,
		"BUG regression check (gh#322): RebuildSearchable must not double the BM25 "+
			"tally COUNT for the property it rebuilds - got %d, want %d (pre-migration value, unchanged)", postCount, preCount)
	assert.Equal(t, preSum, postSum,
		"BUG regression check (gh#322): RebuildSearchable must not double the BM25 "+
			"tally SUM for the property it rebuilds - got %d, want %d (pre-migration value, unchanged)", postSum, preSum)
}

// TestSidecarBackfill_EnableFilterableOverSearchableProp_DoesNotDoubleCountTally
// is the RED repro for gh#322 finding 2: enable-filterable migrating
// a property that is ALREADY searchable (IndexSearchable defaults to true
// for text properties; only IndexFilterable is false here) must not
// re-tally that property's BM25 stats, even though the property IS in
// propsInScope for this migration (enable-filterable's own target).
//
// This is a distinct code path from TestSidecarBackfill_ScopedToMigratingPropOnly
// (which guards a bystander prop NOT in propsInScope): here the double-count
// risk is the migrating prop ITSELF, already searchable, being re-tallied
// by a migration that only adds a filterable index. Causal link: on the
// unfixed tree, backfillSidecarsForMigration re-tallies any prop in
// propsInScope with HasSearchableIndex=true, regardless of which index
// type the migration itself is adding - so an enable-filterable migration
// over a text prop (searchable=true by default, filterable=false) doubles
// the BM25 tally as a side effect of adding an unrelated index type.
// Post-fix, EnableFilterableStrategy's AnalyzerOverlay only ever sets
// ForceFilterable (never ForceSearchable), so completeMigrationOnShard's
// gate never fires and the tally is left untouched.
func TestSidecarBackfill_EnableFilterableOverSearchableProp_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	className := "SidecarBackfillFilterableOverSearchable_" + uuid.NewString()[:8]
	// IndexFilterable=false (migration target), IndexSearchable left nil
	// -> defaults to true for a text prop (already searchable).
	class := newEnableFilterableTestClass(className, sidecarBackfillTextProp)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Equal(t, numObjects, preCount, "sanity: every PutObject must have tallied (property is searchable by default)")

	task, wrapped := newEnableFilterableTask(t, idx, className, sidecarBackfillTextProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted)

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, preCount, postCount,
		"BUG regression check (gh#322): enable-filterable over an already-searchable "+
			"property must not double that property's BM25 tally COUNT - got %d, want %d (pre-migration value, unchanged)", postCount, preCount)
	assert.Equal(t, preSum, postSum,
		"BUG regression check (gh#322): enable-filterable over an already-searchable "+
			"property must not double that property's BM25 tally SUM - got %d, want %d (pre-migration value, unchanged)", postSum, preSum)
}

// TestSidecarBackfill_EnableSearchable_TallyDurableAcrossRestart is the RED
// repro for gh#322 finding 3: the old incremental tally write never
// flushed the property-length tracker, so a crash immediately after a
// completed enable-searchable migration lost the entire new-prop tally in
// memory - on-disk count would read back as 0 even though the in-process
// tracker had the right values.
//
// Causal link: this test catches the bug by discarding the in-process
// tracker entirely and constructing a BRAND NEW inverted.JsonShardMetaData
// pointed at the same on-disk file path (the exact thing a restarted
// process does - NewShard's init path does exactly this construction, see
// shard_init_lsm.go:202), rather than reading back through the live
// shard's already-in-memory tracker. If recomputeSearchableTallyForProp's
// Flush() call were missing (or a no-op), this fresh instance would load
// an empty/stale tracker file and read back zero for a property this test
// asserts has a non-zero, exact-value tally.
func TestSidecarBackfill_EnableSearchable_TallyDurableAcrossRestart(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	migratedClassName := "SidecarBackfillTallyDurable_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillTextClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(migratedClassName, numObjects, nilCount)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	// Control: identical objects, searchable from class creation.
	controlClassName := "SidecarBackfillTallyDurableControl_" + uuid.NewString()[:8]
	vTrue := true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)
	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, nilCount)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}
	controlSum, controlCount, _, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.NotZero(t, controlCount, "sanity: control tally must have counted at least one searchable write")

	// THE PIN: discard the migrated shard's in-memory tracker entirely and
	// load a fresh one from the same on-disk file - the file must already
	// hold the correct, fully-recomputed tally, or this reload will not
	// match the control.
	trackerPath := migratedShard.GetPropertyLengthTracker().FileName()
	require.NotEmpty(t, trackerPath, "sanity: tracker must have a backing file path")
	reloadedTracker, err := inverted.NewJsonShardMetaData(trackerPath, migratedIdx.logger)
	require.NoError(t, err)
	defer reloadedTracker.Close()

	reloadedSum, reloadedCount, reloadedMean, err := reloadedTracker.PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, controlCount, reloadedCount,
		"BUG regression check (gh#322): BM25 tally COUNT must survive a restart "+
			"(fresh tracker reloaded from disk) immediately after a completed enable-searchable migration - "+
			"got %d, want %d (control)", reloadedCount, controlCount)
	assert.Equal(t, controlSum, reloadedSum,
		"BUG regression check (gh#322): BM25 tally SUM must survive a restart "+
			"(fresh tracker reloaded from disk) immediately after a completed enable-searchable migration - "+
			"got %d, want %d (control)", reloadedSum, controlSum)
	assert.InDelta(t, float64(controlSum)/float64(controlCount), reloadedMean, 0.001,
		"BM25 avgdl (prop-length mean) must survive a restart immediately after a completed enable-searchable migration")
}

// -----------------------------------------------------------------------------
// PR #12221 QA finding: recomputeSearchableTallyForProp rescans the objects
// bucket via CursorOnDisk, which by construction only ever sees
// segment-resident data (lsmkv/cursor_bucket_replace.go). The objects
// bucket is flushed exactly once, at the START of the backfill loop
// (OnAfterLsmInitAsync); the recompute runs much later, inside
// runtimeSwap/completeMigrationOnShard, with no re-flush before this fix.
// A write that lands in the objects memtable after the backfill's flush
// but before the recompute's cursor snapshot is therefore invisible to the
// rescan, even though it IS live-searchable (the migration-window
// double-write callback mirrors postings into the searchable bucket
// unconditionally - see registerAddToPropertyValueIndex - while
// SetPropertyLengths on the normal write path only tracks props that are
// ALREADY live-searchable, which the migrating prop is not until this
// recompute flips it). Net effect: avgdl silently undercounts by exactly
// the concurrent-write volume that raced the backfill-to-recompute window.
// -----------------------------------------------------------------------------

// TestSidecarBackfill_EnableSearchable_TallyMissesConcurrentWriteInMemtable
// is the RED/GREEN regression test for the residual above.
//
// Causal link: this test catches the bug because it writes the (n+1)th
// object to the migrated shard strictly AFTER RunReindexOnlyOnShard's
// backfill scan has already returned (so the backfill never processed it)
// and strictly BEFORE RunSwapOnShard's post-swap recompute runs - landing
// it in the objects memtable at exactly the moment
// recomputeSearchableTallyForProp opens its rescan cursor. Verified RED on
// the pre-fix tree (recomputeSearchableTallyForProp's FlushAndSwitch call
// temporarily removed, then restored - see handoff log): migrated tally
// COUNT/SUM = 20/60 (the concurrent object's 7-word contribution silently
// dropped) vs control's 21/67. Post-fix (GREEN): migrated tally is
// bit-for-bit equal to control's 21/67.
func TestSidecarBackfill_EnableSearchable_TallyMissesConcurrentWriteInMemtable(t *testing.T) {
	const numObjects = 20 // no nils - keeps the arithmetic exact and legible
	const concurrentObjText = "alpha bravo charlie delta echo foxtrot golf"
	ctx := testCtx()

	migratedClassName := "SidecarBackfillConcurrentWrite_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillTextClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(migratedClassName, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))

	// THE RACE: written after the backfill scan already returned, before
	// RunPrepareOnShard/RunSwapOnShard run the post-swap recompute. Lands
	// in the objects memtable - nothing flushes it before
	// recomputeSearchableTallyForProp's own FlushAndSwitch call (the fix
	// under test).
	concurrentObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: migratedClassName,
			Properties: map[string]interface{}{
				sidecarBackfillTextProp: concurrentObjText,
			},
		},
	}
	require.NoError(t, migratedShard.PutObject(ctx, concurrentObj))

	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	// Control: identical n+1 objects, searchable from class creation - no
	// backfill/recompute involved at all, so this is unaffected by the bug
	// by construction and pins the ground truth the migrated shard must
	// converge to.
	controlClassName := "SidecarBackfillConcurrentWriteControl_" + uuid.NewString()[:8]
	vTrue := true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)

	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, 0)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}
	controlExtra := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: controlClassName,
			Properties: map[string]interface{}{
				sidecarBackfillTextProp: concurrentObjText,
			},
		},
	}
	require.NoError(t, controlShard.PutObject(ctx, controlExtra))

	controlSum, controlCount, controlMean, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects+1, controlCount, "sanity: control must have tallied all n+1 objects")

	migratedSum, migratedCount, migratedMean, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, controlCount, migratedCount,
		"BUG regression check (gh#322 / PR#12221 residual): BM25 tally COUNT must include an object written to "+
			"the objects memtable between the backfill scan and the post-swap recompute - got %d, want %d (control)",
		migratedCount, controlCount)
	assert.Equal(t, controlSum, migratedSum,
		"BUG regression check (gh#322 / PR#12221 residual): BM25 tally SUM must include an object written to "+
			"the objects memtable between the backfill scan and the post-swap recompute - got %d, want %d (control)",
		migratedSum, controlSum)
	assert.InDelta(t, controlMean, migratedMean, 0.001,
		"BM25 avgdl must include the concurrent-write object's contribution")
}

// TestSidecarBackfill_EnableSearchable_SecondRecomputeConvergesResidualWindowWrite
// pins the claim QA asked for alongside the flush fix: a write that races
// into the objects memtable in the residual window BETWEEN the new flush
// and the cursor open (a window this fix cannot close by construction - the
// flush and the cursor open are two sequential calls, not one atomic
// operation) is not lost. It is durable in a new segment created by the
// racing writer's own path, and any SUBSEQUENT recompute - idempotent by
// construction (ResetProperty + full rescan) - picks it up. The production
// call site for a "subsequent recompute" is a recovery re-entry into
// completeMigrationOnShard (finalizeMigrationAfterRecovery); this test
// exercises the same idempotent primitive directly to keep the repro fast
// and deterministic rather than driving a full crash/resume harness.
//
// Causal link: this test catches a regression in the idempotency claim
// specifically - not the flush-window bug above - because it writes the
// residual-window object AFTER the first (successful, flushed) recompute
// has already run and produced a tally that does NOT include it, then
// asserts a second direct recomputeSearchableTallyForProp call converges to
// the correct total. If a future change made the recompute non-idempotent
// (e.g. an additive tally instead of ResetProperty-then-rescan), this test
// would fail by double-counting the first n objects instead of landing on
// exactly n+1.
func TestSidecarBackfill_EnableSearchable_SecondRecomputeConvergesResidualWindowWrite(t *testing.T) {
	const numObjects = 20
	const residualObjText = "alpha bravo charlie" // 3 words
	ctx := testCtx()

	migratedClassName := "SidecarBackfillResidualWindow_" + uuid.NewString()[:8]
	vFalse := false
	migratedClass := newSidecarBackfillTextClass(migratedClassName, &vFalse, &vFalse)
	migratedShd, migratedIdx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	migratedShard := migratedShd.(*Shard)
	defer migratedShard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(migratedClassName, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	firstSum, firstCount, _, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, firstCount, "sanity: first recompute must land on exactly the n pre-existing objects")

	// Simulate a write racing the residual window between the fix's flush
	// and the cursor open: land an (n+1)th object on the shard strictly
	// AFTER the first recompute has already completed and been flushed.
	residualObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: migratedClassName,
			Properties: map[string]interface{}{
				sidecarBackfillTextProp: residualObjText,
			},
		},
	}
	require.NoError(t, migratedShard.PutObject(ctx, residualObj))

	// THE CONVERGENCE CHECK: a second, independent recompute (same
	// idempotent primitive a recovery re-entry into
	// completeMigrationOnShard would invoke) must pick up the residual
	// write and land on n+1 - not 2n (double-count, would indicate the
	// reset-then-rescan idempotency contract broke) and not n (would
	// indicate the residual write was permanently lost, not just delayed).
	overlay := task.strategy.AnalyzerOverlay([]string{sidecarBackfillTextProp})
	require.NoError(t, migratedShard.recomputeSearchableTallyForProp(ctx, sidecarBackfillTextProp, overlay))

	secondSum, secondCount, _, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.EqualValues(t, numObjects+1, secondCount,
		"a second recompute must converge to n+1 (pick up the residual-window write exactly once) - "+
			"got count=%d after first=%d", secondCount, firstCount)
	assert.Equal(t, firstSum+3, secondSum,
		"a second recompute's SUM must equal the first plus exactly the residual object's 3-word contribution "+
			"(no double-count of the original n objects) - got sum=%d after first=%d", secondSum, firstSum)
}

// -----------------------------------------------------------------------------
// weaviate/0-weaviate-issues#322 post-swap residual window: the double-write
// callback (registered before backfill starts, disabled only after
// runtimeSwap returns - see the deferred disableCallbacks call) mirrors
// POSTINGS for the migrating searchable prop unconditionally, but never fed
// the BM25 prop-length tally. A write landing anywhere between
// recomputeSearchableTallyForProp's rescan and disableCallbacks actually
// firing was therefore searchable (postings present) but permanently
// excluded from avgdl, because no further recompute is guaranteed to ever
// run for this prop on this shard absent a crash-recovery re-entry.
// trackMigratingPropLength / untrackMigratingPropLength
// (inverted_reindex_strategy_enable_searchable.go) close this by feeding
// the tracker from inside the callbacks themselves.
// -----------------------------------------------------------------------------

// TestSidecarBackfill_EnableSearchable_CallbackTallyClosesResidualStrandingWindow
// is the RED/GREEN regression test for the residual above.
//
// Causal link: this test catches the bug because it (1) runs only the
// backfill phase (RunReindexOnlyOnShard) - not the shared
// newSidecarBackfillSearchableFixture, see the class-shape comment below -
// so the double-write callbacks stay registered for the rest of the test;
// it never calls RunPrepareOnShard/RunSwapOnShard, which is what would
// otherwise disable them; (2) calls recomputeSearchableTallyForProp
// directly, the exact primitive runtimeSwap's completeMigrationOnShard
// would have invoked, to establish the "recompute already ran" baseline;
// then (3) writes an (n+1)th object and asserts the tally already reflects
// it with NO further recompute call. Pre-fix, EnableSearchableStrategy's
// add callback only mirrors postings (blockmaxSearchableAddCallback), so
// the tally stays at n instead of n+1. Post-fix, trackMigratingPropLength's
// TrackProperty call inside the add callback closes the gap synchronously
// on write - no second recompute needed, unlike
// TestSidecarBackfill_EnableSearchable_SecondRecomputeConvergesResidualWindowWrite
// above, whose residual write lands strictly AFTER callbacks are disabled.
func TestSidecarBackfill_EnableSearchable_CallbackTallyClosesResidualStrandingWindow(t *testing.T) {
	const numObjects = 20
	const residualObjText = "alpha bravo charlie delta" // 4 words
	ctx := testCtx()

	// Unlike newSidecarBackfillSearchableFixture's from-absolute-zero class
	// (filterable AND searchable both false), this test needs the prop to
	// already carry a live FILTERABLE index before the searchable-enable
	// migration starts. That's not cosmetic: Shard.AnalyzeObject (the
	// normal write path) gates a property out of analysis entirely via
	// inverted.HasAnyInvertedIndex when none of its index flags are live -
	// an enable-searchable migration never flips that gate for ordinary
	// writes (only IsTokenizationChangingMigration wires the per-shard
	// overlay AnalyzeObject consults), so a prop with ZERO live indexes
	// never reaches the double-write callback machinery for a brand-new
	// object at all, and the callback-tally fix under test would never
	// fire. A pre-existing filterable index keeps HasAnyInvertedIndex true
	// throughout the migration, which is exactly the shape the post-swap
	// residual window above targets.
	className := "SidecarBackfillCallbackTally_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	defer task.disableCallbacks()

	// Establish the "recompute already ran" baseline directly - the exact
	// primitive runtimeSwap's completeMigrationOnShard invokes - without
	// going through RunPrepareOnShard/RunSwapOnShard, so the double-write
	// callbacks registered by the fixture's RunReindexOnlyOnShard call
	// stay active for the rest of this test.
	overlay := task.strategy.AnalyzerOverlay([]string{sidecarBackfillTextProp})
	require.NoError(t, shard.recomputeSearchableTallyForProp(ctx, sidecarBackfillTextProp, overlay))

	firstSum, firstCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, firstCount, "sanity: recompute must land on exactly the n pre-existing objects")

	// THE RESIDUAL-STRANDING WRITE: lands strictly after the recompute
	// above, while the migration's double-write callbacks are still
	// registered and active (RunSwapOnShard - and its deferred
	// disableCallbacks - never runs in this test).
	residualObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				sidecarBackfillTextProp: residualObjText,
			},
		},
	}
	require.NoError(t, shard.PutObject(ctx, residualObj))

	// THE CLOSURE CHECK: the tally must already include the residual write
	// with NO further recompute call - proving the double-write callback
	// itself, not just a future rescan, keeps avgdl current.
	secondSum, secondCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.EqualValues(t, numObjects+1, secondCount,
		"BUG regression check (gh#322 / PR#12221 post-swap residual): the BM25 tally COUNT must include a write landing "+
			"after the recompute while the double-write callbacks are still active, with NO further recompute - "+
			"got %d, want %d", secondCount, numObjects+1)
	assert.Equal(t, firstSum+4, secondSum,
		"BUG regression check (gh#322 / PR#12221 post-swap residual): the BM25 tally SUM must include exactly the "+
			"residual write's 4-word contribution with NO further recompute - got sum=%d after first=%d", secondSum, firstSum)
}

// TestSidecarBackfill_EnableSearchable_CallbackTallyHandlesInPlaceEditWithoutInflatingCount
// pins the trickiest edge trackMigratingPropLength's godoc documents: an
// in-place edit of an object that ALREADY contributes to the tally (via the
// swap-time recompute) must leave COUNT unchanged and adjust SUM by exactly
// the net word-count delta - even though inverted.DeltaSkipSearchable hands
// the double-write callback an ITEM-LEVEL delta (only the changed tokens),
// not the object's full value, whenever the migrating prop already carries
// another live index (filterable here) alongside the not-yet-live
// searchable one.
//
// Causal link: this test catches a naive implementation that calls
// TrackProperty on every add-callback invocation unconditionally (Count+1
// every edit, permanently inflating COUNT for objects that were already
// tracked) or one that uses property.Length instead of len(property.Items)
// as the tracked magnitude (wrong unit - Length is a rune count, not a
// token count, so SUM would drift from the control's token-count-based
// value). It edits objects[0] (recompute-tracked, 1-word value "alpha")
// to a 3-word value in place, then asserts COUNT is unchanged (net
// Track+Untrack cancels) and SUM moved by exactly +2 (3 words - 1 word).
func TestSidecarBackfill_EnableSearchable_CallbackTallyHandlesInPlaceEditWithoutInflatingCount(t *testing.T) {
	const numObjects = 20
	ctx := testCtx()

	// Same class shape as TestSidecarBackfill_EnableSearchable_CallbackTallyClosesResidualStrandingWindow
	// (filterable already live) for the same reason: HasAnyInvertedIndex
	// must stay true for "title" throughout the migration for the
	// double-write callback to engage at all.
	className := "SidecarBackfillCallbackTallyEdit_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	defer task.disableCallbacks()

	overlay := task.strategy.AnalyzerOverlay([]string{sidecarBackfillTextProp})
	require.NoError(t, shard.recomputeSearchableTallyForProp(ctx, sidecarBackfillTextProp, overlay))

	firstSum, firstCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, firstCount, "sanity: recompute must land on exactly the n pre-existing objects")

	// objects[0] has wordCount=(0%5)+1=1 -> text "alpha" (sidecarBackfillTextObjects).
	// Both the old and new values are non-empty, so DeltaSkipSearchable
	// computes an item-level delta for "title" (see trackMigratingPropLength's
	// godoc) rather than a full swap.
	edited := objects[0]
	edited.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: "alpha bravo charlie"}
	require.NoError(t, shard.PutObject(ctx, edited))

	secondSum, secondCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.EqualValues(t, numObjects, secondCount,
		"BUG regression check (gh#322 / PR#12221 post-swap residual): an in-place edit of an ALREADY-tracked object must "+
			"not inflate the BM25 tally COUNT (the paired Track/Untrack calls must net to zero) - got %d, want %d (unchanged)",
		secondCount, numObjects)
	assert.Equal(t, firstSum+2, secondSum,
		"BUG regression check (gh#322 / PR#12221 post-swap residual): an in-place edit's SUM delta must equal exactly the "+
			"net word-count change (1 word -> 3 words = +2) even though the double-write callback only ever sees "+
			"an item-level delta, not the object's full value - got sum=%d after first=%d", secondSum, firstSum)
}
