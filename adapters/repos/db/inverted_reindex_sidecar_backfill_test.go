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
