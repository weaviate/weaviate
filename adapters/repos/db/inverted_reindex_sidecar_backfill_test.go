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
	"fmt"
	"sync"
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

// Pins weaviate/0-weaviate-issues#322: enable-* migrations skipped the
// null/length sidecar backfill and BM25 tally for pre-existing objects.
// Tests compare a "migrated" shard against an indexed-from-creation control.

const sidecarBackfillTextProp = "title"

// sidecarBackfillTextObjects builds n objects on a text property: the
// first nilCount have no title property (null path); the rest cycle
// word-counts 1..5 for distinct length-bucket keys. UUIDs need not be
// deterministic - only DocID assignment order must match between shards.
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

// newSidecarMarkerObject builds a single-property text object with a fresh
// random UUID - the shape every marker/racer/residual object injected
// mid-migration across this file and its sibling swap-window,
// tidy-window-race, and mid-tidy-tally test files takes to observe the
// double-write callback machinery's tally/posting behavior on one write.
func newSidecarMarkerObject(className, propName, propValue string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				propName: propValue,
			},
		},
	}
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

// newSidecarBackfillMigratedControlPair builds a "migrated" shard
// (classBuilder(name, false), starts unindexed) and a "control" shard
// (classBuilder(name, true), indexed from creation) with the same object
// shape under distinct class names.
func newSidecarBackfillMigratedControlPair(t *testing.T, ctx context.Context, namePrefix string, numObjects int,
	classBuilder func(className string, indexed bool) *models.Class,
	objectBuilder func(className string, numObjects int) []*storobj.Object,
) (migratedShard *Shard, migratedIdx *Index, controlShard *Shard) {
	t.Helper()

	migratedClassName := namePrefix + "Migrated_" + uuid.NewString()[:8]
	migratedClass := classBuilder(migratedClassName, false)
	migratedShd, idx := testShardWithSettings(t, ctx, migratedClass, enthnsw.UserConfig{Skip: true}, false, false, false)
	migratedShard = migratedShd.(*Shard)
	t.Cleanup(func() { migratedShard.Shutdown(ctx) })
	for _, obj := range objectBuilder(migratedClassName, numObjects) {
		require.NoError(t, migratedShard.PutObject(ctx, obj))
	}

	controlClassName := namePrefix + "Control_" + uuid.NewString()[:8]
	controlClass := classBuilder(controlClassName, true)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true}, false, false, false)
	controlShard = controlShd.(*Shard)
	t.Cleanup(func() { controlShard.Shutdown(ctx) })
	for _, obj := range objectBuilder(controlClassName, numObjects) {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}

	return migratedShard, idx, controlShard
}

// newSidecarBackfillEnableSearchableMigratedControlPair specializes
// newSidecarBackfillMigratedControlPair for enable-searchable: only the
// SEARCHABLE flag toggles between the two shards.
func newSidecarBackfillEnableSearchableMigratedControlPair(t *testing.T, ctx context.Context, namePrefix string,
	numObjects, nilCount int,
) (migratedShard *Shard, migratedIdx *Index, controlShard *Shard) {
	t.Helper()

	vFalse := false
	return newSidecarBackfillMigratedControlPair(t, ctx, namePrefix, numObjects,
		func(className string, indexed bool) *models.Class {
			searchable := &vFalse
			if indexed {
				vTrue := true
				searchable = &vTrue
			}
			return newSidecarBackfillTextClass(className, &vFalse, searchable)
		},
		func(className string, n int) []*storobj.Object {
			return sidecarBackfillTextObjects(className, n, nilCount)
		})
}

// Pins gh#322 on enable-filterable: pre-existing objects' null/length
// sidecar buckets must match a freshly-indexed control.
func TestSidecarBackfill_EnableFilterable_NullAndLength(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	vFalse := false
	migratedShard, migratedIdx, controlShard := newSidecarBackfillMigratedControlPair(t, ctx, "SidecarBackfillFilterable", numObjects,
		func(className string, indexed bool) *models.Class {
			filterable := &vFalse
			if indexed {
				vTrue := true
				filterable = &vTrue
			}
			return newSidecarBackfillTextClass(className, filterable, &vFalse)
		},
		func(className string, n int) []*storobj.Object {
			return sidecarBackfillTextObjects(className, n, nilCount)
		})
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	// Pre-migration: unindexed property, so shard init skips creating these
	// buckets (initPropertyBuckets, shard_init_properties.go).
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillTextProp)),
		"pre-migration null bucket must be absent for an unindexed property")
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameLengthLSM(sidecarBackfillTextProp)),
		"pre-migration length bucket must be absent for an unindexed property")

	task, wrapped := newEnableFilterableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

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

// Pins gh#322 on enable-searchable: also covers the BM25 prop-length tally
// (SetPropertyLengths), fed only by properties with HasSearchableIndex=true.
func TestSidecarBackfill_EnableSearchable_NullLengthAndTally(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	migratedShard, migratedIdx, controlShard := newSidecarBackfillEnableSearchableMigratedControlPair(t, ctx, "SidecarBackfillSearchable", numObjects, nilCount)
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillTextProp)))
	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameLengthLSM(sidecarBackfillTextProp)))

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

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

// Pins gh#322 on enable-rangeable, starting from a property with no prior
// index. Numeric properties report Length=-1 (see extendInvertedIndicesLSM),
// so only the null-state sidecar is meaningful here.
func TestSidecarBackfill_EnableRangeable_Null(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	vFalse := false
	migratedShard, migratedIdx, controlShard := newSidecarBackfillMigratedControlPair(t, ctx, "SidecarBackfillRangeable", numObjects,
		func(className string, indexed bool) *models.Class {
			rangeable := &vFalse
			if indexed {
				vTrue := true
				rangeable = &vTrue
			}
			return newSidecarBackfillNumClass(className, &vFalse, rangeable)
		},
		func(className string, n int) []*storobj.Object { return sidecarBackfillNumObjects(className, n) })
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	require.Nil(t, migratedShard.store.Bucket(helpers.BucketFromPropNameNullLSM(sidecarBackfillNumProp)),
		"pre-migration null bucket must be absent for an unindexed numeric property")

	task, wrapped := newFilterableToRangeableTask(t, migratedIdx, migratedClassName, sidecarBackfillNumProp)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	controlNull := sidecarNullFingerprint(t, controlShard, sidecarBackfillNumProp)
	require.NotEmpty(t, controlNull, "control null fingerprint must be non-empty (sanity check)")

	migratedNull := sidecarNullFingerprint(t, migratedShard, sidecarBackfillNumProp)
	assert.Equal(t, controlNull, migratedNull,
		"null-state sidecar for pre-existing objects must match a freshly-indexed control after enable-rangeable")
}

// Guards a naive fix that backfills every analyzed property instead of
// scoping to the migrating set: a bystander property already searchable
// before the migration must not be re-tallied.
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
				// bystander: already searchable, not part of this migration.
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

// Pins the double-count/durability regressions an incremental per-object
// tally write would reintroduce vs completeMigrationOnShard's full rescan.

// newSidecarBackfillPreTalliedFixture builds a shard whose property is
// already searchable from creation (so every PutObject tallies once) plus
// the pre-migration tally sanity check callers build their assertions on.
func newSidecarBackfillPreTalliedFixture(t *testing.T, ctx context.Context, classNamePrefix string,
	numObjects int, classBuilder func(className string) *models.Class,
) (shard *Shard, idx *Index, preSum, preCount int) {
	t.Helper()

	className := classNamePrefix + "_" + uuid.NewString()[:8]
	class := classBuilder(className)
	shd, index := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard = shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.Equal(t, numObjects, preCount, "sanity: every PutObject must have tallied (property is searchable from class creation)")

	return shard, index, preSum, preCount
}

// Pins gh#322 finding 1: map->blockmax over an already-searchable property
// must not re-tally objects already counted at original PutObject time.
func TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	// IndexSearchable defaults to true, UsingBlockMaxWAND=false: exactly
	// the map->blockmax source state (MapCollection bucket).
	shard, idx, preSum, preCount := newSidecarBackfillPreTalliedFixture(t, ctx, "SidecarBackfillMapToBlockmax", numObjects,
		func(className string) *models.Class {
			return newTestClassWithProps(className, []string{sidecarBackfillTextProp})
		})
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

// Same regression as TestSidecarBackfill_MapToBlockmax_DoesNotDoubleCountTally,
// exercised against RebuildSearchableStrategy - a second strategy with a
// nil AnalyzerOverlay, confirming the ForceSearchable gate isn't coupled to
// EnableSearchableStrategy by name.
func TestSidecarBackfill_RebuildSearchable_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	shard, idx, preSum, preCount := newSidecarBackfillPreTalliedFixture(t, ctx, "SidecarBackfillRebuildSearchable", numObjects,
		func(className string) *models.Class {
			return newRebuildSearchableTestClass(className, []string{sidecarBackfillTextProp})
		})

	task, wrapped := newRebuildSearchableTask(t, idx, shard.Index().Config.ClassName.String(), sidecarBackfillTextProp)
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

// Pins gh#322 finding 2: enable-filterable over a property that is already
// searchable (only filterable is added) must not re-tally that property's
// BM25 stats, even though it's the migration's own target - distinct from
// TestSidecarBackfill_ScopedToMigratingPropOnly, which guards a bystander.
func TestSidecarBackfill_EnableFilterableOverSearchableProp_DoesNotDoubleCountTally(t *testing.T) {
	const numObjects = 25
	ctx := testCtx()

	// IndexFilterable=false (migration target), IndexSearchable left nil
	// -> defaults to true for a text prop (already searchable).
	shard, idx, preSum, preCount := newSidecarBackfillPreTalliedFixture(t, ctx, "SidecarBackfillFilterableOverSearchable", numObjects,
		func(className string) *models.Class {
			return newEnableFilterableTestClass(className, sidecarBackfillTextProp)
		})

	task, wrapped := newEnableFilterableTask(t, idx, shard.Index().Config.ClassName.String(), sidecarBackfillTextProp)
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

// Pins gh#322 finding 3: the tally must Flush() to disk, or a crash
// immediately after a completed enable-searchable migration loses the
// entire new-prop tally.
func TestSidecarBackfill_EnableSearchable_TallyDurableAcrossRestart(t *testing.T) {
	const numObjects = 25
	const nilCount = 5
	ctx := testCtx()

	migratedShard, migratedIdx, controlShard := newSidecarBackfillEnableSearchableMigratedControlPair(t, ctx, "SidecarBackfillTallyDurable", numObjects, nilCount)
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	task, wrapped := newEnableSearchableTask(t, migratedIdx, migratedClassName, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, migratedShard))
	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	// Control: identical objects, searchable from class creation.
	controlSum, controlCount, _, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.NotZero(t, controlCount, "sanity: control tally must have counted at least one searchable write")

	// Discard the in-memory tracker and reload from disk (what a restart
	// does) - the file must already hold the recomputed tally.
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

// newBackfilledEnableSearchableFixture builds a shard with a live
// filterable index (Filterable=true, Searchable=false - so AnalyzeObject
// doesn't gate the prop out of the double-write callback machinery for
// from-scratch writes), numObjects pre-existing text objects, and an
// EnableSearchableStrategy task through RunReindexOnlyOnShard +
// RunPrepareOnShard. recomputeSearchableTallyForProp only runs inside
// RunSwapOnShard (see its call site), so stopping here leaves the tally
// untouched - callers needing a pre-swap sanity check on the tally, or a
// pre-swap hook on the task, may do so before driving RunSwapOnShard
// themselves. Shared by the swap-window, tidy-window-race, and
// mid-tidy-tally crash-recovery suites in sibling files.
func newBackfilledEnableSearchableFixture(t *testing.T, ctx context.Context, classNamePrefix string, numObjects int,
) (shard *Shard, idx *Index, task *ShardReindexTaskGeneric, wrapped *testEnableSearchableStrategyWrapper, objects []*storobj.Object) {
	t.Helper()

	className := classNamePrefix + "_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard = shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects = sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped = newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	return shard, idx, task, wrapped, objects
}

// Pins the residual window where a write lands after the backfill flush but
// before recomputeSearchableTallyForProp's cursor snapshot (see its godoc).

// newSidecarBackfillSearchableFixture builds a shard with numObjects
// pre-existing text objects and an EnableSearchableStrategy task whose
// backfill phase (RunReindexOnlyOnShard) has already run, so the
// double-write callbacks are registered and active on return.
func newSidecarBackfillSearchableFixture(t *testing.T, ctx context.Context, classNamePrefix string, numObjects int,
) (*Shard, *Index, *ShardReindexTaskGeneric, *testEnableSearchableStrategyWrapper) {
	t.Helper()

	className := classNamePrefix + "_" + uuid.NewString()[:8]
	vFalse := false
	class := newSidecarBackfillTextClass(className, &vFalse, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	return shard, idx, task, wrapped
}

// Pins the residual above: an object written after the backfill scan
// returns but before the post-swap recompute lands in the objects memtable
// and must not be silently dropped from the tally.
func TestSidecarBackfill_EnableSearchable_TallyMissesConcurrentWriteInMemtable(t *testing.T) {
	const numObjects = 20 // no nils - keeps the arithmetic exact and legible
	const concurrentObjText = "alpha bravo charlie delta echo foxtrot golf"
	ctx := testCtx()

	migratedShard, _, task, wrapped := newSidecarBackfillSearchableFixture(t, ctx, "SidecarBackfillConcurrentWrite", numObjects)
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	// Written after the backfill scan returns, before the post-swap
	// recompute - lands in the objects memtable.
	concurrentObj := newSidecarMarkerObject(migratedClassName, sidecarBackfillTextProp, concurrentObjText)
	require.NoError(t, migratedShard.PutObject(ctx, concurrentObj))

	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	// Control: identical n+1 objects, searchable from class creation -
	// unaffected by the bug by construction.
	controlClassName := "SidecarBackfillConcurrentWriteControl_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true},
		false, false, false)
	controlShard := controlShd.(*Shard)
	defer controlShard.Shutdown(ctx)

	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, 0)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}
	controlExtra := newSidecarMarkerObject(controlClassName, sidecarBackfillTextProp, concurrentObjText)
	require.NoError(t, controlShard.PutObject(ctx, controlExtra))

	controlSum, controlCount, controlMean, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects+1, controlCount, "sanity: control must have tallied all n+1 objects")

	migratedSum, migratedCount, migratedMean, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, controlCount, migratedCount,
		"regression (weaviate/0-weaviate-issues#322 residual): BM25 tally COUNT must include an object written to "+
			"the objects memtable between the backfill scan and the post-swap recompute - got %d, want %d (control)",
		migratedCount, controlCount)
	assert.Equal(t, controlSum, migratedSum,
		"regression (weaviate/0-weaviate-issues#322 residual): BM25 tally SUM must include an object written to "+
			"the objects memtable between the backfill scan and the post-swap recompute - got %d, want %d (control)",
		migratedSum, controlSum)
	assert.InDelta(t, controlMean, migratedMean, 0.001,
		"BM25 avgdl must include the concurrent-write object's contribution")
}

// Pins idempotency: a write racing into the objects memtable strictly
// between the recompute's flush and its cursor open (a window the flush
// fix cannot close) is not lost - a second, independent recompute must
// converge to n+1, not double-count the first n.
func TestSidecarBackfill_EnableSearchable_SecondRecomputeConvergesResidualWindowWrite(t *testing.T) {
	const numObjects = 20
	const residualObjText = "alpha bravo charlie" // 3 words
	ctx := testCtx()

	migratedShard, _, task, wrapped := newSidecarBackfillSearchableFixture(t, ctx, "SidecarBackfillResidualWindow", numObjects)
	migratedClassName := migratedShard.Index().Config.ClassName.String()

	require.NoError(t, task.RunPrepareOnShard(ctx, migratedShard))
	require.NoError(t, task.RunSwapOnShard(ctx, migratedShard))
	require.True(t, wrapped.migrationCompleted)

	firstSum, firstCount, _, err := migratedShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, firstCount, "sanity: first recompute must land on exactly the n pre-existing objects")

	// Lands strictly after the first recompute has completed and flushed.
	residualObj := newSidecarMarkerObject(migratedClassName, sidecarBackfillTextProp, residualObjText)
	require.NoError(t, migratedShard.PutObject(ctx, residualObj))

	// A second, independent recompute must land on n+1 - not 2n (broken
	// idempotency) and not n (write permanently lost).
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

// Pins weaviate/0-weaviate-issues#322's post-swap residual window: a write
// between the recompute and disableCallbacks was searchable but excluded
// from avgdl until trackMigratingPropLength/untrackMigratingPropLength closed it.

// newSidecarBackfillSearchableCallbackFixture builds a shard with a live
// filterable index (so the double-write callback machinery engages),
// numObjects pre-existing text objects, the backfill phase run with
// callbacks left active, and one recompute already performed to establish
// the "recompute ran, callbacks still active" baseline.
func newSidecarBackfillSearchableCallbackFixture(t *testing.T, ctx context.Context, classNamePrefix string, numObjects int,
) (shard *Shard, task *ShardReindexTaskGeneric, objects []*storobj.Object, firstSum, firstCount int) {
	t.Helper()

	className := classNamePrefix + "_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard = shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	objects = sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ = newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	t.Cleanup(task.disableCallbacks)

	overlay := task.strategy.AnalyzerOverlay([]string{sidecarBackfillTextProp})
	require.NoError(t, shard.recomputeSearchableTallyForProp(ctx, sidecarBackfillTextProp, overlay))

	firstSum, firstCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, firstCount, "sanity: recompute must land on exactly the n pre-existing objects")

	return shard, task, objects, firstSum, firstCount
}

// Pins the residual above: a write landing after the recompute, while
// callbacks are still active, must be reflected in the tally with no
// further recompute call.
func TestSidecarBackfill_EnableSearchable_CallbackTallyClosesResidualStrandingWindow(t *testing.T) {
	const numObjects = 20
	const residualObjText = "alpha bravo charlie delta" // 4 words
	ctx := testCtx()

	shard, _, _, firstSum, _ := newSidecarBackfillSearchableCallbackFixture(t, ctx, "SidecarBackfillCallbackTally", numObjects)
	className := shard.Index().Config.ClassName.String()

	// Lands after the recompute, while callbacks are still active.
	residualObj := newSidecarMarkerObject(className, sidecarBackfillTextProp, residualObjText)
	require.NoError(t, shard.PutObject(ctx, residualObj))

	// Must already include the residual write with no further recompute.
	secondSum, secondCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.EqualValues(t, numObjects+1, secondCount,
		"regression (weaviate/0-weaviate-issues#322 post-swap residual): the BM25 tally COUNT must include a write landing "+
			"after the recompute while the double-write callbacks are still active, with NO further recompute - "+
			"got %d, want %d", secondCount, numObjects+1)
	assert.Equal(t, firstSum+4, secondSum,
		"regression (weaviate/0-weaviate-issues#322 post-swap residual): the BM25 tally SUM must include exactly the "+
			"residual write's 4-word contribution with NO further recompute - got sum=%d after first=%d", secondSum, firstSum)
}

// Pins the trickiest edge in trackMigratingPropLength's godoc: an in-place
// edit of an already-tracked object must leave COUNT unchanged and adjust
// SUM by exactly the net word-count delta, even though the callback only
// ever sees an item-level delta, not the object's full value.
func TestSidecarBackfill_EnableSearchable_CallbackTallyHandlesInPlaceEditWithoutInflatingCount(t *testing.T) {
	const numObjects = 20
	ctx := testCtx()

	shard, _, objects, firstSum, _ := newSidecarBackfillSearchableCallbackFixture(t, ctx, "SidecarBackfillCallbackTallyEdit", numObjects)

	// objects[0]'s starting value is "alpha" (1 word); both sides non-empty,
	// so DeltaSkipSearchable computes an item-level delta, not a full swap.
	edited := objects[0]
	edited.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: "alpha bravo charlie"}
	require.NoError(t, shard.PutObject(ctx, edited))

	secondSum, secondCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.EqualValues(t, numObjects, secondCount,
		"regression (weaviate/0-weaviate-issues#322 post-swap residual): an in-place edit of an ALREADY-tracked object must "+
			"not inflate the BM25 tally COUNT (the paired Track/Untrack calls must net to zero) - got %d, want %d (unchanged)",
		secondCount, numObjects)
	assert.Equal(t, firstSum+2, secondSum,
		"regression (weaviate/0-weaviate-issues#322 post-swap residual): an in-place edit's SUM delta must equal exactly the "+
			"net word-count change (1 word -> 3 words = +2) even though the double-write callback only ever sees "+
			"an item-level delta, not the object's full value - got sum=%d after first=%d", secondSum, firstSum)
}

// Pins a known residual: trackMigratingPropLength's Length<=0 gate cannot
// distinguish a genuine empty-value edit from DeltaSkipSearchable's
// synthetic placeholder, causing an off-by-one Count on empty-value
// transitions. See the Skip message below for the fix path.
func TestSidecarBackfill_EnableSearchable_CallbackTallyOffByOneOnEmptyValueEdit(t *testing.T) {
	t.Skip("RED pin for weaviate/0-weaviate-issues#322's callback-tally-vs-empty-value-edit residual: " +
		"trackMigratingPropLength's Length<=0 gate cannot distinguish inverted.DeltaSkipSearchable's " +
		"synthetic \"this side doesn't apply\" placeholder (Items:[], Length:0 - correctly skipped, keeps " +
		"newly-appearing/disappearing props exactly right) from a GENUINE edit to/from the empty string " +
		"(also Items:[], Length:0 on the affected side) - both look identical to the gate. Net effect: " +
		"Count is off by -1 for an in-place edit TO empty (vs the live write path's net-zero) and +1 for an " +
		"edit FROM empty, for any such transition landing while this task's double-write callbacks are " +
		"active. Red when un-skipped: migratedCount = controlCount-1, deterministic. Converges on any recompute re-entry (ResetProperty + full rescan reads the object's " +
		"true current value). Exact fix needs a shard-level force-searchable overlay registry threaded into " +
		"the live write path's analysis (the surface PR weaviate/weaviate#12211's force-index overlay " +
		"machinery builds for its own callers) - wrong to duplicate that machinery here. Un-skip once that " +
		"surface (or an equivalent fix) lands; the assertion below then goes green.")

	const numObjects = 20
	ctx := testCtx()

	shard, _, objects, _, _ := newSidecarBackfillSearchableCallbackFixture(t, ctx, "SidecarBackfillCallbackTallyEmptyEdit", numObjects)

	// Clears objects[0]'s title to "" while callbacks are active - a
	// genuine Length:0 delta entry, identical in shape to the synthetic
	// placeholder the gate must also skip.
	edited := objects[0]
	edited.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: ""}
	require.NoError(t, shard.PutObject(ctx, edited))

	// Control: same edit through the always-live write path - the ground
	// truth this shard's tally must converge to.
	controlClassName := "SidecarBackfillCallbackTallyEmptyEditControl_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	controlClass := newSidecarBackfillTextClass(controlClassName, &vFalse, &vTrue)
	controlShd, _ := testShardWithSettings(t, ctx, controlClass, enthnsw.UserConfig{Skip: true}, false, false, false)
	controlShard := controlShd.(*Shard)
	t.Cleanup(func() { controlShard.Shutdown(ctx) })

	controlObjects := sidecarBackfillTextObjects(controlClassName, numObjects, 0)
	for _, obj := range controlObjects {
		require.NoError(t, controlShard.PutObject(ctx, obj))
	}
	controlEdited := controlObjects[0]
	controlEdited.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: ""}
	require.NoError(t, controlShard.PutObject(ctx, controlEdited))

	_, controlCount, _, err := controlShard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, controlCount,
		"sanity: the live write path must net Count unchanged across an edit-to-empty (subtract the old "+
			"non-empty value, track the new empty one - SetPropertyLengths tracks every searchable prop "+
			"unconditionally, including zero-length values)")

	_, migratedCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equal(t, controlCount, migratedCount,
		"BUG (gh#322 residual, pinned not fixed - see the skip message above): BM25 tally COUNT must match "+
			"the live write path's semantics across an in-place edit to the empty string - got %d, want %d "+
			"(control)", migratedCount, controlCount)
}

// Same TOCTOU as inverted.TestJsonShardMetaData_UnTrackProperty_ChecksPresenceBeforeMutating,
// exercised through the real production call path under concurrent load.

// Drives a real concurrent race between recomputeSearchableTallyForProp
// (ResetProperty + rescan) and a real delete through the live write path,
// asserting neither the write nor the tally is corrupted regardless of
// scheduler interleaving.
func TestSidecarBackfill_EnableSearchable_DeleteDuringRecomputeDoesNotFailOrCorruptTally(t *testing.T) {
	// Enough objects that the rescan performs real, measurable I/O.
	const numObjects = 100
	ctx := testCtx()

	shard, task, objects, _, _ := newSidecarBackfillSearchableCallbackFixture(t, ctx, "SidecarBackfillDeleteDuringRecompute", numObjects)

	overlay := task.strategy.AnalyzerOverlay([]string{sidecarBackfillTextProp})

	// In-place edit to empty deletes objects[0]'s only item, firing the
	// delete callback concurrently with the recompute below.
	edited := objects[0]
	edited.Object.Properties = map[string]interface{}{sidecarBackfillTextProp: ""}

	var wg sync.WaitGroup
	wg.Add(2)

	var (
		recomputeErr error
		putErr       error
	)
	go func() {
		defer wg.Done()
		recomputeErr = shard.recomputeSearchableTallyForProp(ctx, sidecarBackfillTextProp, overlay)
	}()
	go func() {
		defer wg.Done()
		putErr = shard.PutObject(ctx, edited)
	}()
	wg.Wait()

	require.NoError(t, recomputeErr, "sanity: the concurrent recompute call itself must succeed")

	assert.NoError(t, putErr,
		"regression (weaviate/0-weaviate-issues#322 delete-vs-recompute TOCTOU): a delete callback racing the post-swap tally "+
			"recompute's ResetProperty must not fail the user's write")

	sum, count, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, 0,
		"regression (weaviate/0-weaviate-issues#322 delete-vs-recompute TOCTOU): BM25 tally COUNT must never go negative when a "+
			"delete callback races the post-swap recompute - got %d", count)
	assert.GreaterOrEqual(t, sum, 0,
		"regression (weaviate/0-weaviate-issues#322 delete-vs-recompute TOCTOU): BM25 tally SUM must never go negative when a "+
			"delete callback races the post-swap recompute - got %d", sum)
}
