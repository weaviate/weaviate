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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	retirementProp            = "score"
	retirementFilterableTrack = "enable_filterable_score_1"
	retirementRangeableTrack  = "filterable_to_rangeable_score_1"
)

// plantTwoRecords gives one property a record on each of two index types, so
// a delete of one has a neighbour it must not touch.
func plantTwoRecords(t *testing.T, lsmPath string) {
	t.Helper()
	for _, name := range []string{retirementFilterableTrack, retirementRangeableTrack} {
		mkTrackerDir(t, lsmPath, name, append(completedSentinels, finalizedSentinel)...)
		mkRecoveryPayload(t, lsmPath, name, retirementProp)
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, ".migrations", name, "properties.mig"),
			[]byte(retirementProp), 0o644))
	}
}

func recordExists(t *testing.T, lsmPath, trackerName string) bool {
	t.Helper()
	return dirExistsAt(t, lsmPath, filepath.Join(".migrations", trackerName))
}

// Dropping an index removes its bucket on purpose, so the record naming that
// bucket has to go with it — a record outliving its bucket has the next shard
// load re-open an index the user deleted.
//
// The neighbour is the point: one index type's tracker scope reaches
// strategies that promote a different index's bucket, and retiring one of
// those would unshield a bucket nobody asked to delete. Retirement is scoped
// by the bucket a tracker promotes, not by the scope it falls into.
func TestIndexDeleteRetiresOnlyTheRecordOfTheBucketItRemoves(t *testing.T) {
	ctx := testCtx()
	className := "RecordRetirement_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	class.Properties[0].IndexRangeFilters = boolPtr(true)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())
	lsmPath := shard.pathLSM()

	filterableBucket := helpers.BucketFromPropNameLSM(retirementProp)
	rangeableBucket := helpers.BucketRangeableFromPropNameLSM(retirementProp)
	require.NotNil(t, shard.store.Bucket(filterableBucket))
	require.NotNil(t, shard.store.Bucket(rangeableBucket))
	plantTwoRecords(t, lsmPath)

	// The apply of a filterable-index DELETE: the merged property keeps
	// rangeable on and turns filterable off. Identical in shape to the flip
	// of a sibling migration on the same property, whose apply also arrives
	// with one index still disabled.
	dropped := &models.Property{
		Name:              retirementProp,
		DataType:          class.Properties[0].DataType,
		IndexFilterable:   boolPtr(false),
		IndexSearchable:   boolPtr(false),
		IndexRangeFilters: boolPtr(true),
	}
	eg := enterrors.NewErrorGroupWrapper(nullLogger())
	var payloadReads atomic.Int64
	shard.updatePropertyBuckets(ctx, eg, dropped, &payloadReads)
	require.NoError(t, eg.Wait())

	assert.False(t, dirExistsAt(t, lsmPath, filterableBucket), "the dropped index's bucket")
	assert.False(t, recordExists(t, lsmPath, retirementFilterableTrack),
		"its record must go with it, or the next load re-opens the bucket just deleted")

	assert.True(t, dirExistsAt(t, lsmPath, rangeableBucket), "the neighbour index's bucket")
	assert.True(t, recordExists(t, lsmPath, retirementRangeableTrack),
		"the neighbour's record is what keeps its bucket out of the next start's sweep")
}

// Same delete against a tenant that is not loaded. The cold path removes bucket
// dirs by name and reads nothing else, so the record needs its own removal
// here — otherwise reactivating the tenant re-opens the deleted index.
func TestUnloadedIndexDeleteRetiresOnlyTheRecordOfTheBucketItRemoves(t *testing.T) {
	const coldTenant = "cold-tenant"

	ctx := testCtx()
	className := "ColdRecordRetirement_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	class.Properties[0].IndexRangeFilters = boolPtr(true)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer shd.Shutdown(context.Background())

	coldLSM := shardPathLSM(idx.path(), coldTenant)
	require.NoError(t, os.MkdirAll(coldLSM, 0o755))
	filterableBucket := helpers.BucketFromPropNameLSM(retirementProp)
	rangeableBucket := helpers.BucketRangeableFromPropNameLSM(retirementProp)
	mkSidecarDir(t, coldLSM, filterableBucket)
	mkSidecarDir(t, coldLSM, rangeableBucket)
	plantTwoRecords(t, coldLSM)

	cold := NewLazyLoadShard(ctx, nil, coldTenant, idx, class, idx.centralJobQueue,
		idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
		false, idx.bitmapBufPool)
	idx.shards.Store(coldTenant, cold)
	defer func() {
		if cold.isLoaded() {
			require.NoError(t, cold.Shutdown(context.Background()))
		}
	}()

	dropped := &models.Property{
		Name:              retirementProp,
		DataType:          class.Properties[0].DataType,
		IndexFilterable:   boolPtr(false),
		IndexSearchable:   boolPtr(false),
		IndexRangeFilters: boolPtr(true),
	}
	eg := enterrors.NewErrorGroupWrapper(nullLogger())
	cold.updateUnloadedPropertyBuckets(ctx, eg, dropped)
	require.NoError(t, eg.Wait())

	assert.False(t, cold.isLoaded(), "a delete on a cold tenant must not hydrate it")
	assert.False(t, dirExistsAt(t, coldLSM, filterableBucket))
	assert.False(t, recordExists(t, coldLSM, retirementFilterableTrack))
	assert.True(t, dirExistsAt(t, coldLSM, rangeableBucket))
	assert.True(t, recordExists(t, coldLSM, retirementRangeableTrack))
}

// The end-of-swap trim removes what older generations of the same property
// left on disk. A record is not leftovers: it says an index is promoted and
// not yet advertised, and only an owner that knows why — the first start
// after the flip, an index drop — may retire it. A re-run of the same property
// passing by is not one of them; it supersedes the record through finalize's
// older-generation arm at the next start instead.
func TestEndOfSwapTrimKeepsARecordAndRemovesPlainLeftovers(t *testing.T) {
	const propName = "title"

	ctx := testCtx()
	className := "TrimRecord_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())
	lsmPath := shard.pathLSM()

	recorded := append(append([]string{}, completedSentinels...), finalizedSentinel)
	mkTrackerDir(t, lsmPath, "enable_filterable_title_1", recorded...)
	mkTrackerDir(t, lsmPath, "enable_filterable_title_2", completedSentinels...)

	// The generation-3 run that just tidied, trimming everything older.
	task := NewShardReindexTaskGeneric("EnableFilterable", idx.logger,
		&EnableFilterableStrategy{propNames: []string{propName}, generation: 3},
		reindexTaskConfig{}, &UuidKeyParser{}, uuidObjectsIteratorAsync)
	task.trimOlderGenerationsLocked(idx.logger, shard, nil, []string{propName})

	assert.True(t, recordExists(t, lsmPath, "enable_filterable_title_1"),
		"a record retires through a named owner, not through a re-run passing by")
	assert.False(t, recordExists(t, lsmPath, "enable_filterable_title_2"),
		"an older generation with no record is exactly what the trim is for")
}

// The sweeps that run at submit, at cancel, and when a task ends FAILED or
// CANCELLED share their machinery with the DELETE path but have the opposite
// need: the record, its bucket and its payload are the protected residue the
// terminal contract promises, retired later by a re-submit, an index drop, or
// the first start after a flip.
func TestTerminalSweepLeavesACompletedMigrationsResidueInPlace(t *testing.T) {
	ctx := testCtx()
	className := "ResidueContract_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	class.Properties[0].IndexRangeFilters = boolPtr(true)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())
	lsmPath := shard.pathLSM()
	plantTwoRecords(t, lsmPath)

	for _, indexType := range []string{"filterable", "rangeable"} {
		cleanSweep(t, ctx, shard, retirementProp, indexType)
	}

	for _, tracker := range []string{retirementFilterableTrack, retirementRangeableTrack} {
		assert.Truef(t, recordExists(t, lsmPath, tracker), "record %q", tracker)
		assert.FileExists(t, filepath.Join(lsmPath, ".migrations", tracker, reindexRecoveryPayloadFile),
			"the payload names the task, which is what makes the residue reconcilable later")
	}
	assert.True(t, dirExistsAt(t, lsmPath, helpers.BucketFromPropNameLSM(retirementProp)))
	assert.True(t, dirExistsAt(t, lsmPath, helpers.BucketRangeableFromPropNameLSM(retirementProp)))
}
