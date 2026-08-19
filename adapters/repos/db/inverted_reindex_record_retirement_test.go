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

// recordShapes are the two shapes a completed migration's tracker has when an
// index DELETE reaches it: before the next load has recorded the promotion, and
// after. A DELETE owes the same answer to both, since only the load that has not
// happened yet tells them apart.
var recordShapes = []struct {
	name      string
	sentinels []string
}{
	{name: "promotion not recorded yet", sentinels: completedSentinels},
	{name: "promotion recorded", sentinels: recordedSentinels},
}

// plantTwoCompletedMigrations gives one property a completed migration on each
// of two index types, so a delete of one has a neighbour it must not touch.
func plantTwoCompletedMigrations(t *testing.T, lsmPath string, sentinels []string) {
	t.Helper()
	for _, name := range []string{retirementFilterableTrack, retirementRangeableTrack} {
		mkTrackerDir(t, lsmPath, name, sentinels...)
		mkRecoveryPayload(t, lsmPath, name, retirementProp)
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, ".migrations", name, "properties.mig"),
			[]byte(retirementProp), 0o644))
	}
}

func trackerExists(t *testing.T, lsmPath, trackerName string) bool {
	t.Helper()
	return dirExistsAt(t, lsmPath, filepath.Join(".migrations", trackerName))
}

// A dropped index's record must go with its bucket, or the next load
// reopens a deleted index. But retirement is scoped by the bucket a tracker
// promotes, not by its tracker scope: one index type's scope reaches
// strategies that promote a different index's bucket, and retiring those too
// would unshield a bucket nobody asked to delete.
func TestIndexDeleteRetiresOnlyTheRecordOfTheBucketItRemoves(t *testing.T) {
	for _, shape := range recordShapes {
		t.Run(shape.name, func(t *testing.T) {
			indexDeleteRetiresOnlyItsOwnRecord(t, shape.sentinels)
		})
	}
}

func indexDeleteRetiresOnlyItsOwnRecord(t *testing.T, sentinels []string) {
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
	plantTwoCompletedMigrations(t, lsmPath, sentinels)

	// A filterable-index DELETE apply: rangeable stays on, filterable turns
	// off — the same shape as a sibling migration's flip landing with one
	// index still disabled.
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
	assert.False(t, trackerExists(t, lsmPath, retirementFilterableTrack),
		"its record must go with it, or the next load re-opens the bucket just deleted")

	assert.True(t, dirExistsAt(t, lsmPath, rangeableBucket), "the neighbour index's bucket")
	assert.True(t, trackerExists(t, lsmPath, retirementRangeableTrack),
		"the neighbour's record is what keeps its bucket out of the next start's sweep")
}

// Same delete on a cold tenant: the path removes bucket dirs by name only, so
// the record needs its own removal, or reactivating the tenant reopens the
// deleted index.
func TestUnloadedIndexDeleteRetiresOnlyTheRecordOfTheBucketItRemoves(t *testing.T) {
	for _, shape := range recordShapes {
		t.Run(shape.name, func(t *testing.T) {
			unloadedIndexDeleteRetiresOnlyItsOwnRecord(t, shape.sentinels)
		})
	}
}

func unloadedIndexDeleteRetiresOnlyItsOwnRecord(t *testing.T, sentinels []string) {
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
	plantTwoCompletedMigrations(t, coldLSM, sentinels)

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
	assert.False(t, trackerExists(t, coldLSM, retirementFilterableTrack))
	assert.True(t, dirExistsAt(t, coldLSM, rangeableBucket))
	assert.True(t, trackerExists(t, coldLSM, retirementRangeableTrack))
}

// A record is not leftovers the end-of-swap trim may remove: only a named
// owner (schema flip, index drop) retires it. A same-property re-run
// supersedes it through finalize's older-generation arm at the next start
// instead.
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

	mkTrackerDir(t, lsmPath, "enable_filterable_title_1", recordedSentinels...)
	mkTrackerDir(t, lsmPath, "enable_filterable_title_2", completedSentinels...)

	// The generation-3 run that just tidied, trimming everything older.
	task := NewShardReindexTaskGeneric("EnableFilterable", idx.logger,
		&EnableFilterableStrategy{propNames: []string{propName}, generation: 3},
		reindexTaskConfig{}, &UuidKeyParser{}, uuidObjectsIteratorAsync)
	task.trimOlderGenerationsLocked(idx.logger, shard, nil, []string{propName})

	assert.True(t, trackerExists(t, lsmPath, "enable_filterable_title_1"),
		"a record retires through a named owner, not through a re-run passing by")
	assert.False(t, trackerExists(t, lsmPath, "enable_filterable_title_2"),
		"an older generation with no record is exactly what the trim is for")
}

// Submit, cancel, and FAILED/CANCELLED sweeps share machinery with DELETE but
// have the opposite need: the record, bucket, and payload are protected
// residue, retired later by a re-submit, an index drop, or the first start
// after a flip.
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
	plantTwoCompletedMigrations(t, lsmPath, recordedSentinels)

	for _, indexType := range []string{"filterable", "rangeable"} {
		cleanSweep(t, ctx, shard, retirementProp, indexType)
	}

	for _, tracker := range []string{retirementFilterableTrack, retirementRangeableTrack} {
		assert.Truef(t, trackerExists(t, lsmPath, tracker), "record %q", tracker)
		assert.FileExists(t, filepath.Join(lsmPath, ".migrations", tracker, reindexRecoveryPayloadFile),
			"the payload names the task, which is what makes the residue reconcilable later")
	}
	assert.True(t, dirExistsAt(t, lsmPath, helpers.BucketFromPropNameLSM(retirementProp)))
	assert.True(t, dirExistsAt(t, lsmPath, helpers.BucketRangeableFromPropNameLSM(retirementProp)))
}
