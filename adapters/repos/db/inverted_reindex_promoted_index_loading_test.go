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
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The record names the buckets a completed migration produced. The class the
// shard loads from must carry those index flags on, or the shard comes up
// without the buckets it just rebuilt — and leaves the real schema alone,
// which is what every other reader on the node answers from.
func TestClassWithPromotedIndexesForcesOnlyTheRecordedFlags(t *testing.T) {
	off := false
	base := &models.Class{Class: "Promoted", Properties: []*models.Property{
		{Name: "alpha", IndexFilterable: &off, IndexSearchable: &off, IndexRangeFilters: &off},
		{Name: "beta", IndexFilterable: &off},
	}}
	promoted := map[string]map[string]struct{}{
		"alpha": {"filterable": {}, "rangeable": {}},
	}

	effective := classWithPromotedIndexes(base, promoted)

	require.NotSame(t, base, effective, "the schema every other reader answers from must be left alone")
	assert.False(t, *base.Properties[0].IndexFilterable, "the input class must be unchanged")

	assert.True(t, *effective.Properties[0].IndexFilterable)
	assert.True(t, *effective.Properties[0].IndexRangeFilters)
	assert.False(t, *effective.Properties[0].IndexSearchable,
		"a flag no record names stays as the schema has it")
	assert.False(t, *effective.Properties[1].IndexFilterable,
		"a property no record names is untouched")

	assert.Same(t, base, classWithPromotedIndexes(base, nil),
		"no records means no copy")
}

// finalizedMigrationIndexes reads only records, never the trackers of a
// migration still in flight, one whose strategy flips no flag, or one whose
// completion the shield can no longer read.
func TestFinalizedMigrationIndexesReadsOnlyRecords(t *testing.T) {
	lsmPath := t.TempDir()
	plant := func(migName, props string, sentinels ...string) {
		mkTrackerDir(t, lsmPath, migName, sentinels...)
		require.NoError(t, os.WriteFile(
			filepath.Join(lsmPath, ".migrations", migName, "properties.mig"),
			[]byte(props), 0o644))
	}

	plant("enable_filterable_alpha_beta_1", "alpha,beta", recordedSentinels...)
	plant("filterable_to_rangeable_alpha_1", "alpha", recordedSentinels...)
	// In flight: no record, so nothing is promoted for it yet.
	plant("enable_searchable_gamma_1", "gamma", "started.mig")
	// A strategy that flips no flag never gets a record; one planted by hand
	// still names no index to force on.
	plant("rebuild_searchable_delta_1", "delta", recordedSentinels...)
	// A tracker dir removal that stopped partway can leave the marker without
	// the completion the shield reads, so forcing this flag on would open a
	// bucket the next sweep is free to delete.
	plant("enable_filterable_epsilon_1", "epsilon", finalizedSentinel)

	assert.Equal(t, map[string]map[string]struct{}{
		"alpha": {"filterable": {}, "rangeable": {}},
		"beta":  {"filterable": {}},
	}, finalizedMigrationIndexes(lsmPath))
}

// A bucket the shard does not open is invisible to reads and to the backup
// path, which enumerates the store's loaded buckets. So is the null and length
// state of the same property, which the shard only builds for a property that
// has some inverted index — and during this window the schema says it has
// none.
func TestShardOpensAPromotedIndexAndItsSidecarStateBeforeTheFlip(t *testing.T) {
	const (
		propName   = "title"
		numObjects = 10
	)
	ctx := testCtx()
	className := "PromotedLoading_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.NoError(t, shard.Shutdown(ctx))

	idx.shardReindexer = NewShardReindexerV3Noop()
	loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	idx.shards.Store(shardName, loaded)
	live := loaded.(*Shard)
	defer live.Shutdown(context.Background())

	require.False(t, *class.Properties[0].IndexFilterable,
		"the flip has not landed; the whole point is what the shard does before it")

	valueBucket := helpers.BucketFromPropNameLSM(propName)
	assert.NotNil(t, live.store.Bucket(valueBucket),
		"the promoted index must be open, not merely on disk")
	assert.NotNil(t, live.store.Bucket(helpers.BucketFromPropNameNullLSM(propName)),
		"a write in this window analyses the null state of the property and needs its bucket")
	assert.NotNil(t, live.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName)),
		"and its property-length bucket")

	files, err := live.store.ListFiles(context.Background(), live.path())
	require.NoError(t, err)
	inBackup := func(prefix string) bool {
		for _, f := range files {
			if strings.HasPrefix(f, prefix) {
				return true
			}
		}
		return false
	}
	assert.True(t, inBackup(filepath.Join("lsm", valueBucket)),
		"a backup taken in this window must carry the promoted index")
	assert.True(t, inBackup(filepath.Join("lsm", ".migrations")),
		"and the record that says what it is, so a restore knows too")
}
