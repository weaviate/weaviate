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
	"encoding/binary"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// newNoLiveIndexRangeableTestClass forces IndexFilterable false, so the
// property has no live index and only the double-write can cover it.
func newNoLiveIndexRangeableTestClass(className string) *models.Class {
	c := newFilterableToRangeableTestClass(className)
	noIndex := false
	c.Properties[0].IndexFilterable = &noIndex
	return c
}

// readRangeableIDs returns docIDs for one int64 value in a RoaringSetRange
// bucket; sibling of filterableToRangeableFingerprint for a single value.
func readRangeableIDs(t *testing.T, b *lsmkv.Bucket, v int64) []uint64 {
	t.Helper()
	require.Equal(t, lsmkv.StrategyRoaringSetRange, b.Strategy(),
		"readRangeableIDs requires a RoaringSetRange bucket")
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	lex, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost pins
// weaviate/0-weaviate-issues#298: a write to a no-live-index property during
// an enable-rangeable migration must survive the swap via the double-write.
func TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	const numObjects = 25
	// Outside the corpus so its posting list is unambiguously this write.
	const concurrentValue = int64(4242)

	className := "EnableRangeableConc_" + uuid.NewString()[:8]
	class := newNoLiveIndexRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	// Drive to reindexed-but-not-swapped: iterator done, double-write live.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	// The concurrent write: lands in the FINALIZING window, before swap.
	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{propName: concurrentValue},
		},
	}))

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rangeBucket, "post-swap rangeable bucket must exist")

	require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
		"positive control: iterator-backfilled corpus (value 0) must be present in the rangeable bucket; "+
			"got none — the migration never populated it and the assertion below would prove nothing")

	ids := readRangeableIDs(t, rangeBucket, concurrentValue)
	assert.NotEmptyf(t, ids,
		"#298 enable-rangeable: object written during the reindex window is NOT under the target rangeable value "+
			"%d — its ForceRangeable double-write was lost, so a range query misses it after the swap", concurrentValue)
}
