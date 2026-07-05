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

// newNoLiveIndexRangeableTestClass builds the FilterableToRangeable class with
// the numeric property carrying NO live inverted index at all: IndexFilterable
// forced false (the base builder leaves it nil → defaults true) and
// IndexRangeFilters left nil → false. AnalyzeObject therefore skips the
// property on ordinary writes, which is precisely the "absent input" case the
// enable-rangeable double-write must cover.
func newNoLiveIndexRangeableTestClass(className string) *models.Class {
	c := newFilterableToRangeableTestClass(className)
	noIndex := false
	c.Properties[0].IndexFilterable = &noIndex
	return c
}

// readRangeableIDs returns the docIDs stored under a single int64 value in a
// RoaringSetRange bucket. Sibling of filterableToRangeableFingerprint, but for
// one caller-chosen value (the concurrent write uses a value outside the 0..N
// corpus so it can be queried in isolation).
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

// TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost is the
// enable-an-index shape of weaviate/0-weaviate-issues#298 (the retokenize
// shapes live in inverted_reindex_write_during_finalize_test.go). The property
// has NO live inverted index, so the ordinary write path's AnalyzeObject skips
// it and the inline double-write has nothing to mirror — only the migration
// pass, analyzing under the strategy's ForceRangeable overlay, makes the
// property present and writes it to the new rangeable bucket.
//
// RED before the fix: with no migration double-write the concurrent value is
// silently dropped and a range query misses it after the swap.
//
// Positive control: the iterator-backfilled corpus IS present in the same
// bucket, proving the target-population path works and the concurrent write's
// absence is specifically the enable-X double-write failing.
func TestReindex_ConcurrentWriteDuringEnableRangeable_NotLost(t *testing.T) {
	ctx := testCtx()
	const propName = filterableToRangeablePropName
	const numObjects = 25
	// Distinct from the 0..(filterableToRangeableNumDistinctValues-1) corpus so
	// its posting list is unambiguously the concurrent write.
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

	// Concurrent write in the FINALIZING window, value outside the corpus.
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

	// Positive control: the backfilled corpus reached the rangeable bucket.
	require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
		"positive control: iterator-backfilled corpus (value 0) must be present in the rangeable bucket; "+
			"got none — the migration never populated it and the assertion below would prove nothing")

	// The concurrent write's value must survive the swap.
	ids := readRangeableIDs(t, rangeBucket, concurrentValue)
	assert.NotEmptyf(t, ids,
		"#298 enable-rangeable: object written during the reindex window is NOT under the target rangeable value "+
			"%d — its ForceRangeable double-write was lost, so a range query misses it after the swap", concurrentValue)
}
