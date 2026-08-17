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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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

// newNoNullStateRangeableTestClass is newNoLiveIndexRangeableTestClass
// without null-state and property-length indexing. A property with no live
// index has neither bucket, and nothing creates them when a migration turns
// its index on, so a write once the property is indexed fails on the missing
// bucket. That gap is tracked separately; tests about the write window use
// this variant so they fail on their own subject.
//
// A separate variant, not a change to the shared fixture: the shared one
// also backs the swap-window double-write guards, which need the buckets.
func newNoNullStateRangeableTestClass(className string) *models.Class {
	c := newNoLiveIndexRangeableTestClass(className)
	c.InvertedIndexConfig.IndexNullState = false
	c.InvertedIndexConfig.IndexPropertyLength = false
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

// TestReindex_WriteAfterEnableRangeableSwap_NotLost pins that writes
// between this shard's swap and the cluster-wide schema flip must still
// reach the rangeable bucket, not be silently dropped.
func TestReindex_WriteAfterEnableRangeableSwap_NotLost(t *testing.T) {
	const propName = filterableToRangeablePropName
	const numObjects = 25
	// Outside the corpus so its posting list is unambiguously this write.
	const postSwapValue = int64(5150)

	for _, tc := range []struct {
		name         string
		wireOverlay  bool
		wantIndexed  bool
		explainEmpty string
	}{
		{
			name:        "overlay wired: the write reaches the rangeable bucket",
			wireOverlay: true,
			wantIndexed: true,
			explainEmpty: "object written after the swap is NOT under rangeable value %d — " +
				"the double-write is gone and the schema flip has not landed, so the write was silently dropped from the index",
		},
		{
			name:        "overlay not wired: the write is silently dropped",
			wireOverlay: false,
			wantIndexed: false,
			explainEmpty: "without the overlay the write must be missing from rangeable value %d; " +
				"finding it means some other path already covers the window and the overlay is not what this test claims",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "EnableRangeablePostSwap_" + uuid.NewString()[:8]
			class := newNoNullStateRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			if tc.wireOverlay {
				payload := &ReindexTaskPayload{
					MigrationType: ReindexTypeEnableRangeable,
					Properties:    []string{propName},
				}
				require.True(t, maybeWirePerPropOverlaySet(shard, payload,
					[]*ShardReindexTaskGeneric{task}))
			}

			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			// The live schema still says IndexRangeFilters=false here: the
			// flip is the provider's job and has not run.
			require.False(t, inverted.HasRangeableIndex(class.Properties[0]),
				"the window this test covers only exists while the flag is still false")

			require.NoError(t, shard.PutObject(ctx, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:         strfmt.UUID(uuid.NewString()),
					Class:      className,
					Properties: map[string]interface{}{propName: postSwapValue},
				},
			}))

			rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
			require.NotNil(t, rangeBucket, "post-swap rangeable bucket must exist")
			require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
				"positive control: the backfilled corpus (value 0) must be present, else the assertion below proves nothing")

			ids := readRangeableIDs(t, rangeBucket, postSwapValue)
			if tc.wantIndexed {
				assert.NotEmptyf(t, ids, tc.explainEmpty, postSwapValue)
			} else {
				assert.Emptyf(t, ids, tc.explainEmpty, postSwapValue)
			}
		})
	}
}

// TestRangeableWriteOverlay_ClearsOnceTheFlipLands pins the overlay's exit:
// once the live schema carries the flag the entry is redundant, so the write
// path drops it rather than paying for it on every object.
func TestRangeableWriteOverlay_ClearsOnceTheFlipLands(t *testing.T) {
	trueVal := true
	props := []*models.Property{{
		Name:     "price",
		DataType: schema.DataTypeInt.PropString(),
	}}

	s := &Shard{}
	s.SetRangeableWriteOverlay("price")
	require.Equal(t, map[string]inverted.PropertyOverlay{"price": {ForceRangeable: true}},
		s.writeAnalyzerOverlay(props),
		"flag still false: the overlay must force the write into the rangeable bucket")

	props[0].IndexRangeFilters = &trueVal
	assert.Nil(t, s.writeAnalyzerOverlay(props),
		"flag now live: the overlay adds nothing and must not be projected")
	assert.Nil(t, s.SnapshotRangeableWriteOverlay([]string{"price"}),
		"the redundant entry must be dropped, not re-evaluated on every write")
}
