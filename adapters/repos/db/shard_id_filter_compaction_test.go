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
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The doc id of an object that was deleted, or updated to a new doc id, stays
// out of an id filter through the compaction of the _id property bucket.
//
// Every class keeps the _id property in a bucket of the Set strategy, and the
// shard takes the old doc id out of it with SetDeleteSingle when the object is
// deleted, or updated in a way that gives it a new doc id (a new vector). The
// Set compactor used to drop the tombstone of a value deleted in the same
// segment it was added in, and keep the value, when the key lived in only one
// of the two segments of the compaction that takes the oldest segment in: the
// old doc id was back in the id filter, for good. The objects bucket no longer
// resolves it, so a search does not return a deleted object; what is wrong is
// the filter's set of doc ids.
func TestIDFilterKeepsOldDocIDsOutThroughCompaction(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: "IDFilterCompaction",
		Properties: []*models.Property{{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}},
	}
	const (
		deleteObject = "deleted"
		updateObject = "updated to a new doc id"
	)
	tests := []struct {
		name   string
		change string
		// putAt and changeAt are the segments of the _id bucket, oldest
		// first, the object is put in and then deleted or updated in
		putAt, changeAt int
	}{
		{name: "deleted, the newer segment the only one with its id", change: deleteObject, putAt: 1, changeAt: 1},
		{name: "deleted, the older segment the only one with its id", change: deleteObject, putAt: 0, changeAt: 0},
		{name: "updated to a new doc id, the newer segment the only one with its id", change: updateObject, putAt: 1, changeAt: 1},
		{name: "updated to a new doc id, the older segment the only one with its id", change: updateObject, putAt: 0, changeAt: 0},
		{name: "put in the older segment, deleted in the newer", change: deleteObject, putAt: 0, changeAt: 1},
		{name: "put in the older segment, updated to a new doc id in the newer", change: updateObject, putAt: 0, changeAt: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardLike, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false)
			shard := underlyingShard(t, shardLike)
			// flushes and compactions happen when the test says, not on the
			// index's own cycles
			cycles := shard.index.cycleCallbacks
			for _, cycle := range []cyclemanager.CycleManager{cycles.compactionCycle, cycles.compactionAuxCycle, cycles.flushCycle} {
				require.True(t, <-cycle.Stop(ctx))
			}

			bucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(filters.InternalPropID))
			require.NotNil(t, bucket)
			require.Equal(t, lsmkv.StrategySetCollection, bucket.Strategy())

			id := strfmt.UUID(uuid.NewString())
			var want []uint64
			for segment := 0; segment < 2; segment++ {
				// every segment holds another object, so that it is written
				putIDFilterObject(t, ctx, shardLike, class, strfmt.UUID(uuid.NewString()), []float32{1, 0})
				if segment == tt.putAt {
					putIDFilterObject(t, ctx, shardLike, class, id, []float32{1, 0})
				}
				if segment == tt.changeAt {
					oldDocID := docIDOf(t, shard, id)
					switch tt.change {
					case deleteObject:
						require.NoError(t, shardLike.DeleteObject(ctx, id, time.Now()))
					case updateObject:
						putIDFilterObject(t, ctx, shardLike, class, id, []float32{0, 1})
						newDocID := docIDOf(t, shard, id)
						require.NotEqual(t, oldDocID, newDocID, "a new vector gives the object a new doc id")
						want = []uint64{newDocID}
					}
				}
				require.NoError(t, bucket.FlushAndSwitch())
			}
			require.Len(t, segmentFilesOf(t, bucket), 2)
			requireIDFilterDocIDs(t, ctx, shard, class, id, want, "flushed")

			shard.cycleCallbacks.compactionCallbacks.CycleCallback(func() bool { return false })
			require.Len(t, segmentFilesOf(t, bucket), 1, "the two segments of the _id bucket are compacted into one")
			requireIDFilterDocIDs(t, ctx, shard, class, id, want, "compacted")
		})
	}
}

func putIDFilterObject(t *testing.T, ctx context.Context, shard ShardLike, class *models.Class,
	id strfmt.UUID, vector []float32,
) {
	t.Helper()
	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         id,
			Class:      class.Class,
			Properties: map[string]interface{}{"name": "same"},
		},
		Vector: vector,
	}))
}

// requireIDFilterDocIDs checks the doc ids an id filter gives, and that a
// search with it returns the object only while it is there.
func requireIDFilterDocIDs(t *testing.T, ctx context.Context, shard *Shard, class *models.Class,
	id strfmt.UUID, want []uint64, at string,
) {
	t.Helper()
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On:       &filters.Path{Class: schema.ClassName(class.Class), Property: filters.InternalPropID},
		Value:    &filters.Value{Value: id.String(), Type: schema.DataTypeText},
	}}

	found, _, err := shard.ObjectSearch(ctx, 10, filter, nil, nil, nil, additional.Properties{}, nil)
	require.NoError(t, err)
	var wantIDs, foundIDs []strfmt.UUID
	if len(want) > 0 {
		wantIDs = []strfmt.UUID{id}
	}
	for _, obj := range found {
		foundIDs = append(foundIDs, obj.ID())
	}
	require.Equal(t, wantIDs, foundIDs, "search, %s", at)

	allowList, err := shard.buildAllowList(ctx, filter, additional.Properties{})
	require.NoError(t, err)
	defer allowList.Close()
	require.ElementsMatch(t, want, allowList.Slice(), "doc ids, %s", at)
}

func segmentFilesOf(t *testing.T, bucket *lsmkv.Bucket) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(bucket.GetDir(), "segment-*.db"))
	require.NoError(t, err)
	return files
}
