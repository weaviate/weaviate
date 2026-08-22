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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindex_ConcurrentWriteInRegistrationGap_NotLost pins
// weaviate/weaviate#11688: a write that lands before the double-write mirror is
// armed reaches no mirror, so the iteration horizon has to be fixed after the
// arming. Fixed before it, the backfill skips the same write
// (LastUpdateTimeUnix >= horizon) and it is lost for good.
func TestReindex_ConcurrentWriteInRegistrationGap_NotLost(t *testing.T) {
	// The horizon's other half: it delegates everything at or after it to the
	// mirror, so a mirror directory that a sweep removed takes exactly those
	// writes with it. The reverse edge has to take them back, and it has to
	// fire on a record that has no checkpoint to vouch for anything yet.
	sweepTheMirrorDirectory := func(t *testing.T, ctx context.Context, shard *Shard,
		task *ShardReindexTaskGeneric, class *models.Class, propName string,
	) {
		t.Helper()
		ingest := task.ingestBucketName(propName)
		require.NoError(t, shard.store.ShutdownBucket(ctx, ingest))
		require.NoError(t, os.RemoveAll(filepath.Join(shard.pathLSM(), ingest)))

		shard.reconcileMigrationRecords(ctx, class)
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	}

	tests := []struct {
		name    string
		disrupt func(*testing.T, context.Context, *Shard, *ShardReindexTaskGeneric, *models.Class, string)
	}{
		{name: "the rebuild runs with everything the mirror staged still on disk"},
		{name: "the mirror's directory is swept before the rebuild runs", disrupt: sweepTheMirrorDirectory},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRegistrationGapWritesSurvive(t, tt.disrupt)
		})
	}
}

func testRegistrationGapWritesSurvive(t *testing.T,
	disrupt func(*testing.T, context.Context, *Shard, *ShardReindexTaskGeneric, *models.Class, string),
) {
	const (
		numObjects        = 25
		numGapUpdates     = 10 // updated inside the (old) gap via the hook
		numPostInitUpdate = 5  // updated after callbacks are active
		gapValueBase      = int64(1000)
		postValueBase     = int64(2000)
	)
	const propName = filterableToRangeablePropName

	ctx := testCtx()
	className := "EnableRangeableGapWrites_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	objs := makeFilterableToRangeableTestObjects(t, numObjects, className)
	for _, obj := range objs {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// update sets a CURRENT LastUpdateTimeUnix — what makes the backfill
	// iterator skip the object once it is >= reindexStarted.
	update := func(i int, val int64) {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 objs[i].ID(),
				Class:              className,
				Properties:         map[string]interface{}{propName: val},
				CreationTimeUnix:   time.Now().UnixMilli(),
				LastUpdateTimeUnix: time.Now().UnixMilli(),
			},
		}))
	}

	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)

	// Land the gap writes exactly where #11688 loses them: after the ingest
	// buckets exist but before the mirror is armed.
	gapWritesDone := false
	origRegister := task.registerDoubleWriteCallbacksFn
	task.registerDoubleWriteCallbacksFn = func(shard *Shard, props []string,
		bucketNamer func(string) string,
	) func() {
		for i := 0; i < numGapUpdates; i++ {
			update(i, gapValueBase+int64(i))
		}
		gapWritesDone = true
		return origRegister(shard, props, bucketNamer)
	}

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	require.True(t, gapWritesDone, "registration wrapper must have fired during OnAfterLsmInit")

	// Callbacks are registered now; these updates must reach the rangeable
	// bucket via the double-write path (the iterator will skip them).
	for i := numGapUpdates; i < numGapUpdates+numPostInitUpdate; i++ {
		update(i, postValueBase+int64(i))
	}

	if disrupt != nil {
		disrupt(t, ctx, shard, task, class, propName)
	}

	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, wrapped.migrationCompleted, "migration must complete")

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rangeBucket, "post-migration rangeable bucket must exist")

	require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
		"positive control: iterator-backfilled corpus value 0 must be present")

	// Gap writes: kept only because the horizon is fixed after the arming.
	for i := 0; i < numGapUpdates; i++ {
		val := gapValueBase + int64(i)
		assert.Lenf(t, readRangeableIDs(t, rangeBucket, val), 1,
			"gap-updated object %d must survive under value %d — a miss means "+
				"the arm-to-horizon gap lost it", i, val)
	}

	// Post-registration writes: served only via the double-write path.
	for i := numGapUpdates; i < numGapUpdates+numPostInitUpdate; i++ {
		val := postValueBase + int64(i)
		assert.Lenf(t, readRangeableIDs(t, rangeBucket, val), 1,
			"post-registration-updated object %d must survive under value %d "+
				"via the double-write path", i, val)
	}

	// Convergence: every object must appear exactly once — no lost rows, no
	// ghosts under a second value.
	seen := map[uint64]int{}
	countValue := func(v int64) {
		for _, id := range readRangeableIDs(t, rangeBucket, v) {
			seen[id]++
		}
	}
	for v := int64(0); v < filterableToRangeableNumDistinctValues; v++ {
		countValue(v)
	}
	for i := 0; i < numGapUpdates; i++ {
		countValue(gapValueBase + int64(i))
	}
	for i := numGapUpdates; i < numGapUpdates+numPostInitUpdate; i++ {
		countValue(postValueBase + int64(i))
	}
	assert.Len(t, seen, numObjects,
		"every object must be present exactly once across the expected values")
	for id, n := range seen {
		assert.Equalf(t, 1, n, "docID %d appears under %d values (ghost)", id, n)
	}
}
