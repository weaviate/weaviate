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

// TestReindex_ConcurrentWriteInRegistrationGap_NotLost is the regression test
// for the markStarted→registerDoubleWriteCallbacks write-loss window
// (weaviate/weaviate#11688). Ported from 323bcf22f5 (originally on
// columnar-v2, enable-columnar) and adapted to the enable-rangeable
// scaffolding on this branch.
//
// Pre-fix, onAfterLsmInitWithTracker captured the reindexStarted timestamp
// BEFORE loading the ingest buckets and registering the double-write
// callbacks. A write landing in that window was
//
//	(a) skipped by the backfill iterator, because its LastUpdateTimeUnix is
//	    >= reindexStarted (the iterator assumes such writes are
//	    double-written), AND
//	(b) NOT double-written into the ingest bucket, because the callbacks
//	    were not registered yet
//
// so the row never reached the target bucket — permanently missing after
// the migration reported FINISHED.
//
// The onBeforeDoubleWriteRegistration hook fires at the exact spot of the old
// gap (after the old markStarted position, before callback registration).
// Updates injected there change each object's score to a value outside the
// corpus; each MUST survive under its new value after the swap. Against the
// pre-fix ordering the gap assertions fail with numGapUpdates values missing;
// with the fix (timestamp captured after registration, ceiled up one ms) they
// pass because reindexStarted now sorts strictly after every gap write, so the
// iterator backfills them.
//
// A second batch of updates lands after OnAfterLsmInit returns (callbacks
// active, iteration not yet started); those survive only via the double-write
// path (the iterator skips them), pinning the mechanism the iterator-skip
// predicate relies on.
func TestReindex_ConcurrentWriteInRegistrationGap_NotLost(t *testing.T) {
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

	// update overwrites object i with a fresh score and a CURRENT
	// LastUpdateTimeUnix — exactly what a live PATCH/PUT does. The timestamp
	// is what makes the backfill iterator skip the object once it is
	// >= reindexStarted.
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

	gapWritesDone := false
	task.onBeforeDoubleWriteRegistration = func() {
		for i := 0; i < numGapUpdates; i++ {
			update(i, gapValueBase+int64(i))
		}
		gapWritesDone = true
	}

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	require.True(t, gapWritesDone, "hook must have fired during OnAfterLsmInit")

	// Callbacks are registered now; these updates must reach the rangeable
	// bucket via the double-write path (the iterator will skip them).
	for i := numGapUpdates; i < numGapUpdates+numPostInitUpdate; i++ {
		update(i, postValueBase+int64(i))
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

	// Positive control: an unmodified corpus value must be backfilled, else
	// the migration populated nothing and the gap assertions prove nothing.
	require.NotEmptyf(t, readRangeableIDs(t, rangeBucket, 0),
		"positive control: iterator-backfilled corpus value 0 must be present")

	// The gap writes: each updated object must be served under its new value.
	// Missing here means the write in the markStarted→register gap was lost.
	for i := 0; i < numGapUpdates; i++ {
		val := gapValueBase + int64(i)
		assert.Lenf(t, readRangeableIDs(t, rangeBucket, val), 1,
			"gap-updated object %d must survive under value %d — a miss means "+
				"the markStarted→registerDoubleWriteCallbacks gap lost it", i, val)
	}

	// The post-registration writes: served only via the double-write path.
	for i := numGapUpdates; i < numGapUpdates+numPostInitUpdate; i++ {
		val := postValueBase + int64(i)
		assert.Lenf(t, readRangeableIDs(t, rangeBucket, val), 1,
			"post-registration-updated object %d must survive under value %d "+
				"via the double-write path", i, val)
	}

	// Convergence: every object appears exactly once across the expected
	// values — no lost rows (count < numObjects) and no ghosts (a docID under
	// two values). Reads the corpus values plus every injected value.
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
