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
	"time"

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

// readRangeableDocIDs returns the docIDs indexed under a single int64 value in
// a RoaringSetRange (rangeable) bucket. Same read path as
// [filterableToRangeableFingerprint], narrowed to one value so a test can
// assert presence/absence of one specific write.
func readRangeableDocIDs(t *testing.T, b *lsmkv.Bucket, value int64) []uint64 {
	t.Helper()
	require.NotNil(t, b, "rangeable bucket must exist")
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	require.NoError(t, err)
	var ids []uint64
	if bm != nil {
		ids = bm.ToArray()
	}
	if release != nil {
		release()
	}
	return ids
}

// TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap is the regression pin
// for weaviate/weaviate#11692 — the multi-node residual that the single-clock
// fix (weaviate/weaviate#11688, capture reindexStarted after registration,
// ms-ceiled) did NOT close. It went red against the pre-fix code (the backfill
// skipped any object whose LastUpdateTimeUnix was at/after the replica-local
// reindexStarted watermark) and is green since [uuidObjectsIteratorAsync]
// analyzes every scanned object unconditionally.
//
// # The bug this pins
//
// The pre-fix backfill iterator classified every object it scanned by one
// comparison:
//
//	obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli()  → backfill (analyze)
//	obj.LastUpdateTimeUnix() >= reindexStarted.UnixMilli() → skip (assume mirrored)
//
// The "skip" branch was only sound if every object with a timestamp at/after
// reindexStarted was in fact captured by the double-write callbacks. That holds
// under a SINGLE clock: reindexStarted is captured after callback registration,
// so any write timestamped >= reindexStarted physically happened after the
// callbacks were live and was mirrored.
//
// It does NOT hold across nodes. LastUpdateTimeUnix is stamped on the
// coordinator that received the write (usecases/objects/*.go: `LastUpdateTimeUnix
// = m.timeSource.Now()`), and the replica preserves that stamp verbatim
// (no re-stamp on the shard write path). reindexStarted is stamped on the
// replica's own clock (markStarted(time.Now())). With the coordinator's clock
// ahead of the replica's, a write could:
//
//   - arrive at the replica BEFORE callback registration (so it is NOT
//     mirrored), yet
//   - carry a coordinator timestamp >= reindexStarted (so the backfill SKIPPED
//     it, assuming it was mirrored).
//
// Skipped AND unmirrored ⇒ the row was permanently missing from the migrated
// index after the migration reported FINISHED.
//
// # How this test models the skew, and what it does NOT show
//
// A replica preserves whatever LastUpdateTimeUnix the coordinator stamped, so a
// coordinator whose clock is ahead is modeled exactly by putting an object with
// a future LastUpdateTimeUnix on a single shard. The skip decision was a pure
// local timestamp comparison, so this stand-in is faithful for the
// classification leg. It deliberately does NOT demonstrate the reachability
// leg — a replicated write racing callback registration under real replication
// timing; that ordering claim rests on the code reading of the
// markStarted-after-registration sequence (weaviate/weaviate#11688), not on
// this test.
//
// Probes (all written BEFORE the migration starts, i.e. before callback
// registration — so none is mirrored; the double-write path cannot save any of
// them; the ONLY variable is the coordinator timestamp):
//
//   - control (controlValue): honest past timestamp → must be backfilled.
//     Same journey as the skewed insert minus the skew — proves the harness
//     covers pre-registration writes when the timestamp is honest.
//   - skewed insert (skewValue): timestamp a full DAY ahead (so the pin stays
//     red through #11688's ms-ceil fix — this is the residual, not the
//     single-clock bug) → pre-fix skipped-and-unmirrored → lost.
//   - skewed update (updateOldValue → updateNewValue): the delete-side
//     companion. Pre-fix, the add leg lost updateNewValue the same way; the
//     migrated bucket must ALSO not resurrect updateOldValue.
//   - skewed delete (deletedValue): an object deleted pre-registration (with a
//     future deletion time) must not leave its posting in the migrated bucket.
func TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap(t *testing.T) {
	const (
		numCorpus      = 25         // baseline population, all timestamp 0 → all backfilled
		controlValue   = int64(100) // distinct from corpus values 0..4 and from the sentinels below
		skewValue      = int64(999) // skewed-insert sentinel; its loss is unambiguous
		updateOldValue = int64(555) // overwritten pre-registration; must NOT resurrect
		updateNewValue = int64(556) // the update's current value; must survive
		deletedValue   = int64(777) // deleted pre-registration; must NOT appear
	)
	propName := filterableToRangeablePropName

	ctx := testCtx()
	className := "MultiNodeClockSkewGap_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	// Positive-control corpus: all timestamp 0, all strictly below any
	// reindexStarted, so the backfill indexes every one. If the bucket ends up
	// empty the skew assertion would be vacuous, so we assert one corpus value
	// is present below.
	for _, obj := range makeFilterableToRangeableTestObjects(t, numCorpus, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	putWithTimestamp := func(id string, value, tsMillis int64) {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(id),
				Class:              className,
				Properties:         map[string]interface{}{propName: value},
				CreationTimeUnix:   tsMillis,
				LastUpdateTimeUnix: tsMillis,
			},
		}))
	}

	// All probes land BEFORE the migration → before callback registration →
	// none is mirrored. The only variable is the coordinator timestamp.
	//
	//   control: an hour in the PAST — the honest-timestamp baseline.
	//   skewed:  a DAY in the FUTURE, modeling a coordinator clock far ahead
	//            of this replica; pre-fix this landed at/above the local
	//            watermark and was skipped by the backfill.
	controlTs := time.Now().Add(-time.Hour).UnixMilli()
	skewTs := time.Now().Add(24 * time.Hour).UnixMilli()
	putWithTimestamp(uuid.NewString(), controlValue, controlTs)
	putWithTimestamp(uuid.NewString(), skewValue, skewTs)

	// Skewed UPDATE: created with an honest timestamp, then overwritten
	// pre-registration by a coordinator-ahead write. The migrated bucket must
	// carry the NEW value and must not resurrect the OLD one.
	updateID := uuid.NewString()
	putWithTimestamp(updateID, updateOldValue, controlTs)
	putWithTimestamp(updateID, updateNewValue, skewTs)

	// Skewed DELETE: created with an honest timestamp, then deleted
	// pre-registration with a coordinator-ahead deletion time. The migrated
	// bucket must not contain its posting.
	deleteID := uuid.NewString()
	putWithTimestamp(deleteID, deletedValue, controlTs)
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(deleteID), time.Now().Add(24*time.Hour)))

	// Drive the production migration path to completion.
	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
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

	// Positive control 1: a corpus value must be backfilled, else the migration
	// populated nothing and the skew assertion below would prove nothing.
	require.NotEmpty(t, readRangeableDocIDs(t, rangeBucket, 0),
		"positive control: backfilled corpus value 0 must be present")

	// Positive control 2: the un-skewed pre-registration write (past timestamp)
	// is picked up by the backfill. This is the SAME situation as the skewed
	// write minus the clock skew — it survives, proving the harness indexes
	// pre-registration writes when the timestamp is honest.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, controlValue), 1,
		"control: an unmirrored pre-registration write with an honest (past) "+
			"timestamp must be backfilled under value %d", controlValue)

	// THE GAP. Same unmirrored pre-registration write, only the timestamp is
	// coordinator-ahead. Pre-fix the backfill skipped it as "already mirrored"
	// while the mirror never saw it. The skew-immune scan must index it.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, skewValue), 1,
		"GAP (weaviate/weaviate#11692): an unmirrored pre-registration write whose "+
			"coordinator timestamp (%d) is ahead of the replica's reindexStarted is "+
			"skipped by the backfill AND missed by the double-write callbacks — "+
			"permanently lost from the migrated index. value %d absent",
		skewTs, skewValue)

	// Delete-side companions (weaviate/weaviate#11692 has a resurrection
	// failure mode, not just a loss one): the skewed update's CURRENT value
	// must be indexed, its OVERWRITTEN value must not resurrect, and the
	// deleted object must not appear at all.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, updateNewValue), 1,
		"GAP (weaviate/weaviate#11692, update add leg): the current value %d of an "+
			"object overwritten pre-registration by a coordinator-ahead write is "+
			"missing from the migrated index", updateNewValue)
	assert.Emptyf(t, readRangeableDocIDs(t, rangeBucket, updateOldValue),
		"resurrection (weaviate/weaviate#11692, update delete leg): the OVERWRITTEN "+
			"value %d of a pre-registration skewed update is still queryable in the "+
			"migrated index — a filter on the stale value false-positives", updateOldValue)
	assert.Emptyf(t, readRangeableDocIDs(t, rangeBucket, deletedValue),
		"resurrection (weaviate/weaviate#11692, delete): an object deleted "+
			"pre-registration (deletion time coordinator-ahead) still has its posting "+
			"%d in the migrated index", deletedValue)
}
