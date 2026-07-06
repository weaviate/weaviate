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

// TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap is the reproduction pin
// for weaviate/weaviate#11692 — the multi-node residual that the single-clock
// fix (weaviate/weaviate#11688, capture reindexStarted after registration,
// ms-ceiled) does NOT close.
//
// RED PIN. This test asserts the CORRECT (skew-immune) behavior and therefore
// FAILS against current main. It is committed red on purpose: it is the
// reproduction case for the architectural gap, not a regression against a fix
// that already exists. See the PR body (issue #11692) for the design options.
//
// # The invariant and why skew breaks it
//
// The backfill iterator ([uuidObjectsIteratorAsync]) classifies every object
// it scans by one comparison:
//
//	obj.LastUpdateTimeUnix() < reindexStarted.UnixMilli()  → backfill (analyze)
//	obj.LastUpdateTimeUnix() >= reindexStarted.UnixMilli() → skip (assume mirrored)
//
// The "skip" branch is only sound if every object with a timestamp at/after
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
// ahead of the replica's, a write can:
//
//   - arrive at the replica BEFORE callback registration (so it is NOT
//     mirrored), yet
//   - carry a coordinator timestamp >= reindexStarted (so the backfill SKIPS
//     it, assuming it was mirrored).
//
// Skipped AND unmirrored ⇒ the row is permanently missing from the migrated
// index after the migration reports FINISHED.
//
// # How this test models the skew
//
// A replica preserves whatever LastUpdateTimeUnix the coordinator stamped, so a
// coordinator whose clock is ahead is modeled exactly by putting an object with
// a future LastUpdateTimeUnix. Both the skewed object and a control object are
// written BEFORE the migration starts, i.e. before callback registration — so
// neither is mirrored; the double-write path cannot save either. The ONLY
// difference between them is the timestamp:
//
//   - control (value controlValue): a normal past timestamp → below cutoff →
//     backfilled → survives.
//   - skewed  (value skewValue):    a future coordinator timestamp → at/above
//     cutoff → skipped → LOST.
//
// control present + skewed absent isolates cross-node skew as the sole cause.
// (On current main the object would also be lost via the pre-#11688 ordering
// bug; the skew here is a full day so the gap survives #11688's fix too — the
// residual this pin is about.)
func TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap(t *testing.T) {
	t.Skip("RFC weaviate/weaviate#11692: reproduces the multi-node clock-skew double-write gap (a coordinator-stamped write with timestamp >= the replica's reindexStarted is skipped by backfill AND unmirrored). Un-skip when the skew-immune cutoff fix lands.")

	const (
		numCorpus    = 25         // baseline population, all timestamp 0 → all backfilled
		controlValue = int64(100) // distinct from corpus values 0..4 and from skewValue
		skewValue    = int64(999) // distinct sentinel; its loss is unambiguous
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

	putWithTimestamp := func(value, tsMillis int64) {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 strfmt.UUID(uuid.NewString()),
				Class:              className,
				Properties:         map[string]interface{}{propName: value},
				CreationTimeUnix:   tsMillis,
				LastUpdateTimeUnix: tsMillis,
			},
		}))
	}

	// Both written BEFORE the migration → before callback registration → neither
	// is mirrored. The only variable is the timestamp.
	//
	//   control: an hour in the PAST → below cutoff → must be backfilled.
	//   skewed:  a DAY in the FUTURE, modeling a coordinator clock far ahead of
	//            this replica → at/above cutoff → skipped by the backfill.
	controlTs := time.Now().Add(-time.Hour).UnixMilli()
	skewTs := time.Now().Add(24 * time.Hour).UnixMilli()
	putWithTimestamp(controlValue, controlTs)
	putWithTimestamp(skewValue, skewTs)

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

	// THE GAP (RED). Same unmirrored pre-registration write, only the timestamp
	// is coordinator-ahead. Under skew the backfill skips it as "already
	// mirrored" while the mirror never saw it. A skew-immune mechanism must
	// still index it.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, skewValue), 1,
		"GAP (weaviate/weaviate#11692): an unmirrored pre-registration write whose "+
			"coordinator timestamp (%d) is ahead of the replica's reindexStarted is "+
			"skipped by the backfill AND missed by the double-write callbacks — "+
			"permanently lost from the migrated index. value %d absent",
		skewTs, skewValue)
}
