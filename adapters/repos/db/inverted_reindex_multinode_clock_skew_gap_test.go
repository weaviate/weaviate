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

// TestReindex_MultiNodeClockSkew_ReopensDoubleWriteGap pins the multi-node
// residual left open by the single-clock fix in weaviate/weaviate#11688:
// LastUpdateTimeUnix is coordinator-clock, reindexStarted is replica-clock,
// so a coordinator-ahead write can land pre-registration (unmirrored) yet
// carry a timestamp >= reindexStarted (skipped by the old cutoff-based
// backfill) — permanently lost (weaviate/weaviate#11692). Covers the insert,
// update, and delete legs.
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

	// Positive-control corpus, so the skew assertion below isn't vacuous.
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

	// All probes are written before the migration starts, so none is
	// mirrored; only the coordinator timestamp varies (control: honest
	// past; skewed: a day ahead, modeling a coordinator-ahead clock).
	controlTs := time.Now().Add(-time.Hour).UnixMilli()
	skewTs := time.Now().Add(24 * time.Hour).UnixMilli()
	putWithTimestamp(uuid.NewString(), controlValue, controlTs)
	putWithTimestamp(uuid.NewString(), skewValue, skewTs)

	// Skewed update: honest timestamp, then overwritten pre-registration by
	// a coordinator-ahead write.
	updateID := uuid.NewString()
	putWithTimestamp(updateID, updateOldValue, controlTs)
	putWithTimestamp(updateID, updateNewValue, skewTs)

	// Skewed delete: honest timestamp, then deleted pre-registration with a
	// coordinator-ahead deletion time.
	deleteID := uuid.NewString()
	putWithTimestamp(deleteID, deletedValue, controlTs)
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(deleteID), time.Now().Add(24*time.Hour)))

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

	// Positive control: else the skew assertion below would be vacuous.
	require.NotEmpty(t, readRangeableDocIDs(t, rangeBucket, 0),
		"positive control: backfilled corpus value 0 must be present")

	// Control: same situation as the skewed write, minus the skew.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, controlValue), 1,
		"control: an unmirrored pre-registration write with an honest (past) "+
			"timestamp must be backfilled under value %d", controlValue)

	// The gap: same write, coordinator-ahead timestamp instead.
	assert.Lenf(t, readRangeableDocIDs(t, rangeBucket, skewValue), 1,
		"GAP (weaviate/weaviate#11692): an unmirrored pre-registration write whose "+
			"coordinator timestamp (%d) is ahead of the replica's reindexStarted is "+
			"skipped by the backfill AND missed by the double-write callbacks — "+
			"permanently lost from the migrated index. value %d absent",
		skewTs, skewValue)

	// Resurrection legs: the bug also has a resurrect-stale-value mode, not
	// just a loss mode.
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
