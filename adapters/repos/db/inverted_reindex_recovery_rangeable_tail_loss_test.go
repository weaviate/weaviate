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
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// rangeableKey encodes an int64 property value into the uint64 storage key the
// rangeable write/read path uses (LexicographicallySortableInt64 → BigEndian).
// Matches WriteToReindexBucket and shard.addToPropertyRangeBucket.
func rangeableKey(t *testing.T, value int64) uint64 {
	t.Helper()
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	return binary.BigEndian.Uint64(lex)
}

// rangeableHasDocID reports whether docID appears under `value` via an
// OperatorEqual read — the production range-query read shape.
func rangeableHasDocID(t *testing.T, b *lsmkv.Bucket, value int64, docID uint64) bool {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(context.Background(), rangeableKey(t, value), filters.OperatorEqual)
	require.NoError(t, err)
	defer func() {
		if release != nil {
			release()
		}
	}()
	if bm == nil {
		return false
	}
	for _, id := range bm.ToArray() {
		if id == docID {
			return true
		}
	}
	return false
}

// tailPosting is one post-flip write applied directly to the live (ex-ingest)
// rangeable bucket, exactly as shard.addToPropertyRangeBucket would.
type tailPosting struct {
	value int64
	docID uint64
}

// TestRangeableRecovery_PostFlipWriteTail_SurvivesGracefulRestart is the
// shard-level regression guard for the "face 3" restart-crossing posting-loss
// window of weaviate/0-weaviate-issues#335, exercising the REAL migration +
// finalizeMigrationDir rename + reload (not a hand-rolled rename).
//
// A replica that completed the enable-rangeable migration in-process holds the
// promoted (ex-ingest) rangeable bucket live at its still-deferred ingest dir.
// Post-flip writes land in that bucket's memtable/WAL. Across a GRACEFUL
// restart (on-shutdown flush runs), the next-boot finalize renames the ingest
// dir to canonical and reloads. This asserts every post-flip posting survives
// that round trip.
//
// It is GREEN: the single-node path preserves the tail, which refutes
// sub-mechanisms (a)/(b) as deterministic single-node bugs. The field's 6/1500
// loss (weaviate/weaviate#11985, node 2) therefore points upstream of the
// storage mechanics (multi-node / concurrency), not to swap/finalize/reload.
func TestRangeableRecovery_PostFlipWriteTail_SurvivesGracefulRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangeTailLoss_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName
	canonical := helpers.BucketRangeableFromPropNameLSM(propName)

	class := newFilterableToRangeableTestClass(className)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	// Backfill source data: 25 objects with values cycling 0..4.
	for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Drive the inline migration to completion (swap done; the promoted
	// rangeable bucket is live at its deferred ingest dir).
	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	rb := shard.store.Bucket(canonical)
	require.NotNil(t, rb, "promoted rangeable bucket must be live post-migration")
	require.True(t, rb.HasAnyData(), "backfill must have populated the promoted bucket")

	// Post-flip writes: a small tail of new postings, applied to the live
	// bucket the same way the write path does. docIDs are disjoint from the
	// backfill (0..24) so presence is unambiguous. The tail is deliberately
	// small so it stays in the reused WAL on shutdown (< MaxReuseWalSize) —
	// the field shape.
	tail := []tailPosting{
		{value: 0, docID: 1000},
		{value: 1, docID: 1001},
		{value: 2, docID: 1002},
		{value: 3, docID: 1003},
		{value: 4, docID: 1004},
		{value: 7, docID: 1005},
	}
	for _, p := range tail {
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, p.value), p.docID))
	}

	// Pre-restart: every tail posting is served (the "1500-pass" state).
	for _, p := range tail {
		require.Truef(t, rangeableHasDocID(t, rb, p.value, p.docID),
			"pre-restart: tail posting value=%d docID=%d must be served", p.value, p.docID)
	}

	// Receipt that this test exercises the deferred-rename finalize path: the
	// live bucket is still on disk at the ingest sidecar dir, not the canonical.
	lsmPath := shard.pathLSM()
	ingestDir := findRangeableIngestDir(t, lsmPath)
	canonicalDir := filepath.Join(lsmPath, canonical)

	// Graceful shutdown: RestartAt runs the on-shutdown flush (not SIGKILL).
	require.NoError(t, shard.Shutdown(ctx))

	// Restart: the next-boot finalize renames the ingest dir to canonical and
	// reloads the promoted bucket.
	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shard.Name(), className)
	defer shard2.Shutdown(ctx)

	require.NoDirExists(t, ingestDir, "finalize must have renamed the ingest dir away")
	require.DirExists(t, canonicalDir, "finalize must have promoted to the canonical dir")

	rb2 := shard2.store.Bucket(canonical)
	require.NotNil(t, rb2, "promoted rangeable bucket must reload after restart")

	var missing []tailPosting
	for _, p := range tail {
		if !rangeableHasDocID(t, rb2, p.value, p.docID) {
			missing = append(missing, p)
		}
	}
	require.Emptyf(t, missing,
		"post-restart: %d/%d post-flip rangeable postings vanished across the "+
			"graceful restart (finalize rename + reload); missing=%v",
		len(missing), len(tail), missing)
}

// TestRangeableRecovery_PostFlipPatchTail_SurvivesGracefulRestart is the
// UPDATE-shaped variant of the face-3 pin. The field workload
// (weaviate/weaviate#11985 ConcurrentUpdatesNoLossNoPanic) is PATCHes of
// existing objects, each of which does Remove(oldValue) + Add(newValue) on the
// rangeable bucket. A doc moved value V→W (flushed to a segment) then W→V (left
// in the reused WAL tail) must, post-restart, still read under V: the WAL
// tail's Add(V) has to win over the earlier segment's Remove(V). If the
// finalize rename + WAL-recovery reload mis-orders the tail relative to the
// flushed removal segment, the doc silently vanishes from value V.
func TestRangeableRecovery_PostFlipPatchTail_SurvivesGracefulRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangePatchLoss_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName
	canonical := helpers.BucketRangeableFromPropNameLSM(propName)

	class := newFilterableToRangeableTestClass(className)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	rb := shard.store.Bucket(canonical)
	require.NotNil(t, rb)
	require.True(t, rb.HasAnyData())

	// Post-flip movers: docIDs 1000..1199, final value V=3 ("sentinel band").
	// Each is moved twice: V→W is flushed to a segment (the Remove(V) lands
	// there), then W→V lands in the tail WAL (the Add(V) that must survive).
	const (
		numMovers  = 200
		firstDocID = 1000
		valueV     = int64(3)
		valueW     = int64(9)
	)
	// Move V→W for all movers, then flush so the Remove(V)/Add(W) is on disk.
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueV), docID)) // initial land under V
		require.NoError(t, rb.RoaringSetRangeRemove(rangeableKey(t, valueV), docID))
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueW), docID))
	}
	require.NoError(t, rb.FlushAndSwitch())

	// Move W→V for all movers — this is the authoritative-pass tail; it stays
	// in the fresh active memtable / reused WAL on shutdown.
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		require.NoError(t, rb.RoaringSetRangeRemove(rangeableKey(t, valueW), docID))
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueV), docID))
	}

	// Pre-restart: every mover is served under V (the 1500-pass state).
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		require.Truef(t, rangeableHasDocID(t, rb, valueV, docID),
			"pre-restart: mover docID=%d must be served under sentinel value %d", docID, valueV)
	}

	require.NoError(t, shard.Shutdown(ctx))

	shard2 := restartWithCompletedRangeableSchema(t, ctx, idx, shard.Name(), className)
	defer shard2.Shutdown(ctx)

	rb2 := shard2.store.Bucket(canonical)
	require.NotNil(t, rb2)

	var missing []uint64
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		if !rangeableHasDocID(t, rb2, valueV, docID) {
			missing = append(missing, docID)
		}
	}
	require.Emptyf(t, missing,
		"post-restart: %d/%d movers vanished from sentinel value %d across the "+
			"graceful restart; missing docIDs=%v",
		len(missing), numMovers, valueV, missing)
}
