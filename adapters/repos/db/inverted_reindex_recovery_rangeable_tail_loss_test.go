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
)

// rangeableKey encodes value into the rangeable bucket's uint64 storage key
// (matches WriteToReindexBucket and shard.addToPropertyRangeBucket).
func rangeableKey(t *testing.T, value int64) uint64 {
	t.Helper()
	lex, err := entinverted.LexicographicallySortableInt64(value)
	require.NoError(t, err)
	return binary.BigEndian.Uint64(lex)
}

// rangeableHasDocID reports whether docID appears under value via an
// OperatorEqual read.
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

// tailPosting is one post-flip write applied directly to the live rangeable bucket.
type tailPosting struct {
	value int64
	docID uint64
}

// TestRangeableRecovery_PostFlipWriteTail_SurvivesGracefulRestart pins
// weaviate/0-weaviate-issues#335: post-flip rangeable writes must survive a
// graceful restart across the deferred-rename finalize.
func TestRangeableRecovery_PostFlipWriteTail_SurvivesGracefulRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangeTailLoss_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName
	canonical := helpers.BucketRangeableFromPropNameLSM(propName)

	// Drive the migration to completion; the promoted bucket is live at its
	// deferred ingest dir.
	shard, idx := setupRangeableMigratedShard(t, ctx, className)

	rb := shard.store.Bucket(canonical)
	require.NotNil(t, rb, "promoted rangeable bucket must be live post-migration")
	require.True(t, rb.HasAnyData(), "backfill must have populated the promoted bucket")

	// Post-flip writes: a small tail kept under MaxReuseWalSize so it stays
	// in the reused WAL on shutdown, not flushed to a segment.
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

	// Pre-restart: every tail posting is served.
	for _, p := range tail {
		require.Truef(t, rangeableHasDocID(t, rb, p.value, p.docID),
			"pre-restart: tail posting value=%d docID=%d must be served", p.value, p.docID)
	}

	// Confirm the live bucket is still at the ingest dir (deferred-rename path).
	lsmPath := shard.pathLSM()
	ingestDir := findRangeableIngestDir(t, lsmPath)
	canonicalDir := filepath.Join(lsmPath, canonical)

	// Graceful shutdown: RestartAt runs the on-shutdown flush (not SIGKILL).
	require.NoError(t, shard.Shutdown(ctx))

	// Restart: next-boot finalize renames the ingest dir to canonical and reloads.
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

// TestRangeableRecovery_PostFlipPatchTail_SurvivesGracefulRestart pins
// weaviate/0-weaviate-issues#335: a doc moved V→W (flushed to a segment) then
// W→V (left in the reused WAL tail) must still read under V post-restart —
// the WAL tail's Add(V) has to win over the earlier segment's Remove(V).
func TestRangeableRecovery_PostFlipPatchTail_SurvivesGracefulRestart(t *testing.T) {
	ctx := testCtx()
	className := "RangePatchLoss_" + uuid.NewString()[:8]
	propName := filterableToRangeablePropName
	canonical := helpers.BucketRangeableFromPropNameLSM(propName)

	shard, idx := setupRangeableMigratedShard(t, ctx, className)

	rb := shard.store.Bucket(canonical)
	require.NotNil(t, rb)
	require.True(t, rb.HasAnyData())

	// Post-flip movers: each moves V→W (flushed to a segment) then W→V (left
	// in the tail WAL) — the WAL Add(V) must survive.
	const (
		numMovers  = 200
		firstDocID = 1000
		valueV     = int64(3)
		valueW     = int64(9)
	)
	// Move V→W for all movers, then flush so the Remove(V)/Add(W) is on disk.
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueV), docID))
		require.NoError(t, rb.RoaringSetRangeRemove(rangeableKey(t, valueV), docID))
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueW), docID))
	}
	require.NoError(t, rb.FlushAndSwitch())

	// Move W→V for all movers — the tail that stays in the reused WAL on
	// shutdown.
	for k := 0; k < numMovers; k++ {
		docID := uint64(firstDocID + k)
		require.NoError(t, rb.RoaringSetRangeRemove(rangeableKey(t, valueW), docID))
		require.NoError(t, rb.RoaringSetRangeAdd(rangeableKey(t, valueV), docID))
	}

	// Pre-restart: every mover is served under V.
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
