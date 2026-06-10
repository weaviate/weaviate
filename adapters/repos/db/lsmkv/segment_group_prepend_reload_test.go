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

package lsmkv

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// newColumnarPrependTestBucket creates a columnar int64 bucket with
// keepTombstones=true — the exact configuration the enable-columnar
// migration uses for its ingest buckets (see ShardReindexTaskGeneric
// loadIngestBuckets). No cleanup is registered: the tests below manage
// shutdown/reload explicitly.
func newColumnarPrependTestBucket(t *testing.T, ctx context.Context, dir string) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithKeepTombstones(true),
		WithColumnarSchema(&columnar.Schema{
			Columns: []columnar.Column{{Name: "val", Type: columnar.ColumnTypeInt64}},
		}),
	)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9) // prevent auto-flush
	return b
}

// TestSegmentGroup_PrependSegments_ColumnarSurvivesReload pins the
// DURABLE ordering contract of PrependSegmentsFromBucket: prepended
// (backfill) segments must sort strictly OLDER than the target bucket's
// double-written data not only in the in-process segment-position order
// but also under the filename-sorted order a reload uses.
//
// This is the enable-columnar migration end state: the ingest bucket's
// double-writes sit in its active MEMTABLE (whose future segment file
// name was fixed at memtable creation, BEFORE the backfill segments were
// created), while the backfill arrives via prepend. Without shifting the
// backfill segment names below the memtable's pre-assigned name, the
// post-flush on-disk names invert the logical order: a filename-sorted
// reload (FinalizeCompletedMigrations at next boot, plain restarts)
// treats the stale backfill as newest — stale values shadow
// double-written updates and ColumnarDelete'd docIDs resurrect.
func TestSegmentGroup_PrependSegments_ColumnarSurvivesReload(t *testing.T) {
	ctx := context.Background()

	// Target (= ingest) bucket FIRST: its active memtable's future
	// segment name is assigned now, predating the backfill segments.
	tgtDir := t.TempDir()
	tgt := newColumnarPrependTestBucket(t, ctx, tgtDir)

	// Double-writes land in the ingest memtable: fresh values for docIDs
	// 1 and 2, a delete for docID 3.
	require.NoError(t, tgt.ColumnarPutInt64(1, 0, 111))
	require.NoError(t, tgt.ColumnarPutInt64(2, 0, 222))
	require.NoError(t, tgt.ColumnarDelete(3))

	// Source (= backfill/reindex) bucket SECOND: stale snapshot values
	// for the same docIDs, flushed so the segments are copyable.
	srcDir := t.TempDir()
	src := newColumnarPrependTestBucket(t, ctx, srcDir)
	require.NoError(t, src.ColumnarPutInt64(1, 0, 11))
	require.NoError(t, src.ColumnarPutInt64(2, 0, 22))
	require.NoError(t, src.ColumnarPutInt64(3, 0, 33))
	require.NoError(t, src.FlushAndSwitch())
	require.NoError(t, src.Shutdown(ctx))

	require.NoError(t, tgt.PrependSegmentsFromBucket(ctx, srcDir))

	assertNewestWins := func(t *testing.T, b *Bucket, phase string) {
		t.Helper()
		v, ok, err := b.ColumnarLookupInt64(1, 0)
		require.NoError(t, err, phase)
		require.True(t, ok, "%s: docID 1 must be present", phase)
		assert.Equal(t, int64(111), v, "%s: docID 1 must serve the double-written value", phase)

		v, ok, err = b.ColumnarLookupInt64(2, 0)
		require.NoError(t, err, phase)
		require.True(t, ok, "%s: docID 2 must be present", phase)
		assert.Equal(t, int64(222), v, "%s: docID 2 must serve the double-written value", phase)

		_, ok, err = b.ColumnarLookupInt64(3, 0)
		require.NoError(t, err, phase)
		assert.False(t, ok, "%s: docID 3 was ColumnarDelete'd and must not resurrect", phase)
	}

	// In-process, pre-flush: memtable beats prepended segments.
	assertNewestWins(t, tgt, "in-process pre-flush")

	// Flush the double-write memtable. Its segment file name was fixed
	// at memtable creation time — BEFORE the backfill segments were
	// created — so without a shift the on-disk names now invert the
	// logical order.
	require.NoError(t, tgt.FlushAndSwitch())

	// In-process, post-flush: segment-position order is still correct
	// (prepend inserted at the head, flush appended at the tail).
	assertNewestWins(t, tgt, "in-process post-flush")

	// Shut down and reload from disk: the reload sorts segments by file
	// name. The double-written values must STILL win.
	require.NoError(t, tgt.Shutdown(ctx))

	reloaded := newColumnarPrependTestBucket(t, ctx, tgtDir)
	defer reloaded.Shutdown(ctx)
	assertNewestWins(t, reloaded, "after reload")
}
