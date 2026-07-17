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

//go:build integrationTest

package lsmkv

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// dumpDir lists the files (with sizes) in a bucket dir for post-mortem
// forensics — which of the tail's writes lives in a .db segment vs a .wal.
func dumpDir(t *testing.T, label, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Logf("[%s] dir %s: read error %v", label, dir, err)
		return
	}
	t.Logf("[%s] dir %s contents:", label, dir)
	for _, e := range entries {
		info, _ := e.Info()
		var sz int64
		if info != nil {
			sz = info.Size()
		}
		t.Logf("    %-40s %d bytes", e.Name(), sz)
	}
}

// rangeableSwapBucketOpts mirrors the field/default bucket options for a
// rangeable bucket (INDEX_RANGEABLE_IN_MEMORY defaults off, so the on-disk
// segment read path is used — the config the weaviate/weaviate#11985 CI ran).
func rangeableSwapBucketOpts() []BucketOption {
	return []BucketOption{
		WithStrategy(StrategyRoaringSetRange),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithUseBloomFilter(false),
		// production default (DefaultPersistenceMaxReuseWalSize = 4096): a
		// small post-flip tail is persisted as a reused WAL on shutdown, not
		// flushed to a segment.
		WithMinWalThreshold(4096),
	}
}

// readRangeableEqual returns the sorted docIDs stored under key `val` with
// OperatorEqual — the same read shape the production range query path and the
// db-package fingerprint helper use.
func readRangeableEqual(t *testing.T, b *Bucket, val uint64) []uint64 {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	bm, release, err := reader.Read(context.Background(), val, filters.OperatorEqual)
	require.NoError(t, err)
	var ids []uint64
	if bm != nil {
		ids = bm.ToArray()
	}
	if release != nil {
		release()
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestRangeableSwap_PostFlipWriteTail_SurvivesGracefulRestart is the
// storage-level regression guard for the "face 3" restart-crossing
// posting-loss window of weaviate/0-weaviate-issues#335. It asserts NO loss:
// the isolated write→flush→rename→reload path preserves the post-flip tail
// across a graceful restart, which REFUTES sub-mechanisms (a) "shutdown flush
// doesn't run for swapped buckets" and (b) "segment mis-order/drop on reload"
// as deterministic single-node bugs. If a future change to SwapBucketPointer,
// finalizeMigrationDir, PrependSegmentsFromBucket, or WAL recovery reintroduces
// the loss, this goes red.
//
// Sequence (mirrors the in-process enable-rangeable migration + deferred
// finalize rename):
//  1. Build a populated rangeable *ingest* bucket the way the migration does:
//     backfill segments prepended from a reindex bucket (PrependSegmentsFromBucket)
//     plus an unflushed double-write memtable.
//  2. SwapBucketPointer(main <- ingest): the live bucket is now the ex-ingest
//     bucket, still on disk at the ingest dir.
//  3. Post-flip writes land in the ex-ingest bucket's active memtable/WAL.
//  4. Graceful store shutdown (on-shutdown flush runs — matches RestartAt, not
//     SIGKILL).
//  5. The next-restart finalize renames the ingest dir to the canonical main
//     dir (finalizeMigrationDir: os.Rename after removing any stale main dir).
//  6. Reload the bucket from the canonical dir and assert every posting —
//     backfill, double-write, AND the post-flip tail — is present.
func TestRangeableSwap_PostFlipWriteTail_SurvivesGracefulRestart(t *testing.T) {
	t.Run("onDisk", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		logger, _ := test.NewNullLogger()

		const (
			mainName    = "property_score_rangeable"
			ingestName  = "property_score_rangeable__rangeable_ingest_1"
			reindexName = "property_score_rangeable__rangeable_reindex_1"
		)
		mainDir := filepath.Join(dir, mainName)
		ingestDir := filepath.Join(dir, ingestName)
		reindexDir := filepath.Join(dir, reindexName)
		backupDir := filepath.Join(dir, "property_score_rangeable__rangeable_backup_1")

		opts := rangeableSwapBucketOpts()

		// ---- Phase 1: running process, in-process migration ----
		store, err := New(dir, dir, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)

		// Empty canonical bucket, as PreReindexHook creates it.
		require.NoError(t, store.CreateOrLoadBucket(ctx, mainName, opts...))

		// Backfill lives in a reindex bucket, flushed to real segments.
		require.NoError(t, store.CreateOrLoadBucket(ctx, reindexName, opts...))
		reindexBucket := store.Bucket(reindexName)
		require.NotNil(t, reindexBucket)
		// Historical postings: docIDs 0..999, value == docID.
		const backfillN = 1000
		for i := uint64(0); i < backfillN; i++ {
			require.NoError(t, reindexBucket.RoaringSetRangeAdd(i, i))
		}
		require.NoError(t, reindexBucket.FlushAndSwitch())

		// Ingest bucket: prepend backfill segments + double-write window,
		// then flush so the bulk lives in segments and the active memtable is
		// clean — exactly the steady state after the flush cycle has drained
		// the migration's bulk writes. The post-flip tail then lands in a
		// fresh, small memtable.
		require.NoError(t, store.CreateOrLoadBucket(ctx, ingestName, opts...))
		ingestBucket := store.Bucket(ingestName)
		require.NotNil(t, ingestBucket)
		require.NoError(t, ingestBucket.PrependSegmentsFromBucket(ctx, reindexDir))
		// Double-write window postings: docIDs 1000..1099.
		const doubleWriteN = 100
		for i := uint64(backfillN); i < backfillN+doubleWriteN; i++ {
			require.NoError(t, ingestBucket.RoaringSetRangeAdd(i, i))
		}
		require.NoError(t, ingestBucket.FlushAndSwitch())

		// Reindex bucket is shut down + dir removed by prep.
		require.NoError(t, store.ShutdownBucket(ctx, reindexName))
		require.NoError(t, os.RemoveAll(reindexDir))

		// ---- Phase 2a: SwapBucketPointer (in-memory pointer flip) ----
		oldMain, err := store.SwapBucketPointer(ctx, mainName, ingestName)
		require.NoError(t, err)
		// Phase 2b: shut down displaced old-main, rename its dir to backup.
		require.NoError(t, oldMain.Shutdown(ctx))
		require.NoError(t, os.Rename(mainDir, backupDir))

		// ---- Phase 3: post-flip writes to the ex-ingest bucket ----
		// The authoritative re-PATCH tail: a handful of postings that stay
		// in the reused WAL on shutdown (commitlog < MaxReuseWalSize), which
		// is the field shape (node 2: 6/1500 lost across a graceful restart).
		const tailStart = backfillN + doubleWriteN
		const tailN = 6
		live := store.Bucket(mainName)
		require.NotNil(t, live)
		require.Equal(t, ingestDir, live.GetDir(),
			"live main bucket must still be on disk at the ingest dir")
		for i := uint64(tailStart); i < tailStart+tailN; i++ {
			require.NoError(t, live.RoaringSetRangeAdd(i, i))
		}

		// Sanity: everything is served pre-restart (the 1500-pass state).
		for i := uint64(0); i < tailStart+tailN; i++ {
			require.Equal(t, []uint64{i}, readRangeableEqual(t, live, i),
				"pre-restart: posting for value %d must be served", i)
		}

		// ---- Phase 4: graceful shutdown (RestartAt: flush runs) ----
		require.NoError(t, store.Shutdown(ctx))

		dumpDir(t, "post-shutdown ingest dir", ingestDir)

		// ---- Phase 5: next-restart finalize rename (finalizeMigrationDir) ----
		require.NoError(t, os.RemoveAll(backupDir))
		if _, statErr := os.Stat(mainDir); statErr == nil {
			require.NoError(t, os.RemoveAll(mainDir))
		}
		require.NoError(t, os.Rename(ingestDir, mainDir))

		dumpDir(t, "post-rename main dir", mainDir)

		// ---- Phase 6: reload from canonical dir, assert no loss ----
		store2, err := New(dir, dir, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		defer store2.Shutdown(ctx)

		require.NoError(t, store2.CreateOrLoadBucket(ctx, mainName, opts...))
		reloaded := store2.Bucket(mainName)
		require.NotNil(t, reloaded)

		var missing []uint64
		for i := uint64(0); i < tailStart+tailN; i++ {
			if got := readRangeableEqual(t, reloaded, i); len(got) != 1 || got[0] != i {
				missing = append(missing, i)
			}
		}
		require.Emptyf(t, missing,
			"post-restart: %d/%d postings vanished across the graceful restart; "+
				"missing docIDs=%v (tail band is [%d,%d))",
			len(missing), tailStart+tailN, missing, tailStart, tailStart+tailN)
	})
}

// TestRangeableSwap_PostFlipWriteTail_RealFlushCycle drives the same face-3
// swap→post-flip-write→graceful-shutdown→rename→reload round trip, but with the
// PRODUCTION flush cycle running (a real cyclemanager on a fast ticker plus the
// default MaxReuseWalSize) so the registered flush callback switches the WAL
// mid-stream and the graceful shutdown races an in-flight flush — the field
// condition my explicit-FlushAndSwitch tests don't exercise. It repeats the
// write/switch churn to widen the shutdown-vs-flush window.
func TestRangeableSwap_PostFlipWriteTail_RealFlushCycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	const (
		mainName    = "property_score_rangeable"
		ingestName  = "property_score_rangeable__rangeable_ingest_1"
		reindexName = "property_score_rangeable__rangeable_reindex_1"
	)
	mainDir := filepath.Join(dir, mainName)
	ingestDir := filepath.Join(dir, ingestName)
	reindexDir := filepath.Join(dir, reindexName)
	backupDir := filepath.Join(dir, "property_score_rangeable__rangeable_backup_1")

	// Real flush cycle on a fast ticker + a low WAL threshold, so the callback
	// fires and switches the commitlog repeatedly during the writes below.
	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager("flush",
		cyclemanager.NewFixedTicker(2*time.Millisecond), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()

	opts := []BucketOption{
		WithStrategy(StrategyRoaringSetRange),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithUseBloomFilter(false),
		WithWalThreshold(4096),
		WithMinWalThreshold(4096),
	}

	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		flushCallbacks)
	require.NoError(t, err)

	require.NoError(t, store.CreateOrLoadBucket(ctx, mainName, opts...))
	require.NoError(t, store.CreateOrLoadBucket(ctx, reindexName, opts...))
	reindexBucket := store.Bucket(reindexName)
	const backfillN = 500
	for i := uint64(0); i < backfillN; i++ {
		require.NoError(t, reindexBucket.RoaringSetRangeAdd(i, i))
	}
	require.NoError(t, reindexBucket.FlushAndSwitch())

	require.NoError(t, store.CreateOrLoadBucket(ctx, ingestName, opts...))
	ingestBucket := store.Bucket(ingestName)
	require.NoError(t, ingestBucket.PrependSegmentsFromBucket(ctx, reindexDir))
	require.NoError(t, store.ShutdownBucket(ctx, reindexName))
	require.NoError(t, os.RemoveAll(reindexDir))

	oldMain, err := store.SwapBucketPointer(ctx, mainName, ingestName)
	require.NoError(t, err)
	require.NoError(t, oldMain.Shutdown(ctx))
	require.NoError(t, os.Rename(mainDir, backupDir))

	// Post-flip writes with the flush cycle churning underneath. Pace the
	// writes so the 2ms ticker interleaves flush/switch between them.
	const tailStart = backfillN
	const tailN = 500
	live := store.Bucket(mainName)
	require.Equal(t, ingestDir, live.GetDir())
	for i := uint64(tailStart); i < tailStart+tailN; i++ {
		require.NoError(t, live.RoaringSetRangeAdd(i, i))
		if i%25 == 0 {
			time.Sleep(3 * time.Millisecond)
		}
	}

	for i := uint64(0); i < tailStart+tailN; i++ {
		require.Equalf(t, []uint64{i}, readRangeableEqual(t, live, i),
			"pre-restart: posting for value %d must be served", i)
	}

	// Graceful shutdown while the flush cycle is still active (matches
	// production: the cycle is stopped as part of, not before, teardown).
	require.NoError(t, store.Shutdown(ctx))
	shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	require.NoError(t, flushCycle.StopAndWait(shutCtx))
	cancel()

	dumpDir(t, "post-shutdown ingest dir (real cycle)", ingestDir)

	require.NoError(t, os.RemoveAll(backupDir))
	if _, statErr := os.Stat(mainDir); statErr == nil {
		require.NoError(t, os.RemoveAll(mainDir))
	}
	require.NoError(t, os.Rename(ingestDir, mainDir))

	store2, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store2.Shutdown(ctx)

	require.NoError(t, store2.CreateOrLoadBucket(ctx, mainName, opts...))
	reloaded := store2.Bucket(mainName)
	require.NotNil(t, reloaded)

	var missing []uint64
	for i := uint64(0); i < tailStart+tailN; i++ {
		if got := readRangeableEqual(t, reloaded, i); len(got) != 1 || got[0] != i {
			missing = append(missing, i)
		}
	}
	require.Emptyf(t, missing,
		"post-restart: %d/%d postings vanished across the graceful restart "+
			"(real flush cycle); missing docIDs=%v", len(missing), tailStart+tailN, missing)
}
