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

	"github.com/sirupsen/logrus"
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

// rangeableSwapNames holds the bucket names and on-disk dirs for a rangeable
// swap round-trip: the canonical (main) bucket, the deferred-rename ingest
// sidecar, the transient reindex backfill bucket, and the displaced-old-main
// backup dir. The names match the production enable-rangeable layout.
type rangeableSwapNames struct {
	main, ingest, reindex                     string
	mainDir, ingestDir, reindexDir, backupDir string
}

func newRangeableSwapNames(dir string) rangeableSwapNames {
	const (
		main    = "property_score_rangeable"
		ingest  = "property_score_rangeable__rangeable_ingest_1"
		reindex = "property_score_rangeable__rangeable_reindex_1"
		backup  = "property_score_rangeable__rangeable_backup_1"
	)
	return rangeableSwapNames{
		main: main, ingest: ingest, reindex: reindex,
		mainDir:    filepath.Join(dir, main),
		ingestDir:  filepath.Join(dir, ingest),
		reindexDir: filepath.Join(dir, reindex),
		backupDir:  filepath.Join(dir, backup),
	}
}

// openRangeableSwapStore opens a store whose flush cycle is driven by `flush`
// (pass a noop group for the explicit-FlushAndSwitch case, a live group to race
// shutdown against an in-flight flush). Compaction cycles are always noop.
func openRangeableSwapStore(t *testing.T, dir string, logger logrus.FieldLogger,
	flush cyclemanager.CycleCallbackGroup,
) *Store {
	t.Helper()
	store, err := New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), flush)
	require.NoError(t, err)
	return store
}

// buildAndSwapRangeableIngest reproduces the in-process enable-rangeable
// migration plus the phase-2 swap: backfill `backfillN` postings (value==docID)
// through a reindex bucket flushed to real segments, prepend them into a fresh
// ingest bucket, optionally add `doubleWriteN` double-write-window postings and
// (when flushIngest) flush the ingest bulk to segments, then
// SwapBucketPointer(main<-ingest) and rename the displaced old-main dir to
// backup. Returns the live (ex-ingest) bucket, still on disk at the ingest dir.
func buildAndSwapRangeableIngest(t *testing.T, ctx context.Context, store *Store,
	n rangeableSwapNames, opts []BucketOption, backfillN, doubleWriteN int, flushIngest bool,
) *Bucket {
	t.Helper()
	// Empty canonical bucket, as PreReindexHook creates it.
	require.NoError(t, store.CreateOrLoadBucket(ctx, n.main, opts...))

	// Historical postings live in a reindex bucket, flushed to real segments.
	require.NoError(t, store.CreateOrLoadBucket(ctx, n.reindex, opts...))
	reindexBucket := store.Bucket(n.reindex)
	require.NotNil(t, reindexBucket)
	for i := uint64(0); i < uint64(backfillN); i++ {
		require.NoError(t, reindexBucket.RoaringSetRangeAdd(i, i))
	}
	require.NoError(t, reindexBucket.FlushAndSwitch())

	// Ingest bucket: prepend backfill segments, add the double-write window,
	// and (for the on-disk case) flush so the bulk lives in segments and the
	// active memtable is clean — the steady state after the flush cycle drains
	// the migration's bulk writes.
	require.NoError(t, store.CreateOrLoadBucket(ctx, n.ingest, opts...))
	ingestBucket := store.Bucket(n.ingest)
	require.NotNil(t, ingestBucket)
	require.NoError(t, ingestBucket.PrependSegmentsFromBucket(ctx, n.reindexDir))
	for i := uint64(backfillN); i < uint64(backfillN+doubleWriteN); i++ {
		require.NoError(t, ingestBucket.RoaringSetRangeAdd(i, i))
	}
	if flushIngest {
		require.NoError(t, ingestBucket.FlushAndSwitch())
	}

	// Reindex bucket is shut down + dir removed by prep.
	require.NoError(t, store.ShutdownBucket(ctx, n.reindex))
	require.NoError(t, os.RemoveAll(n.reindexDir))

	// Phase 2a: SwapBucketPointer (in-memory pointer flip). Phase 2b: shut down
	// the displaced old-main and rename its dir to backup.
	oldMain, err := store.SwapBucketPointer(ctx, n.main, n.ingest)
	require.NoError(t, err)
	require.NoError(t, oldMain.Shutdown(ctx))
	require.NoError(t, os.Rename(n.mainDir, n.backupDir))

	live := store.Bucket(n.main)
	require.NotNil(t, live)
	require.Equal(t, n.ingestDir, live.GetDir(),
		"live main bucket must still be on disk at the ingest dir")
	return live
}

// finalizeRenameRangeableIngest performs the next-restart finalize
// (finalizeMigrationDir): drop the backup, remove any stale main dir, and
// rename the ingest sidecar dir to the canonical main dir.
func finalizeRenameRangeableIngest(t *testing.T, n rangeableSwapNames) {
	t.Helper()
	require.NoError(t, os.RemoveAll(n.backupDir))
	if _, statErr := os.Stat(n.mainDir); statErr == nil {
		require.NoError(t, os.RemoveAll(n.mainDir))
	}
	require.NoError(t, os.Rename(n.ingestDir, n.mainDir))
}

// reloadRangeableMain reopens the store (all-noop cycles) and loads the promoted
// canonical bucket from its now-renamed dir.
func reloadRangeableMain(t *testing.T, ctx context.Context, dir string,
	logger logrus.FieldLogger, n rangeableSwapNames, opts []BucketOption,
) (*Store, *Bucket) {
	t.Helper()
	store := openRangeableSwapStore(t, dir, logger, cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, store.CreateOrLoadBucket(ctx, n.main, opts...))
	reloaded := store.Bucket(n.main)
	require.NotNil(t, reloaded)
	return store, reloaded
}

// assertRangeableContiguousServed asserts values [0, upTo) each read back as
// exactly {i} — the pre-restart "everything served" (the 1500-pass) state.
func assertRangeableContiguousServed(t *testing.T, b *Bucket, upTo uint64) {
	t.Helper()
	for i := uint64(0); i < upTo; i++ {
		require.Equalf(t, []uint64{i}, readRangeableEqual(t, b, i),
			"pre-restart: posting for value %d must be served", i)
	}
}

// assertNoRangeablePostingsLost asserts every value in [0, upTo) survived the
// round trip; the failure message names each missing docID.
func assertNoRangeablePostingsLost(t *testing.T, b *Bucket, upTo uint64) {
	t.Helper()
	var missing []uint64
	for i := uint64(0); i < upTo; i++ {
		if got := readRangeableEqual(t, b, i); len(got) != 1 || got[0] != i {
			missing = append(missing, i)
		}
	}
	require.Emptyf(t, missing,
		"post-restart: %d/%d postings vanished across the graceful restart; "+
			"missing docIDs=%v", len(missing), upTo, missing)
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
// finalize rename): build a populated ingest bucket (backfill segments +
// unflushed double-write memtable), SwapBucketPointer(main<-ingest), land
// post-flip writes in the ex-ingest bucket's active memtable/WAL, graceful
// shutdown (on-shutdown flush runs — matches RestartAt, not SIGKILL), the
// next-restart finalize rename, then reload and assert every posting present.
func TestRangeableSwap_PostFlipWriteTail_SurvivesGracefulRestart(t *testing.T) {
	t.Run("onDisk", func(t *testing.T) {
		ctx := context.Background()
		dir := t.TempDir()
		logger, _ := test.NewNullLogger()
		names := newRangeableSwapNames(dir)
		opts := rangeableSwapBucketOpts()

		store := openRangeableSwapStore(t, dir, logger, cyclemanager.NewCallbackGroupNoop())

		const (
			backfillN    = 1000 // historical postings 0..999
			doubleWriteN = 100  // double-write window 1000..1099
		)
		live := buildAndSwapRangeableIngest(t, ctx, store, names, opts, backfillN, doubleWriteN, true)

		// Post-flip tail: the authoritative re-PATCH postings that stay in the
		// reused WAL on shutdown (commitlog < MaxReuseWalSize), which is the
		// field shape (node 2: 6/1500 lost across a graceful restart).
		const (
			tailStart = backfillN + doubleWriteN
			tailN     = 6
		)
		for i := uint64(tailStart); i < tailStart+tailN; i++ {
			require.NoError(t, live.RoaringSetRangeAdd(i, i))
		}
		assertRangeableContiguousServed(t, live, tailStart+tailN)

		// Graceful shutdown (RestartAt: flush runs), then next-restart finalize.
		require.NoError(t, store.Shutdown(ctx))
		dumpDir(t, "post-shutdown ingest dir", names.ingestDir)

		finalizeRenameRangeableIngest(t, names)
		dumpDir(t, "post-rename main dir", names.mainDir)

		store2, reloaded := reloadRangeableMain(t, ctx, dir, logger, names, opts)
		defer store2.Shutdown(ctx)
		assertNoRangeablePostingsLost(t, reloaded, tailStart+tailN)
	})
}

// TestRangeableSwap_PostFlipWriteTail_RealFlushCycle drives the same face-3
// swap→post-flip-write→graceful-shutdown→rename→reload round trip, but with the
// PRODUCTION flush cycle running (a real cyclemanager on a fast ticker plus the
// default MaxReuseWalSize) so the registered flush callback switches the WAL
// mid-stream and the graceful shutdown races an in-flight flush — the field
// condition the explicit-FlushAndSwitch test doesn't exercise. It repeats the
// write/switch churn to widen the shutdown-vs-flush window.
func TestRangeableSwap_PostFlipWriteTail_RealFlushCycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	names := newRangeableSwapNames(dir)

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

	store := openRangeableSwapStore(t, dir, logger, flushCallbacks)

	// No double-write window here; the bulk lands via prepend + the live cycle.
	const backfillN = 500
	live := buildAndSwapRangeableIngest(t, ctx, store, names, opts, backfillN, 0, false)

	// Post-flip writes with the flush cycle churning underneath. Pace the
	// writes so the 2ms ticker interleaves flush/switch between them.
	const (
		tailStart = backfillN
		tailN     = 500
	)
	for i := uint64(tailStart); i < tailStart+tailN; i++ {
		require.NoError(t, live.RoaringSetRangeAdd(i, i))
		if i%25 == 0 {
			time.Sleep(3 * time.Millisecond)
		}
	}
	assertRangeableContiguousServed(t, live, tailStart+tailN)

	// Graceful shutdown while the flush cycle is still active (matches
	// production: the cycle is stopped as part of, not before, teardown).
	require.NoError(t, store.Shutdown(ctx))
	shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	require.NoError(t, flushCycle.StopAndWait(shutCtx))
	cancel()
	dumpDir(t, "post-shutdown ingest dir (real cycle)", names.ingestDir)

	finalizeRenameRangeableIngest(t, names)

	store2, reloaded := reloadRangeableMain(t, ctx, dir, logger, names, opts)
	defer store2.Shutdown(ctx)
	assertNoRangeablePostingsLost(t, reloaded, tailStart+tailN)
}
