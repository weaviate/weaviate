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
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// stageIngestBucketAtCanonicalName reproduces the on-disk + in-memory state a
// runtime reindex leaves at commit time: canonical name registered but served
// from an ..._ingest_<gen> sidecar, with the canonical dir already trimmed away.
func stageIngestBucketAtCanonicalName(t *testing.T, ctx context.Context, store *Store,
	canonicalName string, seed map[string]string,
) (canonicalDir, ingestDir string) {
	t.Helper()
	return stageIngestBucketAtCanonicalNameOpts(t, ctx, store, canonicalName,
		func(b *Bucket) {
			for k, v := range seed {
				require.NoError(t, b.Put([]byte(k), []byte(v)))
			}
		}, WithStrategy(StrategyReplace))
}

// stageIngestBucketAtCanonicalNameOpts is [stageIngestBucketAtCanonicalName]
// for any bucket strategy: opts configure both buckets, seedIngest writes the
// pre-swap data into the ingest bucket.
func stageIngestBucketAtCanonicalNameOpts(t *testing.T, ctx context.Context, store *Store,
	canonicalName string, seedIngest func(b *Bucket), opts ...BucketOption,
) (canonicalDir, ingestDir string) {
	t.Helper()
	ingestName := canonicalName + "__ingest_0"

	// OLD canonical bucket (its dir is what a downgrade will open), and the NEW
	// ingest bucket that holds the reindexed data.
	require.NoError(t, store.CreateOrLoadBucket(ctx, canonicalName, opts...))
	require.NoError(t, store.CreateOrLoadBucket(ctx, ingestName, opts...))

	if seedIngest != nil {
		seedIngest(store.Bucket(ingestName))
	}

	canonicalDir = store.Bucket(canonicalName).GetDir()
	ingestDir = store.Bucket(ingestName).GetDir()

	// STAGE (mirrors runtimeSwap) then TRIM (mirrors the commit path): the
	// pointer now serves from ingestDir; canonicalDir is gone on disk.
	displaced, err := store.SwapBucketPointer(ctx, canonicalName, ingestName)
	require.NoError(t, err)
	require.NoError(t, displaced.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(canonicalDir))

	require.Equal(t, ingestDir, store.Bucket(canonicalName).GetDir(),
		"precondition: live bucket must serve from the ingest sidecar before finalize")
	require.NoDirExists(t, canonicalDir, "precondition: canonical dir trimmed before finalize")
	require.DirExists(t, ingestDir, "precondition: ingest sidecar present before finalize")
	return canonicalDir, ingestDir
}

// TestFinalizeBucketSwapLive_HappyPath pins 0-wi#320: live finalize renames the
// ingest sidecar to the canonical dir, rewrites the pointer, and preserves data.
func TestFinalizeBucketSwapLive_HappyPath(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_title_searchable"
	seed := map[string]string{"a": "1", "b": "2", "c": "3"}
	canonicalDir, ingestDir := stageIngestBucketAtCanonicalName(t, ctx, store, name, seed)

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))

	require.DirExists(t, canonicalDir, "canonical dir must exist on disk after live finalize")
	require.NoDirExists(t, ingestDir, "ingest sidecar must be renamed away after live finalize")
	require.Equal(t, canonicalDir, store.Bucket(name).GetDir(),
		"live bucket must serve from the canonical dir after finalize")

	for k, v := range seed {
		got, err := store.Bucket(name).Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), got, "value written before finalize must survive")
	}

	// Idempotent: a second call is a no-op.
	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	require.Equal(t, canonicalDir, store.Bucket(name).GetDir())
}

// TestFinalizeBucketSwapLive_FlushesBufferedActiveMemtable pins 0-wi#320:
// writes still buffered in the active memtable at finalize time must survive,
// not be lost like FinalizeBucketSwap would cause on a live bucket.
func TestFinalizeBucketSwapLive_FlushesBufferedActiveMemtable(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_body_searchable"
	// Write after staging so values live only in the active memtable (no flush).
	canonicalDir, _ := stageIngestBucketAtCanonicalName(t, ctx, store, name, nil)

	buffered := map[string]string{"live-1": "x", "live-2": "y", "live-3": "z"}
	for k, v := range buffered {
		require.NoError(t, store.Bucket(name).Put([]byte(k), []byte(v)))
	}

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))

	for k, v := range buffered {
		got, err := store.Bucket(name).Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), got,
			"LOST WRITE: value buffered in the active memtable at finalize time is gone")
	}
}

// TestFinalizeBucketSwapLive_ConcurrentReadsReturnCorrectResults pins 0-wi#320
// (c): pinned readers (AcquireBucketForRead) must see correct, never torn,
// data throughout a live finalize. Run with -race.
func TestFinalizeBucketSwapLive_ConcurrentReadsReturnCorrectResults(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_concurrent_reads_searchable"
	const nKeys = 200
	seed := make(map[string]string, nKeys)
	for i := 0; i < nKeys; i++ {
		seed[fmt.Sprintf("k%04d", i)] = fmt.Sprintf("v%04d", i)
	}
	canonicalDir, _ := stageIngestBucketAtCanonicalName(t, ctx, store, name, seed)

	// Pinned read mirrors the shard's tokenization-pin path, exercising the
	// finalize drain (lifetimeLock).
	readPinnedOnce := func(key, want string) error {
		b, release := store.AcquireBucketForRead(name)
		if b == nil {
			release()
			return fmt.Errorf("bucket %q disappeared mid-finalize", name)
		}
		got, err := b.Get([]byte(key))
		release()
		if err != nil {
			return fmt.Errorf("get %q: %w", key, err)
		}
		if string(got) != want {
			return fmt.Errorf("stale/lost read for %q: got %q want %q", key, got, want)
		}
		return nil
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	const readers = 8
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("k%04d", (r*37+i)%nKeys)
				if err := readPinnedOnce(key, "v"+key[1:]); err != nil {
					errCh <- err
					return
				}
			}
		}(r)
	}

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	require.Equal(t, canonicalDir, store.Bucket(name).GetDir())
}

// TestFinalizeBucketSwapLive_ConcurrentWritesNotLost pins 0-wi#320 (d): writes
// acknowledged during a live finalize must never be lost. Run with -race.
func TestFinalizeBucketSwapLive_ConcurrentWritesNotLost(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_concurrent_writes_searchable"
	canonicalDir, _ := stageIngestBucketAtCanonicalName(t, ctx, store, name, nil)

	var stop atomic.Bool
	var wg sync.WaitGroup
	const writers = 8
	acked := make([][]string, writers)
	errCh := make(chan error, writers)
	var startWrites sync.WaitGroup
	startWrites.Add(writers)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			startWrites.Done()
			i := 0
			for !stop.Load() {
				key := fmt.Sprintf("w%d-%06d", w, i)
				val := fmt.Sprintf("val-%d-%06d", w, i)
				// store.Bucket resolves fresh each call; the map entry is never remapped.
				if err := store.Bucket(name).Put([]byte(key), []byte(val)); err != nil {
					errCh <- fmt.Errorf("put %q: %w", key, err)
					return
				}
				acked[w] = append(acked[w], key)
				i++
			}
		}(w)
	}

	// Ensure all writers are in-flight so the finalize overlaps live writes.
	startWrites.Wait()
	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	require.Equal(t, canonicalDir, store.Bucket(name).GetDir())

	total := 0
	for w := 0; w < writers; w++ {
		for _, key := range acked[w] {
			got, err := store.Bucket(name).Get([]byte(key))
			require.NoError(t, err)
			require.NotEmpty(t, got, "LOST WRITE: acknowledged key %q missing after finalize", key)
			total += len(got)
		}
	}
	require.Positive(t, total, "expected writers to have acknowledged at least one write")
}

// TestFinalizeBucketSwapLive_InvertedStrategy pins the StrategyInverted
// branches of flushActiveMemtableInPlace: avg-property-length bookkeeping and
// the #9104 tombstone merge must survive a live finalize.
func TestFinalizeBucketSwapLive_InvertedStrategy(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_title_searchable"
	term := []byte("term")
	canonicalDir, ingestDir := stageIngestBucketAtCanonicalNameOpts(t, ctx, store, name,
		func(b *Bucket) {
			// Docs 10..29 flushed to disk so the finalize-time tombstone merge
			// has a segment to hit.
			for id := uint64(10); id < 30; id++ {
				require.NoError(t, b.MapSet(term, NewMapPairFromDocIdAndTf(id, float32(id), 1, false)))
			}
			require.NoError(t, b.FlushAndSwitch())
		}, WithStrategy(StrategyInverted))

	// Buffered-only post-swap writes: new docs plus a tombstone for a doc in
	// the flushed segment.
	b := store.Bucket(name)
	for id := uint64(100); id < 105; id++ {
		require.NoError(t, b.MapSet(term, NewMapPairFromDocIdAndTf(id, float32(id), 1, false)))
	}
	tomb := NewMapPairFromDocIdAndTf(15, 1, 1, true)
	require.NoError(t, b.MapSet(term, tomb))
	require.NoError(t, b.MapDeleteKey(term, tomb.Key))

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	require.DirExists(t, canonicalDir)
	require.NoDirExists(t, ingestDir)

	pairs, err := store.Bucket(name).MapList(ctx, term)
	require.NoError(t, err)
	got := make(map[uint64]bool, len(pairs))
	for _, p := range pairs {
		got[binary.BigEndian.Uint64(p.Key)] = true
	}
	for id := uint64(10); id < 30; id++ {
		if id == 15 {
			require.False(t, got[id],
				"doc 15 was tombstoned in the buffered memtable; the finalize flush must merge that tombstone into the disk segments")
			continue
		}
		require.True(t, got[id], "pre-swap doc %d must survive the live finalize", id)
	}
	for id := uint64(100); id < 105; id++ {
		require.True(t, got[id], "LOST WRITE: buffered posting for doc %d gone after live finalize", id)
	}
}

// TestFinalizeBucketSwapLive_RoaringSetStrategy pins the roaringset branch
// (filterable buckets): buffered adds/removes must survive the finalize
// flush + rename.
func TestFinalizeBucketSwapLive_RoaringSetStrategy(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_category"
	key := []byte("filter-key")
	canonicalDir, ingestDir := stageIngestBucketAtCanonicalNameOpts(t, ctx, store, name,
		func(b *Bucket) {
			for v := uint64(1); v <= 10; v++ {
				require.NoError(t, b.RoaringSetAddOne(key, v))
			}
			require.NoError(t, b.FlushAndSwitch())
		}, WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))

	// Buffered-only post-swap mutations: new values + a removal of a flushed one.
	b := store.Bucket(name)
	for v := uint64(100); v < 105; v++ {
		require.NoError(t, b.RoaringSetAddOne(key, v))
	}
	require.NoError(t, b.RoaringSetRemoveOne(key, 5))

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	require.DirExists(t, canonicalDir)
	require.NoDirExists(t, ingestDir)

	bm, release, err := store.Bucket(name).RoaringSetGet(ctx, key)
	require.NoError(t, err)
	defer release()
	for v := uint64(1); v <= 10; v++ {
		if v == 5 {
			require.False(t, bm.Contains(v), "buffered removal of value 5 must survive the live finalize")
			continue
		}
		require.True(t, bm.Contains(v), "pre-swap value %d must survive the live finalize", v)
	}
	for v := uint64(100); v < 105; v++ {
		require.True(t, bm.Contains(v), "LOST WRITE: buffered value %d gone after live finalize", v)
	}
}

// TestFinalizeBucketSwapLive_ConcurrentUnpinnedReadsInverted pins 0-wi#320 (c)
// for unpinned readers (bare Store.Bucket(), the production BM25/filter path):
// they are not drained by the finalize but must stay correct anyway (see the
// FinalizeBucketSwapLive godoc). Run with -race.
func TestFinalizeBucketSwapLive_ConcurrentUnpinnedReadsInverted(t *testing.T) {
	ctx := context.Background()
	store := newTestStoreForDrain(t)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const name = "property_unpinned_searchable"
	term := []byte("term")
	const nDocs = 50
	canonicalDir, _ := stageIngestBucketAtCanonicalNameOpts(t, ctx, store, name,
		func(b *Bucket) {
			for id := uint64(0); id < nDocs; id++ {
				require.NoError(t, b.MapSet(term, NewMapPairFromDocIdAndTf(id, float32(id+1), 1, false)))
			}
			require.NoError(t, b.FlushAndSwitch())
		}, WithStrategy(StrategyInverted))

	var stop atomic.Bool
	var wg sync.WaitGroup
	const readers = 8
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				pairs, err := store.Bucket(name).MapList(ctx, term)
				if err != nil {
					errCh <- fmt.Errorf("unpinned MapList: %w", err)
					return
				}
				if len(pairs) != nDocs {
					errCh <- fmt.Errorf("unpinned MapList: got %d postings, want %d", len(pairs), nDocs)
					return
				}
			}
		}()
	}

	require.NoError(t, store.FinalizeBucketSwapLive(ctx, name, canonicalDir))
	stop.Store(true)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	require.Equal(t, canonicalDir, store.Bucket(name).GetDir())
}
