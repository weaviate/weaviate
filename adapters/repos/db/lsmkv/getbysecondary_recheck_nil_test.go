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
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// gh#313: getBySecondaryCore reads the pointer-captured active memtable twice (the
// resolve, then the recheck) with a disk read in between. A same-docID Put landing in
// that window makes the resolve miss the secondary key in the memtable (resolving from
// an older segment) while the recheck finds the primary key live in the memtable,
// yielding a transient nil for an object that is live the whole time. The
// secondaryRecheckHook / secondaryResolveFlushingHook seams land that concurrent Put at
// exactly the vulnerable point so the false-nil reproduces deterministically instead of
// relying on a timing race.

const secondaryPos = 0

// encodeDocID mirrors the little-endian docID secondary key produced by
// upsertObjectDataLSM in shard_write_put.go.
func encodeDocID(docID uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], docID)
	return b[:]
}

// encodePrimaryKey is a 16-byte UUID-shaped primary key, unique per docID.
func encodePrimaryKey(docID uint64) []byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[8:], docID)
	return b[:]
}

// newSecondaryTestBucket builds a Replace bucket with one secondary index, the
// production LTK pread value path, compaction disabled, and any non-empty segment
// mapped so the bloom flag alone picks the read path.
func newSecondaryTestBucket(t testing.TB, useBloom bool) *Bucket {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(true),
		WithMinMMapSize(0),
		WithUseBloomFilter(useBloom),
		WithDisableCompaction(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	return b
}

// forceFlushingMemtable moves the current active memtable into the flushing slot
// (installing a fresh active), so a resolution view observes active + flushing +
// segments at once. Because the test cycle managers are no-ops, nothing drains the
// flushing memtable in the background; the registered cleanup (LIFO, runs before the
// bucket's own Shutdown cleanup) flushes it to a segment, otherwise Shutdown blocks
// forever waiting for b.flushing to clear. Requires a non-empty active memtable (the
// switch is a no-op on an empty one).
func forceFlushingMemtable(t testing.TB, b *Bucket) {
	t.Helper()
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched, "expected a non-empty active memtable to switch into flushing")
	t.Cleanup(func() {
		if b.flushing == nil {
			return
		}
		b.waitForZeroWriters(b.flushing)
		segmentPath, err := b.flushing.flush()
		require.NoError(t, err)
		seg, err := b.disk.initAndPrecomputeNewSegment(segmentPath)
		require.NoError(t, err)
		require.NoError(t, b.atomicallyAddDiskSegmentAndRemoveFlushing(seg))
	})
}

// TestBucketGetBySecondaryRecheckTransientNilSerial pins the serial
// GetBySecondaryWithBuffer form. RED on head (returns nil); GREEN with the memtable
// re-resolve fix (returns the newest value).
func TestBucketGetBySecondaryRecheckTransientNilSerial(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryTestBucket(t, false)

	const docID = uint64(4242)
	priKey := encodePrimaryKey(docID)
	secKey := encodeDocID(docID)

	v1 := make([]byte, 24)
	v1[0] = 1
	require.NoError(t, b.Put(priKey, v1, WithSecondaryKey(secondaryPos, secKey)))
	require.NoError(t, b.FlushAndSwitch()) // secKey->priKey now in a segment; active memtable empty of it

	got, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
	require.NoError(t, err)
	require.Equal(t, v1, got, "sanity: pre-race resolve returns the flushed value")

	v2 := make([]byte, 24)
	v2[0] = 2
	var once sync.Once
	b.secondaryRecheckHook = func() {
		once.Do(func() {
			require.NoError(t, b.Put(priKey, v2, WithSecondaryKey(secondaryPos, secKey)))
		})
	}
	defer func() { b.secondaryRecheckHook = nil }()

	got, _, err = b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "gh#313: live object must not transiently read as nil under a flush-window re-put")
	require.Equal(t, v2, got, "recovered value must be the newest (memtable) version (newest-wins preserved)")
}

// TestBucketGetBySecondaryResolveFlushingTransientNilSerial pins the SECOND gh#313
// window (distinct from the recheck): resolveSecondaryFromMemtables reads the active
// memtable twice - once by the i==0 secondary probe, once by the i==1 flushing branch's
// active-exists check. A same-secondary-key Put landing in the active memtable between
// those two reads makes the i==0 probe miss while the active-exists check finds the
// primary live, so the branch wrongly concludes "re-added under a different secondary
// key" and returns nil for a live object. RED on head; GREEN with resolveSecondaryActiveOnly.
func TestBucketGetBySecondaryResolveFlushingTransientNilSerial(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryTestBucket(t, false)

	const docID = uint64(31337)
	priKey := encodePrimaryKey(docID)
	secKey := encodeDocID(docID)

	v1 := make([]byte, 24)
	v1[0] = 1
	require.NoError(t, b.Put(priKey, v1, WithSecondaryKey(secondaryPos, secKey)))
	forceFlushingMemtable(t, b) // secKey->priKey now lives in the flushing memtable; active is empty

	v2 := make([]byte, 24)
	v2[0] = 2
	var once sync.Once
	b.secondaryResolveFlushingHook = func() {
		once.Do(func() {
			require.NoError(t, b.Put(priKey, v2, WithSecondaryKey(secondaryPos, secKey)))
		})
	}
	defer func() { b.secondaryResolveFlushingHook = nil }()

	got, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "gh#313 flushing branch: live object must not read as nil under a same-docID re-put")
	require.Equal(t, v2, got, "recovered value must be the newest (active memtable) version")
}

// TestBucketGetBySecondaryRecheckDeletedReAddedPreserved guards the genuine-supersede
// semantics the recheck exists for: a docID deleted in the active memtable must still
// read as nil even though its primary key is present (as a tombstone). The gh#313 fix
// must not turn a genuine delete into a spurious hit.
func TestBucketGetBySecondaryRecheckDeletedReAddedPreserved(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryTestBucket(t, false)

	const docID = uint64(777)
	priKey := encodePrimaryKey(docID)
	secKey := encodeDocID(docID)

	v1 := make([]byte, 24)
	v1[0] = 1
	require.NoError(t, b.Put(priKey, v1, WithSecondaryKey(secondaryPos, secKey)))
	require.NoError(t, b.FlushAndSwitch())

	// Delete lands in the active memtable (tombstone under priKey) at the recheck window.
	var once sync.Once
	b.secondaryRecheckHook = func() {
		once.Do(func() {
			require.NoError(t, b.Delete(priKey, WithSecondaryKey(secondaryPos, secKey)))
		})
	}
	defer func() { b.secondaryRecheckHook = nil }()

	got, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
	require.NoError(t, err)
	require.Nil(t, got, "a genuine memtable delete in the recheck window must still resolve to nil")
}

// TestBucketGetBySecondaryFlushingReKeyedPreserved guards the flushing-branch guard's
// original purpose: a primary key present in the flushing memtable under secKey1, then
// re-added in the active memtable under a DIFFERENT secondary key secKey2, must still
// resolve to nil when queried by the stale secKey1. The gh#313 fix (which recovers the
// same-secondary-key re-put) must NOT turn this genuine re-key into a spurious hit.
func TestBucketGetBySecondaryFlushingReKeyedPreserved(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryTestBucket(t, false)

	const docID, newDocID = uint64(880), uint64(881)
	priKey := encodePrimaryKey(docID)
	secKey1 := encodeDocID(docID)
	secKey2 := encodeDocID(newDocID)

	v1 := make([]byte, 24)
	v1[0] = 1
	require.NoError(t, b.Put(priKey, v1, WithSecondaryKey(secondaryPos, secKey1)))
	forceFlushingMemtable(t, b) // secKey1->priKey in flushing; active empty

	v2 := make([]byte, 24)
	v2[0] = 2
	require.NoError(t, b.Put(priKey, v2, WithSecondaryKey(secondaryPos, secKey2))) // re-keyed in active

	got, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey1, nil)
	require.NoError(t, err)
	require.Nil(t, got, "querying the stale secondary key of a re-keyed primary must resolve to nil")

	gotNew, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey2, nil)
	require.NoError(t, err)
	require.Equal(t, v2, gotNew, "the new secondary key must resolve to the re-keyed value")
}

// TestBucketGetBySecondaryRecheckStressNoFalseNil is a probabilistic backstop: real
// concurrent same-docID re-puts + FlushAndSwitch while readers resolve the same docID
// via the serial path, asserting an always-live object never reads as nil. Duration is
// GH313_STRESS_SECONDS (default 3s; set to 20+ for a soak).
func TestBucketGetBySecondaryRecheckStressNoFalseNil(t *testing.T) {
	secs := 3
	if v := os.Getenv("GH313_STRESS_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}

	ctx := context.Background()
	b := newSecondaryTestBucket(t, true) // bloom on (production shape)

	const docID = uint64(555)
	priKey := encodePrimaryKey(docID)
	secKey := encodeDocID(docID)

	// seed and flush so a segment holds the key and the active memtable starts empty
	seed := make([]byte, 24)
	seed[0] = 1
	require.NoError(t, b.Put(priKey, seed, WithSecondaryKey(secondaryPos, secKey)))
	require.NoError(t, b.FlushAndSwitch())

	stop := make(chan struct{})
	var falseNils atomic.Int64
	var version atomic.Uint64
	var wg sync.WaitGroup

	// writer: never deletes docID, so it is live for the whole run; a nil read is a bug.
	wg.Add(1)
	go func() {
		defer wg.Done()
		lastFlush := time.Now()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ver := version.Add(1)
			val := make([]byte, 24)
			val[0] = byte(ver)
			_ = b.Put(priKey, val, WithSecondaryKey(secondaryPos, secKey))
			if time.Since(lastFlush) > 20*time.Millisecond {
				_ = b.FlushAndSwitch()
				lastFlush = time.Now()
			}
		}
	}()

	readers := 6
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
				if err == nil && v == nil {
					falseNils.Add(1)
				}
			}
		}()
	}

	time.Sleep(time.Duration(secs) * time.Second)
	close(stop)
	wg.Wait()

	require.Zero(t, falseNils.Load(),
		"gh#313: an always-live object read nil %d time(s) under flush-concurrent re-puts", falseNils.Load())
}
