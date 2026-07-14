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
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// gh#313: getBySecondaryCore reads the pointer-captured active memtable twice (the
// resolve, then the recheck) with a disk read in between. A same-docID Put landing in
// that window makes the resolve miss the secondary key in the memtable (resolving from
// an older segment) while the recheck finds the primary key live in the memtable,
// yielding a transient nil for an object that is live the whole time. The
// secondaryRecheckHook / secondaryBatchRecheckHook seams land that concurrent Put at
// exactly the vulnerable point so the false-nil reproduces deterministically instead of
// relying on a timing race.

// TestBucketGetBySecondaryRecheckTransientNilSerial pins the serial
// GetBySecondaryWithBuffer form. RED on head (returns nil); GREEN with the memtable
// re-resolve fix (returns the newest value).
func TestBucketGetBySecondaryRecheckTransientNilSerial(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

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

// TestBucketGetBySecondaryRecheckTransientNilBatch pins the batch
// GetBySecondaryBatchWithView form (n>=2 so the batch phases run, not the n==1
// single-key delegate). RED on head (out[0] nil); GREEN with the fix.
func TestBucketGetBySecondaryRecheckTransientNilBatch(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

	const docA, docB = uint64(100), uint64(200)
	priA, priB := encodePrimaryKey(docA), encodePrimaryKey(docB)
	secA, secB := encodeDocID(docA), encodeDocID(docB)

	vA1 := make([]byte, 24)
	vA1[0] = 1
	vB1 := make([]byte, 24)
	vB1[0] = 1
	require.NoError(t, b.Put(priA, vA1, WithSecondaryKey(secondaryPos, secA)))
	require.NoError(t, b.Put(priB, vB1, WithSecondaryKey(secondaryPos, secB)))
	require.NoError(t, b.FlushAndSwitch()) // both in a segment; active memtable empty

	keys := [][]byte{secA, secB}

	view := b.GetConsistentView()
	got, err := b.GetBySecondaryBatchWithView(ctx, secondaryPos, keys, view)
	view.ReleaseView()
	require.NoError(t, err)
	require.Equal(t, vA1, got[0], "sanity: pre-race batch resolves A")
	require.Equal(t, vB1, got[1], "sanity: pre-race batch resolves B")

	vA2 := make([]byte, 24)
	vA2[0] = 2
	var once sync.Once
	b.secondaryBatchRecheckHook = func() {
		once.Do(func() {
			require.NoError(t, b.Put(priA, vA2, WithSecondaryKey(secondaryPos, secA)))
		})
	}
	defer func() { b.secondaryBatchRecheckHook = nil }()

	view = b.GetConsistentView()
	got, err = b.GetBySecondaryBatchWithView(ctx, secondaryPos, keys, view)
	view.ReleaseView()
	require.NoError(t, err)
	require.NotNil(t, got[0], "gh#313 batch: live object A must not read as nil under a phase-window re-put")
	require.Equal(t, vA2, got[0], "recovered A value must be the newest (memtable) version")
	require.Equal(t, vB1, got[1], "unaffected key B stays correct")
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
	b := newSecondaryBatchTestBucket(t, false)

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

// TestBucketGetBySecondaryResolveFlushingTransientNilBatch pins the flushing-branch
// window through the batch phase-0 resolver (n>=2). RED on head; GREEN with the fix.
func TestBucketGetBySecondaryResolveFlushingTransientNilBatch(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

	const docA, docB = uint64(4100), uint64(4200)
	priA, priB := encodePrimaryKey(docA), encodePrimaryKey(docB)
	secA, secB := encodeDocID(docA), encodeDocID(docB)

	vA1 := make([]byte, 24)
	vA1[0] = 1
	vB1 := make([]byte, 24)
	vB1[0] = 1
	require.NoError(t, b.Put(priA, vA1, WithSecondaryKey(secondaryPos, secA)))
	require.NoError(t, b.Put(priB, vB1, WithSecondaryKey(secondaryPos, secB)))
	forceFlushingMemtable(t, b) // both in the flushing memtable; active empty

	vA2 := make([]byte, 24)
	vA2[0] = 2
	var once sync.Once
	b.secondaryResolveFlushingHook = func() {
		once.Do(func() {
			require.NoError(t, b.Put(priA, vA2, WithSecondaryKey(secondaryPos, secA)))
		})
	}
	defer func() { b.secondaryResolveFlushingHook = nil }()

	view := b.GetConsistentView()
	got, err := b.GetBySecondaryBatchWithView(ctx, secondaryPos, [][]byte{secA, secB}, view)
	view.ReleaseView()
	require.NoError(t, err)
	require.NotNil(t, got[0], "gh#313 flushing branch (batch): live object A must not read as nil")
	require.Equal(t, vA2, got[0], "recovered A value must be the newest (active memtable) version")
	require.Equal(t, vB1, got[1], "unaffected key B stays correct")
}

// TestBucketGetBySecondaryRecheckDeletedReAddedPreserved guards the genuine-supersede
// semantics the recheck exists for: a docID deleted in the active memtable must still
// read as nil even though its primary key is present (as a tombstone). The gh#313 fix
// must not turn a genuine delete into a spurious hit.
func TestBucketGetBySecondaryRecheckDeletedReAddedPreserved(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

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
	b := newSecondaryBatchTestBucket(t, false)

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
// via both the serial and batch paths, asserting an always-live object never reads as
// nil. Duration is GH313_STRESS_SECONDS (default 3s; set to 20+ for a soak).
func TestBucketGetBySecondaryRecheckStressNoFalseNil(t *testing.T) {
	secs := 3
	if v := os.Getenv("GH313_STRESS_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}

	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, true) // bloom on (production shape)

	const docID = uint64(555)
	priKey := encodePrimaryKey(docID)
	secKey := encodeDocID(docID)
	coKey := encodeDocID(uint64(556)) // co-resident second key so the batch path is n>=2

	// seed both keys and flush so a segment holds them and the active memtable starts empty
	seed := make([]byte, 24)
	seed[0] = 1
	require.NoError(t, b.Put(priKey, seed, WithSecondaryKey(secondaryPos, secKey)))
	require.NoError(t, b.Put(encodePrimaryKey(556), seed, WithSecondaryKey(secondaryPos, coKey)))
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
		useBatch := r%2 == 0
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if useBatch {
					out, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{secKey, coKey})
					if err == nil && out[0] == nil {
						falseNils.Add(1)
					}
				} else {
					v, _, err := b.GetBySecondaryWithBuffer(ctx, secondaryPos, secKey, nil)
					if err == nil && v == nil {
						falseNils.Add(1)
					}
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
