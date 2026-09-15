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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const secondaryPos = 0

// docIDKey encodes a doc id the way the objects bucket stores its secondary key.
func docIDKey(docID uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, docID)
	return key
}

func primaryKey(docID uint64) []byte {
	return fmt.Appendf(nil, "object-%d", docID)
}

func newSecondaryTestBucket(t testing.TB, useBloom, pread bool) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(
		context.Background(), t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(pread),
		WithMinMMapSize(0),
		WithUseBloomFilter(useBloom),
		WithDisableCompaction(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	return b
}

func putDoc(t testing.TB, b *Bucket, docID uint64, value []byte) {
	t.Helper()
	require.NoError(t, b.Put(primaryKey(docID), value, WithSecondaryKey(secondaryPos, docIDKey(docID))))
}

func deleteDoc(t testing.TB, b *Bucket, docID uint64) {
	t.Helper()
	require.NoError(t, b.Delete(primaryKey(docID), WithSecondaryKey(secondaryPos, docIDKey(docID))))
}

// moveActiveToFlushing parks the active memtable in the flushing slot so a
// view sees active, flushing and segments at once. The no-op cycle managers
// never drain it, and Shutdown waits for it, so the cleanup flushes it.
func moveActiveToFlushing(t testing.TB, b *Bucket) {
	t.Helper()
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched, "active memtable must be non-empty to switch")
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

// lookupOneByOne is the reference result: the single-key path once per key.
func lookupOneByOne(t testing.TB, b *Bucket, keys [][]byte) [][]byte {
	t.Helper()
	out := make([][]byte, len(keys))
	for i, key := range keys {
		value, _, err := b.GetBySecondaryWithBuffer(context.Background(), secondaryPos, key, nil)
		require.NoError(t, err)
		out[i] = value
	}
	return out
}

// batchLookup copies each visited value to its caller position, since the
// bytes handed to visit are only valid during the call.
func batchLookup(ctx context.Context, b *Bucket, pos int, keys [][]byte) ([][]byte, error) {
	out := make([][]byte, len(keys))
	err := b.GetBySecondaryBatch(ctx, pos, keys, func(i int, value []byte) error {
		out[i] = bytes.Clone(value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func requireBatchMatchesOneByOne(t testing.TB, b *Bucket, keys [][]byte) {
	t.Helper()
	want := lookupOneByOne(t, b, keys)
	got, err := batchLookup(context.Background(), b, secondaryPos, keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for i := range keys {
		require.Equalf(t, want[i], got[i], "position %d (key %x)", i, keys[i])
	}
}

func TestBucketGetBySecondaryBatch(t *testing.T) {
	type testCase struct {
		name     string
		useBloom bool
		pos      int
		setup    func(t *testing.T, b *Bucket) (keys [][]byte, want [][]byte)
		wantErr  bool
	}
	cases := []testCase{
		{
			name: "no keys",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				return nil, [][]byte{}
			},
		},
		{
			name: "single key",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 1, []byte("one"))
				return [][]byte{docIDKey(1)}, [][]byte{[]byte("one")}
			},
		},
		{
			name: "unknown secondary index position",
			pos:  1,
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 1, []byte("one"))
				return [][]byte{docIDKey(1)}, nil
			},
			wantErr: true,
		},
		{
			name: "tombstone in the newer segment wins over the older live value",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 7, []byte("live-old"))
				putDoc(t, b, 8, []byte("live2-old"))
				require.NoError(t, b.FlushAndSwitch())
				deleteDoc(t, b, 7)
				putDoc(t, b, 8, []byte("live2-new"))
				require.NoError(t, b.FlushAndSwitch())
				return [][]byte{docIDKey(7), docIDKey(8)}, [][]byte{nil, []byte("live2-new")}
			},
		},
		{
			name: "deleted and re-added across segments returns the newest value",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 55, []byte("v1"))
				putDoc(t, b, 56, []byte("c56-1"))
				require.NoError(t, b.FlushAndSwitch())
				deleteDoc(t, b, 55)
				require.NoError(t, b.FlushAndSwitch())
				putDoc(t, b, 55, []byte("v3"))
				putDoc(t, b, 56, []byte("c56-3"))
				require.NoError(t, b.FlushAndSwitch())
				return [][]byte{docIDKey(55), docIDKey(56)}, [][]byte{[]byte("v3"), []byte("c56-3")}
			},
		},
		{
			name: "re-added under a new secondary key hides the old secondary key",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 60, []byte("v1"))
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.Put(primaryKey(60), []byte("v2"), WithSecondaryKey(secondaryPos, docIDKey(61))))
				require.NoError(t, b.FlushAndSwitch())
				return [][]byte{docIDKey(60), docIDKey(61)}, [][]byte{nil, []byte("v2")}
			},
		},
		{
			name: "flushing memtable value wins when the active memtable has no newer version",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 43, []byte("seg-live-43"))
				require.NoError(t, b.FlushAndSwitch())
				putDoc(t, b, 42, []byte("flushing-val"))
				moveActiveToFlushing(t, b)
				return [][]byte{docIDKey(42), docIDKey(43)}, [][]byte{[]byte("flushing-val"), []byte("seg-live-43")}
			},
		},
		{
			name: "flushing memtable value is hidden by a newer active version under another secondary key",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 43, []byte("seg-live-43"))
				require.NoError(t, b.FlushAndSwitch())
				putDoc(t, b, 42, []byte("flushing-val"))
				moveActiveToFlushing(t, b)
				require.NoError(t, b.Put(primaryKey(42), []byte("v2"), WithSecondaryKey(secondaryPos, docIDKey(999999))))
				return [][]byte{docIDKey(42), docIDKey(43)}, [][]byte{nil, []byte("seg-live-43")}
			},
		},
		{
			name: "flushing memtable value is hidden by an active tombstone",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 43, []byte("seg-live-43"))
				require.NoError(t, b.FlushAndSwitch())
				putDoc(t, b, 42, []byte("flushing-val"))
				moveActiveToFlushing(t, b)
				require.NoError(t, b.Delete(primaryKey(42), WithSecondaryKey(secondaryPos, docIDKey(888888))))
				return [][]byte{docIDKey(42), docIDKey(43)}, [][]byte{nil, []byte("seg-live-43")}
			},
		},
		{
			name:     "bloom false positive on the newest segment does not hide the older value",
			useBloom: true,
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				for _, d := range []uint64{10, 11, 12, 13, 14} {
					putDoc(t, b, d, []byte{byte(d), 0xAB})
				}
				require.NoError(t, b.FlushAndSwitch())
				for _, d := range []uint64{20, 21, 22} {
					putDoc(t, b, d, []byte{byte(d), 0xCD})
				}
				require.NoError(t, b.FlushAndSwitch())

				view := b.GetConsistentView()
				newest := view.Disk[len(view.Disk)-1].(*segment)
				require.True(t, newest.useBloomFilter)
				for _, d := range []uint64{10, 11, 12, 13, 14} {
					require.False(t, newest.secondaryBloomFilters[secondaryPos].Test(docIDKey(d)))
					newest.secondaryBloomFilters[secondaryPos].Add(docIDKey(d))
				}
				view.ReleaseView()

				keys := [][]byte{docIDKey(10), docIDKey(20), docIDKey(11), docIDKey(9_999_999), docIDKey(12), docIDKey(13), docIDKey(14)}
				want := [][]byte{{10, 0xAB}, {20, 0xCD}, {11, 0xAB}, nil, {12, 0xAB}, {13, 0xAB}, {14, 0xAB}}
				return keys, want
			},
		},
		{
			name: "duplicates and absent keys keep caller order",
			setup: func(t *testing.T, b *Bucket) ([][]byte, [][]byte) {
				putDoc(t, b, 1, []byte("one"))
				putDoc(t, b, 2, []byte("two"))
				require.NoError(t, b.FlushAndSwitch())
				putDoc(t, b, 3, []byte("three"))
				keys := [][]byte{docIDKey(3), docIDKey(100), docIDKey(1), docIDKey(3), docIDKey(2), docIDKey(1)}
				want := [][]byte{[]byte("three"), nil, []byte("one"), []byte("three"), []byte("two"), []byte("one")}
				return keys, want
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newSecondaryTestBucket(t, tc.useBloom, true)
			keys, want := tc.setup(t, b)

			got, err := batchLookup(context.Background(), b, tc.pos, keys)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.Equal(t, lookupOneByOne(t, b, keys), got)
		})
	}
}

// buildRandomizedSecondaryBucket writes several overlapping flushed segments
// with tombstones, re-adds a fixed prefix of doc ids under a new secondary key
// in a newer segment, and leaves data in both the flushing and the active
// memtable, so every resolution path is reachable.
func buildRandomizedSecondaryBucket(t testing.TB, seed int64, useBloom, pread bool) (*Bucket, []uint64) {
	t.Helper()
	const (
		universeSize     = 300
		segments         = 5
		rekeyedPrefix    = 60
		rekeyedDocIDBase = 9_000_000
	)
	b := newSecondaryTestBucket(t, useBloom, pread)
	rng := rand.New(rand.NewSource(seed))

	universe := make([]uint64, universeSize)
	for i := range universe {
		universe[i] = uint64(i)
	}
	putVersion := func(d uint64, version int) {
		value := make([]byte, 24)
		rng.Read(value)
		value[0] = byte(version)
		putDoc(t, b, d, value)
	}

	for s := 0; s < segments; s++ {
		for _, d := range universe {
			switch r := rng.Intn(100); {
			case r < 45:
				putVersion(d, s)
			case r < 55:
				deleteDoc(t, b, d)
			}
		}
		require.NoError(t, b.FlushAndSwitch())
	}

	for _, d := range universe[:rekeyedPrefix] {
		putVersion(d, segments)
	}
	require.NoError(t, b.FlushAndSwitch())
	for _, d := range universe[:rekeyedPrefix] {
		value := make([]byte, 24)
		rng.Read(value)
		require.NoError(t, b.Put(primaryKey(d), value, WithSecondaryKey(secondaryPos, docIDKey(d+rekeyedDocIDBase))))
	}
	require.NoError(t, b.FlushAndSwitch())

	putVersion(universe[rekeyedPrefix+rng.Intn(universeSize-rekeyedPrefix)], segments)
	for _, d := range universe[rekeyedPrefix:] {
		if rng.Intn(100) < 30 {
			putVersion(d, segments)
		}
	}
	moveActiveToFlushing(t, b)
	for _, d := range universe[rekeyedPrefix:] {
		if rng.Intn(100) < 25 {
			putVersion(d, segments+1)
		}
	}
	return b, universe
}

func TestBucketGetBySecondaryBatchMatchesPerKeyLookup(t *testing.T) {
	for _, pread := range []bool{true, false} {
		for _, useBloom := range []bool{false, true} {
			for _, seed := range []int64{1, 7, 42, 1337, 90210} {
				t.Run(fmt.Sprintf("pread=%v/bloom=%v/seed=%d", pread, useBloom, seed), func(t *testing.T) {
					b, universe := buildRandomizedSecondaryBucket(t, seed, useBloom, pread)
					rng := rand.New(rand.NewSource(seed * 31))

					keys := make([][]byte, 0, len(universe)+40)
					for _, d := range universe {
						keys = append(keys, docIDKey(d))
					}
					for i := range 20 {
						keys = append(keys, docIDKey(1_000_000+uint64(i)))
					}
					for range 20 {
						keys = append(keys, docIDKey(universe[rng.Intn(len(universe))]))
					}
					rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

					requireBatchMatchesOneByOne(t, b, keys)
				})
			}
		}
	}
}

// observedSegment runs before and after around every secondary lookup on the
// wrapped segment, and returns fail's error instead of looking up when set.
type observedSegment struct {
	Segment
	before, after func()
	fail          func() error
}

func (s *observedSegment) getBySecondary(pos int, key, buffer []byte) ([]byte, []byte, []byte, error) {
	if s.before != nil {
		s.before()
	}
	if s.after != nil {
		defer s.after()
	}
	if s.fail != nil {
		if err := s.fail(); err != nil {
			return nil, nil, nil, err
		}
	}
	return s.Segment.getBySecondary(pos, key, buffer)
}

// newSingleSegmentBucket flushes numKeys docs into one segment and wraps that
// segment in observe.
func newSingleSegmentBucket(t testing.TB, numKeys int, observe *observedSegment) (*Bucket, [][]byte) {
	t.Helper()
	b := newSecondaryTestBucket(t, false, true)
	keys := make([][]byte, numKeys)
	for d := range numKeys {
		value := make([]byte, 48)
		value[0], value[1] = byte(d), byte(d>>8)
		putDoc(t, b, uint64(d), value)
		keys[d] = docIDKey(uint64(d))
	}
	require.NoError(t, b.FlushAndSwitch())

	b.disk.maintenanceLock.Lock()
	defer b.disk.maintenanceLock.Unlock()
	require.Len(t, b.disk.segments, 1)
	observe.Segment = b.disk.segments[0]
	b.disk.segments[0] = observe
	return b, keys
}

// inflightProbe records the peak number of concurrent lookups. Each lookup
// waits until target lookups are in flight, or until grace passes, so a
// serial caller observes a peak of 1 after grace instead of hanging.
type inflightProbe struct {
	mu       sync.Mutex
	inflight int
	peak     int
	target   int
	grace    time.Duration
	gate     chan struct{}
	openOnce sync.Once
	armOnce  sync.Once
}

func newInflightProbe(target int, grace time.Duration) *inflightProbe {
	return &inflightProbe{target: target, grace: grace, gate: make(chan struct{})}
}

func (p *inflightProbe) openGate() { p.openOnce.Do(func() { close(p.gate) }) }

func (p *inflightProbe) enter() {
	p.armOnce.Do(func() { time.AfterFunc(p.grace, p.openGate) })
	reached := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.inflight++
		p.peak = max(p.peak, p.inflight)
		return p.inflight >= p.target
	}()
	if reached {
		p.openGate()
	}
	<-p.gate
}

func (p *inflightProbe) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight--
}

func (p *inflightProbe) peakInflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

func TestBucketGetBySecondaryBatchHonoursConcurrencyBudget(t *testing.T) {
	const (
		numKeys   = 2000
		chunks    = (numKeys + secondaryBatchChunkSize - 1) / secondaryBatchChunkSize
		probeWait = 2 * time.Second
	)
	cases := []struct {
		name     string
		budget   int
		wantPeak int
	}{
		{name: "budget above the worker cap fans out to the cap", budget: 4 * secondaryBatchWorkers, wantPeak: min(secondaryBatchWorkers, chunks)},
		{name: "budget below the worker cap bounds the fan-out", budget: 2, wantPeak: 2},
		{name: "budget of one resolves serially", budget: 1, wantPeak: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := newInflightProbe(tc.wantPeak, probeWait)
			b, keys := newSingleSegmentBucket(t, numKeys, &observedSegment{before: probe.enter, after: probe.leave})
			ctx := concurrency.CtxWithBudget(context.Background(), tc.budget)

			visited := 0
			var visitedMu sync.Mutex
			err := b.GetBySecondaryBatch(ctx, secondaryPos, keys, func(int, []byte) error {
				visitedMu.Lock()
				defer visitedMu.Unlock()
				visited++
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, numKeys, visited)
			require.Equal(t, tc.wantPeak, probe.peakInflight())
		})
	}

	t.Run("one by one control never overlaps", func(t *testing.T) {
		probe := newInflightProbe(secondaryBatchWorkers, probeWait)
		b, keys := newSingleSegmentBucket(t, numKeys, &observedSegment{before: probe.enter, after: probe.leave})

		lookupOneByOne(t, b, keys)
		require.Equal(t, 1, probe.peakInflight())
	})
}

func TestBucketGetBySecondaryBatchStopsEarly(t *testing.T) {
	const numKeys = 2000
	// after the first failure every other worker finishes at most its current chunk
	const fanOutBound = secondaryBatchWorkers * secondaryBatchChunkSize
	readErr := errors.New("segment read failed")
	visitErr := errors.New("visit failed")

	type testCase struct {
		name       string
		budget     int
		failRead   bool
		failVisit  bool
		cancel     bool
		wantErr    error
		maxLookups int64
	}
	cases := []testCase{
		{name: "read error with workers", budget: secondaryBatchWorkers, failRead: true, wantErr: readErr, maxLookups: fanOutBound},
		{name: "read error inline", budget: 1, failRead: true, wantErr: readErr, maxLookups: 1},
		{name: "visit error with workers", budget: secondaryBatchWorkers, failVisit: true, wantErr: visitErr, maxLookups: fanOutBound},
		{name: "visit error inline", budget: 1, failVisit: true, wantErr: visitErr, maxLookups: 1},
		{name: "cancelled during the first lookup with workers", budget: secondaryBatchWorkers, cancel: true, wantErr: context.Canceled, maxLookups: fanOutBound},
		{name: "cancelled during the first lookup inline", budget: 1, cancel: true, wantErr: context.Canceled, maxLookups: secondaryBatchChunkSize},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(concurrency.CtxWithBudget(context.Background(), tc.budget))
			defer cancel()

			var lookups atomic.Int64
			var first sync.Once
			observe := &observedSegment{before: func() {
				lookups.Add(1)
				if tc.cancel {
					first.Do(cancel)
				}
			}}
			if tc.failRead {
				observe.fail = func() error {
					var err error
					first.Do(func() { err = readErr })
					return err
				}
			}
			b, keys := newSingleSegmentBucket(t, numKeys, observe)

			err := b.GetBySecondaryBatch(ctx, secondaryPos, keys, func(int, []byte) error {
				if tc.failVisit {
					return visitErr
				}
				return nil
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.LessOrEqual(t, lookups.Load(), tc.maxLookups)
		})
	}
}

// TestBucketGetBySecondaryBatchReadsOneViewWhileWritesLand holds the batch at
// its first lookup, lets a writer switch the memtable, delete and rewrite keys
// and flush twice, and expects the batch to still see the state its view was
// taken on.
func TestBucketGetBySecondaryBatchReadsOneViewWhileWritesLand(t *testing.T) {
	const numKeys = 200
	writerMayStart := make(chan struct{})
	writerDone := make(chan struct{})
	var gateArmed atomic.Bool
	var startOnce sync.Once
	observe := &observedSegment{before: func() {
		if !gateArmed.Load() {
			return
		}
		startOnce.Do(func() { close(writerMayStart) })
		<-writerDone
	}}
	b, keys := newSingleSegmentBucket(t, numKeys, observe)
	// a non-empty active memtable, so the writer's first switch moves it out
	// of the write path while the batch's view still holds it
	putDoc(t, b, numKeys, []byte("in-active-memtable"))
	keys = append(keys, docIDKey(numKeys))
	before := lookupOneByOne(t, b, keys)
	gateArmed.Store(true)
	defer gateArmed.Store(false)

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.Go(func() error {
		defer close(writerDone)
		<-writerMayStart
		if err := b.FlushAndSwitch(); err != nil {
			return err
		}
		for d := range uint64(numKeys / 2) {
			if err := b.Delete(primaryKey(d), WithSecondaryKey(secondaryPos, docIDKey(d))); err != nil {
				return err
			}
		}
		for d := uint64(numKeys / 2); d <= numKeys; d++ {
			if err := b.Put(primaryKey(d), []byte("rewritten"), WithSecondaryKey(secondaryPos, docIDKey(d))); err != nil {
				return err
			}
		}
		return b.FlushAndSwitch()
	})

	got, err := batchLookup(context.Background(), b, secondaryPos, keys)
	require.NoError(t, err)
	require.NoError(t, eg.Wait())
	gateArmed.Store(false)
	require.Equal(t, before, got)
	require.NotEqual(t, before, lookupOneByOne(t, b, keys), "the writes must have landed")
}

// TestBucketGetBySecondaryBatchHandsOutTheWorkerBuffer pins the callback
// shape: with pread every found value is read into the worker's reused buffer
// and handed to visit as is, so on the inline path every visit aliases the
// same array. A per-value copy, or a buffer reset on a miss, would hand out
// a fresh array each time.
func TestBucketGetBySecondaryBatchHandsOutTheWorkerBuffer(t *testing.T) {
	const numKeys = 500
	b := newSecondaryTestBucket(t, false, true)
	keys := make([][]byte, 0, 2*numKeys)
	// equal-width doc ids give equal-length primary keys, so every stored node
	// has the same size and the buffer never has to grow
	for d := uint64(1000); d < 1000+numKeys; d++ {
		putDoc(t, b, d, bytes.Repeat([]byte{byte(d)}, 256))
		keys = append(keys, docIDKey(d), docIDKey(d+1_000_000)) // a miss between hits
	}
	require.NoError(t, b.FlushAndSwitch())
	ctx := concurrency.CtxWithBudget(context.Background(), 1)

	visited := 0
	arrays := map[*byte]bool{}
	err := b.GetBySecondaryBatch(ctx, secondaryPos, keys, func(_ int, value []byte) error {
		visited++
		arrays[unsafe.SliceData(value)] = true
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numKeys, visited)
	require.Len(t, arrays, 1, "every visit must alias the one reused worker buffer")
}
