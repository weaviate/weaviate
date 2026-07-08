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
	"encoding/binary"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBlockMaxWandMergeFilterConcurrent runs many filtered queries folding
// tombstones into one shared filter (sroar.AndNot) while other goroutines
// tombstone docs — the regime that got the in-place-AndNot #10172 version
// reverted for mutating the shared filter. Under -race it asserts the shared
// filter is never mutated (cardinality and full membership fixed), every result
// stays within the filter, and nothing panics.
//
// Setup: seed alpha/beta/gamma posting lists over nDocs, then tombstone a subset
// of alpha docs — one batch flushed into a segment, another left in the active
// memtable — so every query folds both tombstone sources. The filter admits
// every other doc. Then, concurrently: query workers run the folded search and
// assert every returned id is in the filter; writer goroutines keep tombstoning
// random alpha docs; a watcher asserts the shared filter bitmap never changes.
func TestBlockMaxWandMergeFilterConcurrent(t *testing.T) {
	// Surface a merge panic instead of letting createDiskTermFromCV's recover
	// swallow it.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")

	prevRatio := bm25MergeGateRatio
	bm25MergeGateRatio = 0 // force the merge on for every query
	t.Cleanup(func() { bm25MergeGateRatio = prevRatio })

	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	queries := []string{"alpha", "beta", "gamma"}
	const nDocs = 20000
	// ids across sroar container bands so tombstones and the fold span containers
	docID := func(i int) uint64 { return uint64((i%4)<<16) + uint64(i) }

	for i := 0; i < nDocs; i++ {
		id := docID(i)
		require.NoError(t, bucket.MapSet([]byte("alpha"), NewMapPairFromDocIdAndTf(id, float32(1+i%5), 1, false)))
		if i%2 == 0 {
			require.NoError(t, bucket.MapSet([]byte("beta"), NewMapPairFromDocIdAndTf(id, 2, 1, false)))
		}
		if i%3 == 0 {
			require.NoError(t, bucket.MapSet([]byte("gamma"), NewMapPairFromDocIdAndTf(id, 3, 1, false)))
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// Pre-build a tombstone segment and leave more tombstones in the active
	// memtable so every query folds both. Flushing happens here, not during the
	// concurrent phase: concurrent FlushAndSwitch races the query's memtable read
	// on a pre-existing, merge-unrelated path, and the fold only needs tombstones
	// present, not flushes in flight.
	// Fresh key per delete: MapDeleteKey retains the key in the memtable's map,
	// and the first batch is flushed below, so a reused buffer would persist stale
	// keys into the segment.
	for i := 0; i < nDocs; i += 5 {
		delKey := make([]byte, 8)
		binary.BigEndian.PutUint64(delKey, docID(i))
		require.NoError(t, bucket.MapDeleteKey([]byte("alpha"), delKey))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	for i := 1; i < nDocs; i += 7 {
		delKey := make([]byte, 8)
		binary.BigEndian.PutUint64(delKey, docID(i))
		require.NoError(t, bucket.MapDeleteKey([]byte("alpha"), delKey))
	}

	// The shared filter (every other seeded doc). The merge folds tombstones into
	// a private clone, so its contents must be invariant for the whole run.
	filterBm := sroar.NewBitmap()
	for i := 0; i < nDocs; i += 2 {
		filterBm.Set(docID(i))
	}
	wantCard := filterBm.GetCardinality()
	filter := helpers.NewAllowListFromBitmap(filterBm)
	inFilter := func(id uint64) bool { return id%(1<<16) < nDocs && (id%(1<<16))%2 == 0 }

	dur := 2 * time.Second

	var stop atomic.Bool
	var wg sync.WaitGroup
	var queryRuns, writeRuns atomic.Uint64

	guard := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic in goroutine: %v\n%s", r, debug.Stack())
				}
			}()
			fn()
		}()
	}

	// Writers tombstone random docs. Deletes are what the fold subtracts, and —
	// unlike inserts — they never touch the prop-length counters, so this stays a
	// clean -race test of the merge path, not of the unrelated GetPropLengths race.
	for w := 0; w < 4; w++ {
		guard(func() {
			r := rand.New(rand.NewSource(int64(1000 + w)))
			for !stop.Load() {
				// fresh key per delete: MapDeleteKey retains it by reference, so
				// reusing the buffer races concurrent readers of a prior key's bytes.
				mapKey := make([]byte, 8)
				binary.BigEndian.PutUint64(mapKey, docID(r.Intn(nDocs)))
				if err := bucket.MapDeleteKey([]byte("alpha"), mapKey); err != nil {
					t.Errorf("MapDeleteKey: %v", err)
					return
				}
				writeRuns.Add(1)
			}
		})
	}

	for range 4 {
		guard(func() {
			for !stop.Load() {
				// queryBlockMaxWand, not runBlockMaxWand: require's FailNow is
				// illegal off the test goroutine.
				got, err := queryBlockMaxWand(bucket, queries, filter, nDocs, 10, nil)
				if err != nil {
					t.Errorf("query: %v", err)
					return
				}
				for id := range got {
					if !inFilter(id) {
						t.Errorf("merged query returned doc %d outside the filter", id)
						return
					}
				}
				queryRuns.Add(1)
			}
		})
	}

	guard(func() {
		for !stop.Load() {
			if got := filterBm.GetCardinality(); got != wantCard {
				t.Errorf("shared filter mutated mid-run: cardinality %d != %d", got, wantCard)
				return
			}
		}
	})

	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()

	// full membership, not just cardinality, must match what we seeded
	for i := 0; i < nDocs; i++ {
		require.Equalf(t, i%2 == 0, filterBm.Contains(docID(i)),
			"shared filter membership changed for doc %d", docID(i))
	}
	require.Equal(t, wantCard, filterBm.GetCardinality(), "shared filter cardinality changed")
	require.Positive(t, queryRuns.Load(), "no queries ran")
	require.Positive(t, writeRuns.Load(), "no writes ran")
	t.Logf("%d queries, %d tombstoning writes over %s", queryRuns.Load(), writeRuns.Load(), dur)
}
