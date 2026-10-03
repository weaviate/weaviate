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
	"bytes"
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestMemtableMapConcurrentReadWrite races the lock-free getMap read against the
// serialized appendMapSorted writer on the ACTIVE memtable through the real Bucket.
// It does not flush during the run, so MapList hits the skip list (the wired
// keyMapLockFree branch), not a segment — the integration the unit tests bypass.
// Under -race: no data race, every posting list stays strictly ascending / deduped,
// and nothing panics.
func TestMemtableMapConcurrentReadWrite(t *testing.T) {
	prevSkipList := useSkipListMemtable
	useSkipListMemtable = true
	t.Cleanup(func() { useSkipListMemtable = prevSkipList })

	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyMapCollection))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	const rows = 32
	rowKey := func(i int) []byte { return []byte("row-" + strconv.Itoa(i%rows)) }
	docKey := func(id uint64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, id)
		return b
	}

	// seed the active memtable so every row already has a posting
	for i := 0; i < rows; i++ {
		require.NoError(t, bucket.MapSet(rowKey(i),
			MapPair{Key: docKey(uint64(i)), Value: []byte("seed")}))
	}

	dur := 2 * time.Second
	if v := os.Getenv("BMW_CONCURRENT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dur = time.Duration(n) * time.Second
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
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

	// writers: MapSet with a fresh key buffer per call (the documented contract).
	// appendMapSorted serializes them under the memtable lock; getMap does not wait.
	for w := 0; w < 3; w++ {
		guard(func() {
			r := rand.New(rand.NewSource(int64(w) + 1))
			var id uint64
			for !stop.Load() {
				id++
				if err := bucket.MapSet(rowKey(r.Intn(rows)),
					MapPair{Key: docKey(id), Value: []byte("v")}); err != nil {
					t.Errorf("MapSet: %v", err)
					return
				}
			}
		})
	}

	// readers: the lock-free getMap must always yield a consistent prefix — a
	// strictly ascending, deduped posting list — never a torn or duplicated one.
	for rr := 0; rr < 6; rr++ {
		guard(func() {
			r := rand.New(rand.NewSource(int64(1000 + rr)))
			for !stop.Load() {
				pairs, err := bucket.MapList(ctx, rowKey(r.Intn(rows)))
				if err != nil {
					t.Errorf("MapList: %v", err)
					return
				}
				for k := 1; k < len(pairs); k++ {
					if bytes.Compare(pairs[k-1].Key, pairs[k].Key) >= 0 {
						t.Errorf("MapList result not strictly ascending/deduped at %d", k)
						return
					}
				}
			}
		})
	}

	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
}
