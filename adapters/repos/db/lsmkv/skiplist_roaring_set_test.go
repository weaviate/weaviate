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
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// Replaying the op log at read time must resolve to the same bitmaps the
// red-black tree builds by merging in place at write time — in particular the
// add-then-delete / delete-then-add ordering of a single docID.
func TestSkipListRoaringSetMatchesRBTree(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	rb := newRoaringSetIndex(false)
	sl := newRoaringSetIndex(true)

	keySpace := 50
	for i := 0; i < 3000; i++ {
		key := []byte(fmt.Sprintf("key-%03d", rng.Intn(keySpace)))
		var ins roaringset.Insert
		for j := 0; j < rng.Intn(3); j++ {
			ins.Additions = append(ins.Additions, uint64(rng.Intn(500)))
		}
		for j := 0; j < rng.Intn(3); j++ {
			ins.Deletions = append(ins.Deletions, uint64(rng.Intn(500)))
		}
		rb.insert(key, ins)
		sl.insert(key, ins)
	}

	for i := 0; i < keySpace; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		l1, err1 := rb.get(key)
		l2, err2 := sl.get(key)
		require.Equal(t, err1, err2, "get(%s)", key)
		if err1 != nil {
			continue
		}
		require.Equal(t, l1.Additions.ToArray(), l2.Additions.ToArray(), "additions(%s)", key)
		require.Equal(t, l1.Deletions.ToArray(), l2.Deletions.ToArray(), "deletions(%s)", key)
	}

	flat1, flat2 := rb.flattenInOrder(), sl.flattenInOrder()
	require.Equal(t, len(flat1), len(flat2))
	for i := range flat1 {
		require.Equal(t, flat1[i].Key, flat2[i].Key)
		require.Equal(t, flat1[i].Value.Additions.ToArray(), flat2[i].Value.Additions.ToArray())
		require.Equal(t, flat1[i].Value.Deletions.ToArray(), flat2[i].Value.Deletions.ToArray())
	}
}

// A lock-free reader replaying the op log must always see a consistent prefix:
// docIDs are added in ascending order per key, so any gap in the merged bitmap
// means a torn read. Deletions concurrently target already-added docIDs. Run
// under -race.
func TestSkipListRoaringSetConcurrentReadWrite(t *testing.T) {
	const (
		keys           = 16
		writes         = 10000
		readers        = 4
		readsPerReader = 2000
	)
	sl := newSkipListRoaringSet()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		next := make([]uint64, keys)
		for i := 0; i < writes; i++ {
			k := rng.Intn(keys)
			key := []byte(fmt.Sprintf("key-%04d", k))
			if next[k] > 0 && rng.Intn(8) == 0 {
				sl.insert(key, roaringset.Insert{Deletions: []uint64{uint64(rng.Int63n(int64(next[k])))}})
				continue
			}
			sl.insert(key, roaringset.Insert{Additions: []uint64{next[k]}})
			next[k]++
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < readsPerReader; i++ {
				k := rng.Intn(keys)
				layer, err := sl.get([]byte(fmt.Sprintf("key-%04d", k)))
				if err != nil {
					continue // not yet written
				}
				// every docID below the max must be either present or deleted:
				// additions happen in ascending order, so a hole that is in
				// neither bitmap means the reader saw a torn log
				max := layer.Additions.Maximum()
				for d := uint64(0); d < max; d++ {
					if !layer.Additions.Contains(d) && !layer.Deletions.Contains(d) {
						t.Errorf("key %d: docID %d missing from both bitmaps (max %d)", k, d, max)
						return
					}
				}
			}
		}(int64(r))
	}

	wg.Wait()
}
