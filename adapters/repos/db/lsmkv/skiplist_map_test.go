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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func slDocKey(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}

// The skip list must produce byte-identical get/flatten results to the red-black
// tree it replaces, including dedup-keeps-last semantics on repeated MapPair keys.
func TestSkipListMapEquivalence(t *testing.T) {
	sl := newSkipListMap()
	rb := &binarySearchTreeMap{}
	for i := 0; i < 4000; i++ {
		key := []byte(fmt.Sprintf("term-%03d", i%200)) // 200 distinct row keys
		pair := MapPair{Key: slDocKey(uint64(i % 50)), Value: []byte(fmt.Sprintf("v%d", i))}
		sl.insert(key, pair)
		rb.insert(key, pair)
	}
	// also exercise tombstone pairs
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("term-%03d", i%200))
		pair := MapPair{Key: slDocKey(uint64(i % 50)), Tombstone: true}
		sl.insert(key, pair)
		rb.insert(key, pair)
	}

	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("term-%03d", i))
		got, gerr := sl.get(key)
		want, werr := rb.get(key)
		require.Equal(t, werr == nil, gerr == nil, "presence for %s", key)
		require.Equal(t, want, got, "values for %s", key)
	}

	_, err := sl.get([]byte("absent"))
	require.ErrorIs(t, err, lsmkv.NotFound)

	slFlat := sl.flattenInOrder()
	rbFlat := rb.flattenInOrder()
	require.Len(t, slFlat, len(rbFlat))
	for i := range slFlat {
		require.Equal(t, rbFlat[i].key, slFlat[i].key, "flatten key order at %d", i)
		require.Equal(t, rbFlat[i].values, slFlat[i].values, "flatten values at %d", i)
		if i > 0 {
			require.Negative(t, bytes.Compare(slFlat[i-1].key, slFlat[i].key), "flatten sorted")
		}
	}
}

// Spans multiple value-log chunks and verifies dedup-keeps-last across the chain.
func TestSkipListMapValueLogChunks(t *testing.T) {
	sl := newSkipListMap()
	key := []byte("k")
	const nDocs = 30
	writes := valueChunkSize*10 + 3 // force many chunks
	for i := 0; i < writes; i++ {
		sl.insert(key, MapPair{Key: slDocKey(uint64(i % nDocs)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	got, err := sl.get(key)
	require.NoError(t, err)
	require.Len(t, got, nDocs)
	for id := 0; id < nDocs; id++ {
		// last write for this docID is the largest i with i%nDocs==id
		last := 0
		for i := 0; i < writes; i++ {
			if i%nDocs == id {
				last = i
			}
		}
		require.Equal(t, []byte(fmt.Sprintf("v%d", last)), got[id].Value, "docID %d keeps last write", id)
	}
}

// One writer inserts/appends while many readers get/flatten concurrently. Run
// with -race. Readers must never see a non-sorted/duplicated result and never
// crash; the final state must be exactly correct.
func TestSkipListMapConcurrentRace(t *testing.T) {
	sl := newSkipListMap()
	hot := []byte("hot")
	const nDocs = 40
	const iters = 40000
	sl.insert(hot, MapPair{Key: slDocKey(0), Value: []byte("seed")})

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if vals, err := sl.get(hot); err == nil {
					for i := 1; i < len(vals); i++ {
						if bytes.Compare(vals[i-1].Key, vals[i].Key) >= 0 {
							t.Errorf("get(hot) not strictly sorted/deduped at %d", i)
							return
						}
					}
				}
				flat := sl.flattenInOrder()
				for i := 1; i < len(flat); i++ {
					if bytes.Compare(flat[i-1].key, flat[i].key) >= 0 {
						t.Errorf("flatten not strictly sorted at %d", i)
						return
					}
				}
				_, _ = sl.get([]byte("missing"))
			}
		}()
	}

	for i := 0; i < iters; i++ {
		sl.insert(hot, MapPair{Key: slDocKey(uint64(i % nDocs)), Value: []byte(fmt.Sprintf("v%d", i))})
		if i%4 == 0 {
			sl.insert([]byte(fmt.Sprintf("k%06d", i)), MapPair{Key: slDocKey(uint64(i)), Value: []byte("x")})
		}
	}
	stop.Store(true)
	wg.Wait()

	final, err := sl.get(hot)
	require.NoError(t, err)
	require.Len(t, final, nDocs)
	for i := 1; i < len(final); i++ {
		require.Negative(t, bytes.Compare(final[i-1].Key, final[i].Key))
	}
}
