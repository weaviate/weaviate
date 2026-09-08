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
	"encoding/binary"
	"runtime"
	"testing"
)

// BM25 vocabularies are ~99% single-posting (hapax) terms, so the skip-list index
// must not blow up the per-key footprint on singletons. A fixed 16-slot value chunk
// once cost a full ~1KB size class per key (~6.8x the red-black tree); the growable
// first chunk brings it back near parity. This is a coarse guard with a wide margin
// to tolerate GC/allocator noise while still catching a regression to the old shape.
func TestSkipListMapSingletonMemoryVsRBTree(t *testing.T) {
	const (
		k        = 100_000
		maxRatio = 3.0 // measured ~1.7x; a fixed-16 chunk would blow well past this
	)

	bytesPerKey := func(makeIdx func() mapIndex) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)

		idx := makeIdx()
		for i := 0; i < k; i++ {
			rowKey := make([]byte, 8)
			binary.BigEndian.PutUint64(rowKey, uint64(i))
			idx.insert(rowKey, MapPair{Key: rowKey, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}})
		}

		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(idx)
		return (after.HeapAlloc - before.HeapAlloc) / k
	}

	sl := bytesPerKey(func() mapIndex { return newSkipListMap() })
	rb := bytesPerKey(func() mapIndex { return &binarySearchTreeMap{} })
	ratio := float64(sl) / float64(rb)
	t.Logf("singleton bytes/key: skiplist=%d rbtree=%d ratio=%.2fx", sl, rb, ratio)

	if ratio > maxRatio {
		t.Fatalf("skip-list singleton footprint regressed: %.2fx the red-black tree "+
			"(skiplist=%d B/key, rbtree=%d B/key, max allowed %.1fx) — check valueChunk sizing",
			ratio, sl, rb, maxRatio)
	}
}

// get() on the skip-list map must stay at a single, right-sized allocation
// regardless of posting-list length (the un-pre-sized snapshot used to realloc
// across value-chunk boundaries, growing allocs/op with posting size).
func TestSkipListMapGetSingleAlloc(t *testing.T) {
	for _, postings := range []int{1, 16, 64, 256} {
		m := newSkipListMap()
		term := []byte("term")
		for i := 0; i < postings; i++ {
			docKey := make([]byte, 8)
			binary.BigEndian.PutUint64(docKey, uint64(i))
			m.insert(term, MapPair{Key: docKey, Value: []byte("vv")})
		}
		allocs := testing.AllocsPerRun(100, func() { _, _ = m.get(term) })
		if allocs > 1 {
			t.Errorf("get() with %d postings did %.0f allocs/op, want <= 1", postings, allocs)
		}
	}
}
