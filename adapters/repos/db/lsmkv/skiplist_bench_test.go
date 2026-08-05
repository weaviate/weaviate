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
	"fmt"
	"math/rand"
	"testing"
)

// Inverted-index write shape: zipfian term keys, one MapPair posting per call.
// ~99% of distinct terms take a single posting, so per-new-key cost dominates.
func benchKeys(n, vocab int, seed int64) ([][]byte, []MapPair) {
	r := rand.New(rand.NewSource(seed))
	z := rand.NewZipf(r, 1.07, 1.0, uint64(vocab-1))
	terms := make([][]byte, vocab)
	for i := range terms {
		terms[i] = []byte(fmt.Sprintf("term_%06d", i))
	}
	keys := make([][]byte, n)
	pairs := make([]MapPair, n)
	for i := 0; i < n; i++ {
		keys[i] = terms[z.Uint64()]
		docID := make([]byte, 8)
		for j := range docID {
			docID[j] = byte(r.Intn(256))
		}
		pairs[i] = MapPair{Key: docID, Value: []byte{byte(i), byte(i >> 8)}}
	}
	return keys, pairs
}

func benchInsert(b *testing.B, mk func() mapIndex, n, vocab int) {
	keys, pairs := benchKeys(n, vocab, 7)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := mk()
		for j := 0; j < n; j++ {
			idx.insert(keys[j], pairs[j])
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(n*b.N)/b.Elapsed().Seconds()/1e6, "Minsert/s")
}

// vocab == n makes nearly every insert a new key (worst case for node
// allocation); vocab < n mixes in repeat postings as real text does.
func BenchmarkMapIndexInsert(b *testing.B) {
	for _, tc := range []struct {
		name  string
		n     int
		vocab int
	}{
		{"mostly_new", 200000, 200000},
		{"mixed", 200000, 100000},
		{"hot_terms", 200000, 20000},
	} {
		b.Run(tc.name+"/rbtree", func(b *testing.B) {
			benchInsert(b, func() mapIndex { return &binarySearchTreeMap{} }, tc.n, tc.vocab)
		})
		b.Run(tc.name+"/skiplist", func(b *testing.B) {
			benchInsert(b, func() mapIndex { return newSkipListMap() }, tc.n, tc.vocab)
		})
	}
}

// scalingKeys returns n distinct 12-byte keys in scrambled order, so nodes are
// allocated in an order unrelated to key order — the cache-hostile case.
func scalingKeys(n int, seed int64) [][]byte {
	r := rand.New(rand.NewSource(seed))
	keys := make([][]byte, n)
	perm := r.Perm(n)
	for i := 0; i < n; i++ {
		k := make([]byte, 12)
		binary.BigEndian.PutUint64(k, uint64(perm[i])*2654435761)
		copy(k[8:], []byte("trm"))
		keys[i] = k
	}
	return keys
}

func prefill(idx mapIndex, keys [][]byte) {
	for i, k := range keys {
		idx.insert(k, MapPair{Key: k[:8], Value: []byte{byte(i), byte(i >> 8)}})
	}
}

// If descent cost is dominated by cache misses, the skip list's disadvantage
// against the red-black tree widens as the index grows past cache. A flat ratio
// would mean the cost is per-operation work instead.
func BenchmarkIndexScaling(b *testing.B) {
	for _, n := range []int{50_000, 200_000, 800_000} {
		keys := scalingKeys(n, 11)
		fresh := scalingKeys(n/10, 99)
		for _, impl := range []struct {
			name string
			mk   func() mapIndex
		}{
			{"rbtree", func() mapIndex { return &binarySearchTreeMap{} }},
			{"skiplist", func() mapIndex { return newSkipListMap() }},
		} {
			b.Run(fmt.Sprintf("n=%d/insert/%s", n, impl.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					idx := impl.mk()
					prefill(idx, keys)
					b.StartTimer()
					for j, k := range fresh {
						idx.insert(k, MapPair{Key: k[:8], Value: []byte{byte(j)}})
					}
				}
				b.ReportMetric(float64(len(fresh)*b.N)/b.Elapsed().Seconds()/1e6, "Minsert/s")
			})
			b.Run(fmt.Sprintf("n=%d/get/%s", n, impl.name), func(b *testing.B) {
				idx := impl.mk()
				prefill(idx, keys)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, k := range keys[:len(keys)/10] {
						idx.get(k)
					}
				}
				b.ReportMetric(float64(len(keys)/10*b.N)/b.Elapsed().Seconds()/1e6, "Mget/s")
			})
		}
	}
}
