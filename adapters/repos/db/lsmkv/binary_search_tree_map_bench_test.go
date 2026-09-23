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
	"testing"
)

// mapPairsForDocIDs builds one MapPair per doc ID with the given key encoding.
// The value carries the write's position, so a caller can tell which of two
// writes to the same doc ID survived the dedup.
func mapPairsForDocIDs(docIDs []uint64, bigEndian bool) []MapPair {
	out := make([]MapPair, len(docIDs))
	for i, id := range docIDs {
		k := make([]byte, 8)
		if bigEndian {
			binary.BigEndian.PutUint64(k, id)
		} else {
			binary.LittleEndian.PutUint64(k, id)
		}
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(i))
		out[i] = MapPair{Key: k, Value: v}
	}
	return out
}

// BenchmarkSortAndDedupValues contrasts the two doc-ID key encodings that reach
// this function as MapPair keys. BigEndian doc IDs arrive ascending and skip the sort, while
// the LittleEndian ones the dimensions bucket writes under
// StrategyMapCollection are byte-wise scrambled and pay the full stable sort.
func BenchmarkSortAndDedupValues(b *testing.B) {
	for _, n := range []int{1_000, 200_000} {
		for _, enc := range []struct {
			name      string
			bigEndian bool
		}{{"big-endian-docids", true}, {"little-endian-docids", false}} {
			docIDs := make([]uint64, n)
			for i := range docIDs {
				docIDs[i] = uint64(i)
			}
			in := mapPairsForDocIDs(docIDs, enc.bigEndian)

			b.Run(fmt.Sprintf("n=%d/%s", n, enc.name), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sortAndDedupValues(in)
				}
			})
		}
	}
}
