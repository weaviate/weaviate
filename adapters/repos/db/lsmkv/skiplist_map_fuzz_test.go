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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Differential fuzz: the same random op sequence applied to the skip list and the
// red-black tree must leave them byte-identical for every get() and for
// flattenInOrder(). Covers the edge cases the QA review called out — empty row key,
// nil vs empty Value, tombstone<->reinsert both orders, repeated keys, multi-chunk
// value logs, and docID boundaries {0, 0xFFFF, 0x10000, MaxUint64-1, MaxUint64}.
func TestSkipListMapDifferentialFuzz(t *testing.T) {
	rowKeys := [][]byte{
		{}, // empty row key
		[]byte("a"),
		[]byte("term-0"),
		[]byte("term-1"),
		[]byte("zzz"),
	}
	docIDs := []uint64{
		0, 1, 7, 42,
		0xFFFF, 0x10000, // sroar container-band boundary
		1 << 20,
		^uint64(0) - 1, ^uint64(0), // MaxUint64-1, MaxUint64
	}

	for seed := int64(0); seed < 25; seed++ {
		sl := newSkipListMap()
		rb := &binarySearchTreeMap{}
		r := rand.New(rand.NewSource(seed))

		ops := 500 + r.Intn(2500)
		for i := 0; i < ops; i++ {
			rk := rowKeys[r.Intn(len(rowKeys))]
			pair := MapPair{Key: slDocKey(docIDs[r.Intn(len(docIDs))])}
			switch r.Intn(4) {
			case 0:
				pair.Tombstone = true // delete (Value nil)
			case 1:
				pair.Value = nil // nil value
			case 2:
				pair.Value = []byte{} // empty, non-nil value
			default:
				pair.Value = []byte{byte(i), byte(i >> 8)}
			}
			// identical input to both indexes; neither mutates it afterwards
			sl.insert(rk, pair)
			rb.insert(rk, pair)
		}

		probes := append(append([][]byte{}, rowKeys...), []byte("absent"), []byte("term-2"))
		for _, rk := range probes {
			slv, slErr := sl.get(rk)
			rbv, rbErr := rb.get(rk)
			require.Equal(t, rbErr == nil, slErr == nil, "seed=%d presence for %q", seed, rk)
			if slErr != nil {
				require.ErrorIs(t, slErr, lsmkv.NotFound, "seed=%d %q", seed, rk)
				continue
			}
			require.Equal(t, rbv, slv, "seed=%d values for %q", seed, rk)
		}

		slFlat := sl.flattenInOrder()
		rbFlat := rb.flattenInOrder()
		require.Len(t, slFlat, len(rbFlat), "seed=%d flatten length", seed)
		for i := range slFlat {
			require.Equal(t, rbFlat[i].key, slFlat[i].key, "seed=%d flatten key at %d", seed, i)
			require.Equal(t, rbFlat[i].values, slFlat[i].values, "seed=%d flatten values at %d", seed, i)
			if i > 0 {
				require.Negative(t, bytes.Compare(slFlat[i-1].key, slFlat[i].key),
					"seed=%d flatten not strictly ascending at %d", seed, i)
			}
		}
	}
}
