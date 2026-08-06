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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The skip list must return values byte-identical and order-identical to the
// red-black tree: set decoding depends on later tombstones cancelling earlier
// values, so insertion order is part of the contract.
func TestSkipListMultiMatchesRBTree(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	rb := multiIndex(&binarySearchTreeMulti{})
	sl := multiIndex(newSkipListMulti())

	keySpace := 100
	for i := 0; i < 3000; i++ {
		key := []byte(fmt.Sprintf("key-%03d", rng.Intn(keySpace)))
		values := make([]value, rng.Intn(4)) // sometimes empty, which must still create the key
		for j := range values {
			v := make([]byte, 1+rng.Intn(10))
			rng.Read(v)
			values[j] = value{value: v, tombstone: rng.Intn(5) == 0}
		}
		rb.insert(key, values)
		sl.insert(key, values)
	}

	for i := 0; i < keySpace; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		v1, err1 := rb.get(key)
		v2, err2 := sl.get(key)
		require.Equal(t, err1, err2, "get(%s)", key)
		require.Equal(t, v1, v2, "get(%s)", key)
	}

	flat1, flat2 := rb.flattenInOrder(), sl.flattenInOrder()
	require.Equal(t, len(flat1), len(flat2))
	for i := range flat1 {
		require.Equal(t, flat1[i].key, flat2[i].key)
		require.Equal(t, flat1[i].values, flat2[i].values)
	}
}

// Lock-free readers must always see a consistent prefix of each key's values in
// insertion order, never a torn or reordered log. Run under -race.
func TestSkipListMultiConcurrentReadWrite(t *testing.T) {
	const (
		keys           = 32
		writes         = 20000
		readers        = 4
		readsPerReader = 5000
	)
	sl := newSkipListMulti()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		seqs := make([]uint32, keys)
		for i := 0; i < writes; i++ {
			k := rng.Intn(keys)
			v := make([]byte, 8)
			binary.BigEndian.PutUint32(v[:4], uint32(k))
			binary.BigEndian.PutUint32(v[4:], seqs[k])
			seqs[k]++
			sl.insert([]byte(fmt.Sprintf("key-%04d", k)), []value{{value: v}})
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < readsPerReader; i++ {
				k := rng.Intn(keys)
				vs, err := sl.get([]byte(fmt.Sprintf("key-%04d", k)))
				if err != nil {
					continue // not yet written
				}
				// values must be exactly seq 0..n-1 for this key: any gap,
				// reorder or foreign payload means a torn read
				for want, v := range vs {
					if got := int(binary.BigEndian.Uint32(v.value[:4])); got != k {
						t.Errorf("key %d holds payload of key %d", k, got)
						return
					}
					if got := int(binary.BigEndian.Uint32(v.value[4:])); got != want {
						t.Errorf("key %d: value %d has seq %d", k, want, got)
						return
					}
				}
			}
		}(int64(r))
	}

	wg.Wait()
}
