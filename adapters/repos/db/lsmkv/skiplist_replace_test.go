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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The skip list must be observationally identical to the red-black tree for
// every replaceIndex operation, including the return values the memtable's
// size/count accounting depends on.
func TestSkipListReplaceMatchesRBTree(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	rb := replaceIndex(&binarySearchTree{})
	sl := replaceIndex(newSkipListReplace())

	keySpace := 200
	for i := 0; i < 5000; i++ {
		key := []byte(fmt.Sprintf("key-%03d", rng.Intn(keySpace)))
		switch rng.Intn(3) {
		case 0, 1:
			val := make([]byte, rng.Intn(20))
			rng.Read(val)
			secondary := [][]byte{[]byte(fmt.Sprintf("sec-%d", rng.Intn(keySpace)))}
			net1, prev1, live1 := rb.insert(key, val, secondary)
			net2, prev2, live2 := sl.insert(key, val, secondary)
			require.Equal(t, net1, net2)
			require.Equal(t, prev1, prev2)
			require.Equal(t, live1, live2)
		case 2:
			prev1, dead1 := rb.setTombstone(key, nil, nil)
			prev2, dead2 := sl.setTombstone(key, nil, nil)
			require.Equal(t, prev1, prev2)
			require.Equal(t, dead1, dead2)
		}
	}

	for i := 0; i < keySpace; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))

		v1, err1 := rb.get(key)
		v2, err2 := sl.get(key)
		require.Equal(t, err1, err2, "get(%s)", key)
		require.Equal(t, v1, v2, "get(%s)", key)

		require.Equal(t, rb.exists(key), sl.exists(key), "exists(%s)", key)

		n1, gerr1 := rb.getNode(key)
		n2, gerr2 := sl.getNode(key)
		require.Equal(t, gerr1, gerr2, "getNode(%s)", key)
		if gerr1 == nil {
			require.Equal(t, n1.key, n2.key)
			require.Equal(t, n1.value, n2.value)
			require.Equal(t, n1.secondaryKeys, n2.secondaryKeys)
			require.Equal(t, n1.tombstone, n2.tombstone)
		}
	}

	flat1, flat2 := rb.flattenInOrder(), sl.flattenInOrder()
	require.Equal(t, len(flat1), len(flat2))
	for i := range flat1 {
		require.Equal(t, flat1[i].key, flat2[i].key)
		require.Equal(t, flat1[i].value, flat2[i].value)
		require.Equal(t, flat1[i].secondaryKeys, flat2[i].secondaryKeys)
		require.Equal(t, flat1[i].tombstone, flat2[i].tombstone)
	}

	stats1, stats2 := rb.countStats(), sl.countStats()
	assert.ElementsMatch(t, stats1.upsertKeys, stats2.upsertKeys)
	assert.ElementsMatch(t, stats1.tombstonedKeys, stats2.tombstonedKeys)
}

// Lock-free readers must never observe a torn entry: value, secondaryKeys and
// tombstone are swapped atomically as one pointer, so a value read under a
// concurrent writer must always be internally consistent. Run under -race.
func TestSkipListReplaceConcurrentReadWrite(t *testing.T) {
	const (
		keys           = 64
		writes         = 20000
		readers        = 4
		readsPerReader = 20000
	)
	sl := newSkipListReplace()

	// seed every key so readers always find something
	for k := 0; k < keys; k++ {
		sl.insert(keyForSeq(k, 0), valueForSeq(k, 0), nil)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		for i := 1; i <= writes; i++ {
			k := rng.Intn(keys)
			if rng.Intn(8) == 0 {
				sl.setTombstone(keyForSeq(k, 0), nil, nil)
			} else {
				sl.insert(keyForSeq(k, 0), valueForSeq(k, i), nil)
			}
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < readsPerReader; i++ {
				k := rng.Intn(keys)
				v, err := sl.get(keyForSeq(k, 0))
				if err != nil {
					continue // tombstoned or (never here) missing
				}
				// a torn read would pair another key's payload with this key
				if got := int(binary.BigEndian.Uint32(v[:4])); got != k {
					t.Errorf("torn value: key %d carries payload of key %d", k, got)
					return
				}
			}
		}(int64(r))
	}

	wg.Wait()
}

func keyForSeq(k, _ int) []byte {
	return []byte(fmt.Sprintf("key-%04d", k))
}

// valueForSeq encodes the key in the value so readers can detect torn entries
func valueForSeq(k, seq int) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint32(v[:4], uint32(k))
	binary.BigEndian.PutUint32(v[4:], uint32(seq))
	return v
}
