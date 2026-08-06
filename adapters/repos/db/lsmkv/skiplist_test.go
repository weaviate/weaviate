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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// The generic core works with any value type (here int and []byte): ordered
// iteration, per-key append accumulation, and a raw (un-reduced) value log —
// reduction like dedup is the adapter's job.
func TestSkipListGeneric(t *testing.T) {
	sl := newSkipList[int]()
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k%03d", (i*37)%100)) // 100 distinct keys, scrambled order
		sl.insert(key, i)
	}

	vals, ok := sl.get([]byte("k000"))
	require.True(t, ok)
	require.NotEmpty(t, vals)
	_, ok = sl.get([]byte("absent"))
	require.False(t, ok)

	var keys [][]byte
	sl.forEach(func(key []byte, values []int) { keys = append(keys, key) })
	require.Len(t, keys, 100)
	for i := 1; i < len(keys); i++ {
		require.Negative(t, bytes.Compare(keys[i-1], keys[i]), "forEach ascending")
	}
}

// -race: generic core, one writer + concurrent readers over a []byte value type.
func TestSkipListGenericConcurrentRace(t *testing.T) {
	sl := newSkipList[[]byte]()
	hot := []byte("hot")
	sl.insert(hot, []byte("v0"))

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				sl.get(hot)
				sl.forEach(func(key []byte, values [][]byte) {})
			}
		}()
	}

	const iters = 30000
	for i := 0; i < iters; i++ {
		sl.insert(hot, []byte(fmt.Sprintf("v%d", i)))
		if i%4 == 0 {
			sl.insert([]byte(fmt.Sprintf("k%06d", i)), []byte("x"))
		}
	}
	stop.Store(true)
	wg.Wait()

	vals, ok := sl.get(hot)
	require.True(t, ok)
	require.Len(t, vals, iters+1) // append-only core: every append retained, no dedup
}
