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

package bloomhash

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"
)

const maxKeyLen = 64

func randomKey(r *rand.Rand, n int) []byte {
	k := make([]byte, n)
	r.Read(k)
	return k
}

// A bloom upgrade that changes its hashing or bit locations fails this test.
func TestMatchesLibrary(t *testing.T) {
	tests := []struct {
		name   string
		filter func() *bloom.BloomFilter
	}{
		{"m=1 k=1", func() *bloom.BloomFilter { return bloom.New(1, 1) }},
		{"m=997 k=3", func() *bloom.BloomFilter { return bloom.New(997, 3) }},
		{"m=4096 k=7", func() *bloom.BloomFilter { return bloom.New(4096, 7) }},
		{"estimates n=100 p=0.001", func() *bloom.BloomFilter { return bloom.NewWithEstimates(100, 0.001) }},
		{"estimates n=10000 p=0.001", func() *bloom.BloomFilter { return bloom.NewWithEstimates(10000, 0.001) }},
		{"estimates n=1000 p=0.3", func() *bloom.BloomFilter { return bloom.NewWithEstimates(1000, 0.3) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(1))
			f := tt.filter()
			var added, absent [][]byte
			for n := 0; n <= maxKeyLen; n++ {
				for range 8 {
					added = append(added, randomKey(r, n))
					absent = append(absent, randomKey(r, n))
				}
			}
			for _, k := range added {
				f.Add(k)
			}

			for _, k := range append(added, absent...) {
				require.Equal(t, f.Test(k), Test(f, Sum(k)), "key of length %d: %x", len(k), k)
			}
		})
	}
}

// BenchmarkTestManyFilters probes each key against filters whose combined size
// is far past the CPU caches, as a read does across a bucket's segments.
func BenchmarkTestManyFilters(b *testing.B) {
	const (
		filters      = 32
		keysPerFill  = 1 << 20
		probes       = 100_000
		presentEvery = 6
	)
	r := rand.New(rand.NewSource(1))
	probeKeys := make([][]byte, probes)
	for i := range probeKeys {
		probeKeys[i] = randomKey(r, 4+r.Intn(29))
	}
	fs := make([]*bloom.BloomFilter, filters)
	key := make([]byte, 16)
	for i := range fs {
		fs[i] = bloom.NewWithEstimates(keysPerFill, 0.001)
		for range keysPerFill {
			r.Read(key)
			fs[i].Add(key)
		}
		for j := 0; j < probes; j += presentEvery {
			fs[i].Add(probeKeys[j])
		}
	}

	var hits int
	b.Run(fmt.Sprintf("by-key/filters=%d", filters), func(b *testing.B) {
		for b.Loop() {
			for _, k := range probeKeys {
				for _, f := range fs {
					if f.Test(k) {
						hits++
					}
				}
			}
		}
	})
	b.Run(fmt.Sprintf("sum-once/filters=%d", filters), func(b *testing.B) {
		for b.Loop() {
			for _, k := range probeKeys {
				h := Sum(k)
				for _, f := range fs {
					if Test(f, h) {
						hits++
					}
				}
			}
		}
	})
	if hits == 0 {
		b.Fatal("no probe key hit any filter")
	}
}
