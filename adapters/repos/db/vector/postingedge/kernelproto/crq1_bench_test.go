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

package kernelproto

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

var (
	sinkFloat float32
	sinkInt   int
)

// benchWidths are the code widths of wiki-dpr (768) and dbpedia (1536).
var benchWidths = []int{768, 1536}

// fixture is a real CRQ1 quantizer and distancer of one width, with a
// prototype query of the same width. The two queries differ, which changes
// nothing of what a distance costs.
type fixture struct {
	dims      int
	quantizer *compressionhelpers.CenteredBinaryRotationalQuantizer
	current   *compressionhelpers.CenteredBinaryRQDistancer
	query     *Query
	header    uint64 // the header word of a real code
}

func newFixture(b *testing.B, dims int) *fixture {
	b.Helper()
	rng := rand.New(rand.NewPCG(uint64(dims), 9))
	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(dims,
		compressionhelpers.DefaultFastRotationSeed, make([]float32, dims), distancer.NewCosineDistanceProvider())
	require.NoError(b, err)
	vector := func() []float32 {
		v := make([]float32, dims)
		for i := range v {
			v[i] = float32(rng.NormFloat64())
		}
		return distancer.Normalize(v)
	}
	return &fixture{
		dims:      dims,
		quantizer: quantizer,
		current:   quantizer.NewDistancer(vector()),
		query:     randomQuery(b, rng, dims),
		header:    quantizer.Encode(vector())[0],
	}
}

// codes are n codes of random bits behind the header of a real code, in one
// array as a store keeps them, words+1 each.
func (f *fixture) codes(n int) []uint64 {
	words := f.dims/64 + 1
	rng := rand.New(rand.NewPCG(uint64(n), uint64(f.dims)))
	out := make([]uint64, n*words)
	for i := range out {
		out[i] = rng.Uint64()
		if i%words == 0 {
			out[i] = f.header
		}
	}
	return out
}

// BenchmarkKernel is one code that sits in the L1 cache, scored again and
// again: the arithmetic of a distance with no memory in the way.
func BenchmarkKernel(b *testing.B) {
	for _, dims := range benchWidths {
		f := newFixture(b, dims)
		words := dims/64 + 1
		const n = 64
		codes := f.codes(n)
		code := func(i int) []uint64 { return codes[(i%n)*words : (i%n+1)*words] }

		b.Run(fmt.Sprintf("dims=%d/current distance", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				d, _ := f.current.Distance(code(i))
				sinkFloat += d
			}
		})
		b.Run(fmt.Sprintf("dims=%d/current five calls", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkInt += fiveCalls(f.query, code(i)[1:])
			}
		})
		b.Run(fmt.Sprintf("dims=%d/fused go", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkInt += FusedGo(f.query, code(i)[1:])
			}
		})
		b.Run(fmt.Sprintf("dims=%d/fused neon", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkInt += Fused(f.query, code(i)[1:])
			}
		})
		b.Run(fmt.Sprintf("dims=%d/prototype distance", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sinkFloat += f.query.Distance(code(i))
			}
		})
	}
}

// BenchmarkPostingScan scores the members of one posting of 236 codes, the
// default size, laid out one after another as the posting store keeps them:
// what the scan of a loaded posting costs per member, without its top-k.
// FastScan reads the same codes laid out in blocks of 32.
func BenchmarkPostingScan(b *testing.B) {
	const members = 236
	for _, dims := range benchWidths {
		f := newFixture(b, dims)
		words := dims/64 + 1
		codes := f.codes(members)
		var blocks []*Block
		for first := 0; first < members; first += BlockCodes {
			var block [][]uint64
			for c := first; c < min(first+BlockCodes, members); c++ {
				block = append(block, codes[c*words:(c+1)*words])
			}
			bl, err := NewBlock(block)
			require.NoError(b, err)
			blocks = append(blocks, bl)
		}
		out := make([]float32, members+BlockCodes)

		run := func(name string, scan func()) {
			b.Run(fmt.Sprintf("dims=%d/%s", dims, name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					scan()
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/members, "ns/code")
			})
		}
		run("current distance", func() {
			for c := 0; c < members; c++ {
				out[c], _ = f.current.Distance(codes[c*words : (c+1)*words])
			}
		})
		run("prototype distance", func() {
			for c := 0; c < members; c++ {
				out[c] = f.query.Distance(codes[c*words : (c+1)*words])
			}
		})
		run("fastscan", func() {
			for i, block := range blocks {
				f.query.BlockDistances(block, out[i*BlockCodes:])
			}
		})
	}
}

// BenchmarkScattered scores codes at random places of a sidecar of 1 GiB,
// fetching 8 codes ahead the way the sidecar of the index does: what an edge
// endpoint costs, the code coming from memory and not from the caches.
func BenchmarkScattered(b *testing.B) {
	const sidecarBytes, batch, ahead = 1 << 30, 1024, 8
	for _, dims := range benchWidths {
		f := newFixture(b, dims)
		words := dims/64 + 1
		n := sidecarBytes / (8 * words)
		codes := f.codes(n)
		rng := rand.New(rand.NewPCG(7, uint64(dims)))
		slots := make([]uint32, batch)
		dists := make([]float32, batch)
		prefetch := func(slot uint32) {
			addr := uintptr(unsafe.Pointer(&codes[int(slot)*words]))
			asm.PrefetchN(addr, asm.PrefetchSpan(addr, words*8))
		}

		run := func(name string, score func(code []uint64) float32) {
			b.Run(fmt.Sprintf("dims=%d/%s", dims, name), func(b *testing.B) {
				b.StopTimer()
				for done := 0; done < b.N; done += batch {
					for i := range slots {
						slots[i] = uint32(rng.IntN(n))
					}
					b.StartTimer()
					for i := range min(ahead, batch) {
						prefetch(slots[i])
					}
					for i, slot := range slots {
						if next := i + ahead; next < batch {
							prefetch(slots[next])
						}
						dists[i] = score(codes[int(slot)*words : (int(slot)+1)*words])
					}
					b.StopTimer()
				}
			})
		}
		run("current distance", func(code []uint64) float32 {
			d, _ := f.current.Distance(code)
			return d
		})
		run("prototype distance", f.query.Distance)
	}
}

// BenchmarkQueryLayout is what laying out one query costs: NewDistancer of
// the index (which rotates the query) and the planes, chunks and tables of
// the prototypes on top.
func BenchmarkQueryLayout(b *testing.B) {
	for _, dims := range benchWidths {
		f := newFixture(b, dims)
		rng := rand.New(rand.NewPCG(11, uint64(dims)))
		values := make([]uint8, dims)
		for i := range values {
			values[i] = uint8(rng.IntN(maxValue + 1))
		}
		vector := make([]float32, dims)
		for i := range vector {
			vector[i] = float32(rng.NormFloat64())
		}
		b.Run(fmt.Sprintf("dims=%d/current NewDistancer", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = f.quantizer.NewDistancer(vector)
			}
		})
		b.Run(fmt.Sprintf("dims=%d/prototype layout", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q, err := NewQuery(values, 1, 1, 0, true)
				if err != nil {
					b.Fatal(err)
				}
				sinkInt += q.Words
			}
		})
	}
}
