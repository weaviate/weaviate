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

package hashtree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestPayloadCompressionReduction measures how much of the 16.8x raw-byte savings
// from the level-local encoding survives zstd compression (the same compressor used
// in adapters/clients/replication.go HashTreeLevel).
//
// Three discriminant patterns are tested for each of two heights:
//
//	AllSet    – every node selected; this is the first hashbeat after startup
//	           or after a large divergence.  Highly compressible (runs of 0xFF).
//	Sparse1pc – 1% of nodes selected; typical steady-state with few diffs.
//	           Mostly-zero words, good zstd run-length compression.
//	Random50  – 50% fill, independent uniform bits; worst case for compression
//	           (near-maximum entropy, compressor gains nothing).
func TestPayloadCompressionReduction(t *testing.T) {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer enc.Close()

	compress := func(b []byte) []byte {
		return enc.EncodeAll(b, make([]byte, 0, len(b)))
	}

	type pattern struct {
		name    string
		fill    func(bset *Bitset)
		comment string
	}

	heights := []int{10, 16}

	for _, height := range heights {
		t.Run(fmt.Sprintf("height%d", height), func(t *testing.T) {
			nodesCount := NodesCount(height)
			fullDisc := NewBitset(nodesCount)

			patterns := []pattern{
				{
					name:    "AllSet",
					fill:    func(bset *Bitset) { bset.SetAll() },
					comment: "first hashbeat / full divergence — runs of 0xFF, excellent compression",
				},
				{
					name: "Sparse1pct",
					fill: func(bset *Bitset) {
						rng := rand.New(rand.NewSource(42))
						target := nodesCount / 100
						for i := 0; i < target; i++ {
							bset.Set(rng.Intn(nodesCount))
						}
					},
					comment: "steady state with ~1% nodes differing — typical async replication",
				},
				{
					name: "Random50pct",
					fill: func(bset *Bitset) {
						rng := rand.New(rand.NewSource(42))
						for i := 0; i < nodesCount; i++ {
							if rng.Intn(2) == 0 {
								bset.Set(i)
							}
						}
					},
					comment: "50% random fill — worst case for compression (max entropy)",
				},
			}

			for _, p := range patterns {
				t.Run(p.name, func(t *testing.T) {
					fullDisc.Reset()
					p.fill(fullDisc)

					rawFull, err := fullDisc.Marshal()
					require.NoError(t, err)
					compFull := compress(rawFull)

					var totalRawFull, totalRawLocal int
					var totalCompFull, totalCompLocal int

					t.Logf("  %-12s  height=%d  (%s)", p.name, height, p.comment)
					t.Logf("  %5s  %8s  %8s  %8s  %8s  %8s  %8s",
						"level", "rawFull", "rawLocal", "cmpFull", "cmpLocal", "rawRed", "cmpRed")

					for level := 0; level <= height; level++ {
						localDisc := fullDisc.ExtractSlice(InnerNodesCount(level), LeavesCount(level))
						rawLocal, err := localDisc.Marshal()
						require.NoError(t, err)
						compLocal := compress(rawLocal)

						totalRawFull += len(rawFull)
						totalRawLocal += len(rawLocal)
						totalCompFull += len(compFull)
						totalCompLocal += len(compLocal)

						rawRed := float64(len(rawFull)) / float64(len(rawLocal))
						cmpRed := float64(len(compFull)) / float64(len(compLocal))

						t.Logf("  %5d  %8d  %8d  %8d  %8d  %7.1fx  %7.1fx",
							level,
							len(rawFull), len(rawLocal),
							len(compFull), len(compLocal),
							rawRed, cmpRed)
					}

					totalRawRed := float64(totalRawFull) / float64(totalRawLocal)
					totalCmpRed := float64(totalCompFull) / float64(totalCompLocal)

					t.Logf("  TOTAL (traversal)  raw: %dB→%dB (%.1fx)  compressed: %dB→%dB (%.1fx)",
						totalRawFull, totalRawLocal, totalRawRed,
						totalCompFull, totalCompLocal, totalCmpRed)
					t.Logf("")

					require.Less(t, totalRawLocal, totalRawFull,
						"level-local raw bytes must be strictly smaller than full discriminant bytes across a traversal")
				})
			}
		})
	}
}
