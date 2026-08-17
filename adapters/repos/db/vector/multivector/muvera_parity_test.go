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

package multivector

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// referenceSimHash is the pre-SIMD scalar simHash, kept as the semantic
// reference for the parity test.
func referenceSimHash(e *MuveraEncoder, vec []float32, gaussians [][]float32) uint64 {
	var result uint64
	dist := distancer.NewDotProductProvider().New(vec)

	for i := 0; i < e.config.KSim; i++ {
		dotProduct, err := dist.Distance(gaussians[i])
		if err != nil {
			return 0.0
		}
		if dotProduct < 0 {
			result |= 1 << uint(i)
		}
	}
	return result
}

// referenceEncode is the pre-SIMD scalar encode, kept as the semantic
// reference for the parity test.
func referenceEncode(e *MuveraEncoder, fullVec [][]float32, isDoc bool) []float32 {
	encodedVec := make([]float32, e.config.Repetitions*e.config.NumClusters*e.config.DProjections)

	tmpVec := make([]float32, e.config.NumClusters*e.config.Dimensions)
	for rep := 0; rep < e.config.Repetitions; rep++ {
		repetitionClusterCounts := make([]uint16, e.config.NumClusters)
		clusterMappings := make([]uint64, len(fullVec))
		for relative, token := range fullVec {
			cluster := referenceSimHash(e, token, e.gaussians[rep])
			clusterMappings[relative] = cluster
			repetitionClusterCounts[cluster]++
			startIdx := cluster * uint64(e.config.Dimensions)
			for i := 0; i < e.config.Dimensions; i++ {
				tmpVec[startIdx+uint64(i)] += token[i]
			}
		}

		if isDoc {
			for cluster, count := range repetitionClusterCounts {
				startIdx := uint64(cluster) * uint64(e.config.Dimensions)
				for i := 0; i < e.config.Dimensions; i++ {
					tmpVec[startIdx+uint64(i)] = (1 / float32(count)) * tmpVec[startIdx+uint64(i)]
				}
			}
			for cluster := uint64(0); cluster < uint64(e.config.NumClusters); cluster++ {
				if repetitionClusterCounts[cluster] == 0 {
					minHamming := float32(math.MaxFloat32)
					nearestPoint := uint64(0)
					for docIdx, clusterMapped := range clusterMappings {
						hamming, err := distancer.HammingBitwise([]uint64{cluster}, []uint64{clusterMapped})
						if err != nil {
							return nil
						}
						if hamming < minHamming {
							minHamming = hamming
							nearestPoint = uint64(docIdx)
						}
					}
					startIdx := cluster * uint64(e.config.Dimensions)
					for i := 0; i < e.config.Dimensions; i++ {
						tmpVec[startIdx+uint64(i)] = fullVec[nearestPoint][i]
					}
				}
			}
		}

		scale := 1.0 / float32(math.Sqrt(float64(e.config.DProjections)))
		projOffset := rep * e.config.NumClusters * e.config.DProjections
		matrix := e.S[rep]
		for j := 0; j < e.config.NumClusters; j++ {
			srcStart := j * e.config.Dimensions
			dstStart := projOffset + (j * e.config.DProjections)
			for k := 0; k < e.config.DProjections; k++ {
				var sum float32
				for m := 0; m < e.config.Dimensions; m++ {
					sum += matrix[k][m] * tmpVec[srcStart+m]
				}
				encodedVec[dstStart+k] = sum * scale
			}
		}

		clear(tmpVec)
	}

	return encodedVec
}

func TestMuveraEncodeMatchesReference(t *testing.T) {
	cases := []struct {
		name         string
		ksim         int
		dProjections int
		repetitions  int
		dims         int
		numTokens    int
	}{
		// defaults, ColBERT-ish doc and query shapes
		{"defaults-doc-shape", 4, 16, 10, 128, 100},
		{"defaults-query-shape", 4, 16, 10, 128, 32},
		// single token: every repetition has empty clusters to backfill
		{"single-token", 4, 16, 10, 128, 1},
		{"few-tokens", 4, 16, 10, 128, 3},
		// non-default shapes, incl. dims below the SIMD batch kernel's
		// 64-dim eligibility gate and non-multiple-of-16 dims
		{"small-dims", 3, 8, 5, 48, 20},
		{"odd-dims", 3, 8, 5, 96, 20},
		{"unaligned-dims", 4, 16, 5, 125, 20},
		{"large-ksim", 6, 16, 5, 128, 20},
		{"dproj-not-pow2", 4, 12, 5, 128, 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(tc.dims), uint64(tc.numTokens)))
			encoder := NewMuveraEncoder(ent.MuveraConfig{
				KSim:         tc.ksim,
				DProjections: tc.dProjections,
				Repetitions:  tc.repetitions,
			}, nil)
			encoder.InitEncoder(tc.dims)
			tokens := randomTokens(rng, tc.numTokens, tc.dims)

			for _, isDoc := range []bool{true, false} {
				want := referenceEncode(encoder, tokens, isDoc)
				var got []float32
				if isDoc {
					got = encoder.EncodeDoc(tokens)
				} else {
					got = encoder.EncodeQuery(tokens)
				}
				require.Equal(t, len(want), len(got))
				for i := range want {
					require.InDeltaf(t, want[i], got[i], 1e-3,
						"isDoc=%v index %d: want %v got %v", isDoc, i, want[i], got[i])
				}
			}
		})
	}
}

// The flattened matrices must survive a persist/load round trip (LoadMuveraConfig
// is the path used on startup restore).
func TestMuveraLoadConfigRebuildsFlatMatrices(t *testing.T) {
	encoder := defaultTestEncoder(128)
	rng := rand.New(rand.NewPCG(7, 8))
	tokens := randomTokens(rng, 10, 128)
	want := encoder.EncodeDoc(tokens)

	loaded := NewMuveraEncoder(ent.MuveraConfig{}, nil)
	loaded.LoadMuveraConfig(MuveraData{
		KSim:         uint32(encoder.config.KSim),
		NumClusters:  uint32(encoder.config.NumClusters),
		Dimensions:   uint32(encoder.config.Dimensions),
		DProjections: uint32(encoder.config.DProjections),
		Repetitions:  uint32(encoder.config.Repetitions),
		Gaussians:    encoder.gaussians,
		S:            encoder.S,
	})
	got := loaded.EncodeDoc(tokens)
	require.Equal(t, want, got)
}

func TestMuveraEncodeEmptyDoc(t *testing.T) {
	encoder := defaultTestEncoder(128)
	require.Nil(t, encoder.EncodeDoc([][]float32{}))
	require.Nil(t, encoder.EncodeQuery(nil))
}

func TestMuveraEncodeDeterministic(t *testing.T) {
	encoder := defaultTestEncoder(128)
	rng := rand.New(rand.NewPCG(1, 2))
	tokens := randomTokens(rng, 25, 128)
	a := encoder.EncodeDoc(tokens)
	b := encoder.EncodeDoc(tokens)
	require.Equal(t, a, b)
}
