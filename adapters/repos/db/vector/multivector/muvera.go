//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multivector

import (
	"math"
	"math/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type MuveraConfig struct {
	KSim         int
	NumClusters  int // Number of clusters for K-means or number of bits for SimHash
	Dimensions   int // Dimensions of each vector (128 in the Python implementation)
	DProjections int // Number of projections for D-Projections
	DFinal       int // Number of projections for final projection
	Repetitions  int // Number of repetitions (20 in the Python implementation)
}

type MuveraEncoder struct {
	config               MuveraConfig
	gaussians            [][][]float32 // Random Gaussian vectors for SimHash projection
	S                    [][][]float32 // Random projection matrix with ±1 entries
	Sfinal               [][]float32   // Random projection matrix with ±1 entries
	dotDistancerProvider distancer.Provider
}

func NewMuveraEncoder(config ent.MuveraConfig) *MuveraEncoder {
	encoder := &MuveraEncoder{
		config: MuveraConfig{
			KSim:         config.KSim,
			NumClusters:  int(math.Pow(2, float64(config.KSim))),
			DProjections: config.DProjections,
			Repetitions:  config.Repetitions,
		},
		dotDistancerProvider: distancer.NewDotProductProvider(),
	}

	return encoder
}

func (encoder *MuveraEncoder) InitEncoder(dimensions int) {
	encoder.config.Dimensions = dimensions
	encoder.gaussians = make([][][]float32, encoder.config.Repetitions)
	encoder.S = make([][][]float32, encoder.config.Repetitions)
	for rep := 0; rep < encoder.config.Repetitions; rep++ {
		// Initialize random Gaussian vectors
		encoder.gaussians[rep] = make([][]float32, encoder.config.KSim)
		for i := 0; i < encoder.config.KSim; i++ {
			encoder.gaussians[rep][i] = make([]float32, encoder.config.Dimensions)
			for j := 0; j < encoder.config.Dimensions; j++ {
				u1 := rand.Float64()
				u2 := rand.Float64()
				encoder.gaussians[rep][i][j] = float32(math.Sqrt(-2.0*math.Log(u1)) * math.Cos(2*math.Pi*u2))
			}
		}

		encoder.S[rep] = initProjectionMatrix(encoder.config.DProjections, encoder.config.Dimensions)
	}

	// encoder.Sfinal = initProjectionMatrix(config.DFinal, config.DProjections*config.NumClusters*config.Repetitions)
}

func initProjectionMatrix(rows int, cols int) [][]float32 {
	matrix := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		matrix[i] = make([]float32, cols)
		for j := 0; j < cols; j++ {
			if rand.Float64() < 0.5 {
				matrix[i][j] = 1.0
			} else {
				matrix[i][j] = -1.0
			}
		}
	}
	return matrix
}

// simHash computes the SimHash of a vector using random Gaussian projections
func (e *MuveraEncoder) simHash(vec []float32, gaussians [][]float32) uint64 {
	var result uint64
	distancer := e.dotDistancerProvider.New(vec)

	for i := 0; i < e.config.KSim; i++ {
		dotProduct, err := distancer.Distance(gaussians[i])
		if err != nil {
			return 0.0
		}
		// Set bit based on sign of dot product
		if dotProduct < 0 {
			result |= 1 << uint(i)
		}
	}
	return result
}

func (e *MuveraEncoder) encode(fullVec [][]float32, isDoc bool) []float32 {
	encodedVec := make([]float32, e.config.Repetitions*e.config.NumClusters*e.config.DProjections)

	// For each repetition
	tmpVec := make([]float32, e.config.NumClusters*e.config.Dimensions)
	for rep := 0; rep < e.config.Repetitions; rep++ {
		// Get SimHash for each token
		repetitionClusterCounts := make([]uint16, e.config.NumClusters)
		clusterMappings := make([]uint64, len(fullVec))
		for relative, token := range fullVec {
			cluster := e.simHash(token, e.gaussians[rep])
			clusterMappings[relative] = cluster
			repetitionClusterCounts[cluster]++
			startIdx := cluster * uint64(e.config.Dimensions)
			for i := 0; i < e.config.Dimensions; i++ {
				tmpVec[startIdx+uint64(i)] += token[i]
			}
		}

		// doc ONLY operations
		if isDoc {
			for cluster, count := range repetitionClusterCounts {
				startIdx := uint64(cluster) * uint64(e.config.Dimensions)
				for i := 0; i < e.config.Dimensions; i++ {
					tmpVec[startIdx+uint64(i)] = (1 / float32(count)) * tmpVec[startIdx+uint64(i)]
				}
			}
			for cluster := uint64(0); cluster < uint64(e.config.NumClusters); cluster++ {
				if repetitionClusterCounts[cluster] == 0 {
					// Find nearest non-empty cluster
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
		// doc ONLY operations ended

		scale := 1.0 / float32(math.Sqrt(float64(e.config.DProjections)))
		projOffset := rep * e.config.NumClusters * e.config.DProjections
		matrix := e.S[rep]
		// Process each cluster
		for j := 0; j < e.config.NumClusters; j++ {
			// Calculate source and destination offsets
			srcStart := j * e.config.Dimensions
			dstStart := projOffset + (j * e.config.DProjections)

			// Process in chunks of 4 for better cache utilization
			for k := 0; k < e.config.DProjections; k++ {
				var sum float32
				// Process 4 elements at a time
				for l := 0; l < e.config.Dimensions; l += 4 {
					end := l + 4
					if end > e.config.Dimensions {
						end = e.config.Dimensions
					}
					// Unroll the inner loop
					for m := l; m < end; m++ {
						sum += matrix[k][m] * tmpVec[srcStart+m]
					}
				}
				encodedVec[dstStart+k] = sum * scale
			}
		}

		// Reset tmpVec, this is needed only for query encoding
		for i := range tmpVec {
			tmpVec[i] = 0
		}
	}

	return encodedVec
}

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
	projectedQuery := e.encode(query, false)

	return projectedQuery
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(fullDoc [][]float32) []float32 {
	projectedDoc := e.encode(fullDoc, true)

	return projectedDoc
}
