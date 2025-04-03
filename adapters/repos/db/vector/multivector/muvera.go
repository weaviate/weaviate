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
)

type MuveraConfig struct {
	KSim         int
	NumClusters  int // Number of clusters for K-means or number of bits for SimHash
	Dimensions   int // Dimensions of each vector (128 in the Python implementation)
	DProjections int // Number of projections for D-Projections
	DFinal       int // Number of projections for final projection
	Repetitions  int // Number of repetitions (20 in the Python implementation)
}

func DefaultMuveraConfig() MuveraConfig {
	kSim := 3
	numClusters := int(math.Pow(2, float64(kSim)))
	dProjections := 8
	reps := 20
	return MuveraConfig{
		KSim:         kSim,
		NumClusters:  numClusters,
		Dimensions:   128,
		DProjections: dProjections,
		DFinal:       dProjections * numClusters * reps, // DProjections * NumClusters * Repetitions
		Repetitions:  reps,
	}
}

type MuveraEncoder struct {
	config    MuveraConfig
	gaussians [][]float32 // Random Gaussian vectors for SimHash projection
	S         [][]float32 // Random projection matrix with ±1 entries
	Sfinal    [][]float32 // Random projection matrix with ±1 entries
}

func NewMuveraEncoder(config MuveraConfig) *MuveraEncoder {
	encoder := &MuveraEncoder{
		config: config,
	}

	encoder.gaussians = make([][]float32, config.Repetitions)
	for i := 0; i < config.Repetitions; i++ {
		encoder.gaussians[i] = make([]float32, config.KSim*config.Dimensions)
	}

	encoder.S = make([][]float32, config.DProjections)
	for i := 0; i < config.DProjections; i++ {
		encoder.S[i] = make([]float32, config.Dimensions)
		for j := 0; j < config.Dimensions; j++ {
			if rand.Float64() < 0.5 {
				encoder.S[i][j] = 1.0
			} else {
				encoder.S[i][j] = -1.0
			}
		}
	}

	encoder.Sfinal = make([][]float32, config.DFinal)
	for i := 0; i < config.DFinal; i++ {
		dim := config.DProjections * config.NumClusters * config.Repetitions
		encoder.Sfinal[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			if rand.Float64() < 0.5 {
				encoder.Sfinal[i][j] = 1.0
			} else {
				encoder.Sfinal[i][j] = -1.0
			}
		}
	}

	return encoder
}

func boxMullerVector(dimensions int) []float32 {
	vector := make([]float32, dimensions)

	for i := 0; i < dimensions; i++ {
		u1 := rand.Float64()
		u2 := rand.Float64()
		vector[i] = float32(math.Sqrt(-2.0*math.Log(u1)) * math.Cos(2*math.Pi*u2))
	}

	return vector
}

func (e *MuveraEncoder) InitSimHash() error {
	for rep := 0; rep < e.config.Repetitions; rep++ {
		/*for i := 0; i < e.config.Dimensions*e.config.KSim; i++ {
			// Box-Muller transform
			u1 := rand.Float64()
			u2 := rand.Float64()
			z0 := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
			e.gaussians[rep][i] = float32(z0)
		}*/
		for i := 0; i < e.config.KSim; i++ {
			offsetKSim := i * e.config.Dimensions
			vec := boxMullerVector(e.config.Dimensions)
			copy(e.gaussians[rep][offsetKSim:offsetKSim+e.config.Dimensions], vec)
		}
	}
	return nil
}

// simHash computes the SimHash of a vector using random Gaussian projections
func (e *MuveraEncoder) simHash(vec []float32, gaussians []float32) uint64 {
	var result uint64
	for i := 0; i < e.config.KSim; i++ {
		// Compute dot product with Gaussian vector
		var dotProduct float32
		for j := 0; j < e.config.Dimensions; j++ {
			dotProduct += vec[j] * gaussians[i*e.config.Dimensions+j]
		}
		// Set bit based on sign of dot product
		if dotProduct > 0 {
			result |= 1 << uint(i)
		}
	}
	return result
}

func (e *MuveraEncoder) computeMatrixVecProduct(matrix [][]float32, vec []float32, scale float32) []float32 {
	projectedVec := make([]float32, len(matrix))
	for i := 0; i < len(matrix); i++ {
		for j := 0; j < len(matrix[i]); j++ {
			projectedVec[i] += matrix[i][j] * vec[j]
		}
		projectedVec[i] = scale * projectedVec[i]
	}
	return projectedVec
}

func (e *MuveraEncoder) projectVecFlat(vec []float32, dprojections int) []float32 {
	if dprojections == e.config.Dimensions {
		return vec
	}
	projectedVec := make([]float32, e.config.Repetitions*e.config.NumClusters*dprojections)
	scale := 1.0 / float32(math.Sqrt(float64(dprojections)))

	for i := 0; i < e.config.Repetitions; i++ {
		offsetReps := i * e.config.NumClusters * e.config.Dimensions
		offsetRepsProjected := i * e.config.NumClusters * dprojections
		for j := 0; j < e.config.NumClusters; j++ {
			start := offsetReps + (j * e.config.Dimensions)
			end := start + e.config.Dimensions
			subvector := vec[start:end]
			startProjected := offsetRepsProjected + (j * dprojections)
			endProjected := startProjected + dprojections
			copy(projectedVec[startProjected:endProjected], e.computeMatrixVecProduct(e.S, subvector, scale))
		}
	}

	return projectedVec
}

func (e *MuveraEncoder) finalProjection(vec []float32, dfinal int) []float32 {
	if dfinal == e.config.Dimensions*e.config.Repetitions*e.config.NumClusters {
		return vec
	}
	projectedVec := make([]float32, dfinal)
	scale := 1.0 / float32(math.Sqrt(float64(dfinal)))

	for i := 0; i < dfinal; i++ {
		var sum float32
		for j := 0; j < len(vec); j++ {
			sum += vec[j] * e.Sfinal[i][j]
		}
		projectedVec[i] = scale * sum
	}
	return projectedVec
}

func (e *MuveraEncoder) encode(fullVec [][]float32) ([]float32, []map[uint64]int) {
	encodedVec := make([]float32, e.config.Repetitions*e.config.NumClusters*e.config.Dimensions)

	// For each repetition
	repetitionClusterCounts := make([]map[uint64]int, e.config.Repetitions)
	for rep := 0; rep < e.config.Repetitions; rep++ {
		// Get SimHash for each token
		repetitionClusterCounts[rep] = make(map[uint64]int)
		offsetRep := rep * e.config.NumClusters * e.config.Dimensions
		for _, token := range fullVec {
			cluster := e.simHash(token, e.gaussians[rep])
			repetitionClusterCounts[rep][cluster]++
			startIdx := uint64(offsetRep) + cluster*uint64(e.config.Dimensions)
			for i := 0; i < e.config.Dimensions; i++ {
				encodedVec[startIdx+uint64(i)] += token[i]
			}
		}
	}

	return encodedVec, repetitionClusterCounts
}

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
	encodedQuery, _ := e.encode(query)
	projectedQuery := e.projectVecFlat(encodedQuery, e.config.DProjections)
	return e.finalProjection(projectedQuery, e.config.DFinal)
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(fullDoc [][]float32) []float32 {
	encodedDoc, repetitionClusterCounts := e.encode(fullDoc)

	// For each repetition
	for rep := 0; rep < e.config.Repetitions; rep++ {
		// Normalize by cluster counts
		offsetRep := rep * e.config.NumClusters * e.config.Dimensions
		for cluster, count := range repetitionClusterCounts[rep] {
			startIdx := uint64(offsetRep) + cluster*uint64(e.config.Dimensions)
			for i := 0; i < e.config.Dimensions; i++ {
				encodedDoc[startIdx+uint64(i)] = (1 / float32(count)) * encodedDoc[startIdx+uint64(i)]
			}
		}

		// Handle empty clusters by finding nearest non-empty cluster
		for cluster := uint64(0); cluster < uint64(e.config.NumClusters); cluster++ {
			if repetitionClusterCounts[rep][cluster] == 0 {
				// Find nearest non-empty cluster
				minHamming := math.MaxInt32
				nearestCluster := uint64(0)
				for c, count := range repetitionClusterCounts[rep] {
					if count > 0 {
						hamming := hammingDistance(cluster, c)
						if hamming < minHamming {
							minHamming = hamming
							nearestCluster = c
						}
					}
				}
				startIdx := uint64(offsetRep) + cluster*uint64(e.config.Dimensions)
				nearestStartIdx := uint64(offsetRep) + nearestCluster*uint64(e.config.Dimensions)
				for i := 0; i < e.config.Dimensions; i++ {
					encodedDoc[startIdx+uint64(i)] = encodedDoc[nearestStartIdx+uint64(i)]
				}
			}
		}
	}
	projectedDoc := e.projectVecFlat(encodedDoc, e.config.DProjections)
	return e.finalProjection(projectedDoc, e.config.DFinal)
}

// hammingDistance calculates the Hamming distance between two uint64 numbers
func hammingDistance(a, b uint64) int {
	x := a ^ b
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}
