package multivector

import (
	"math"
	"math/rand"
)

// MuveraConfig holds the configuration for Muvera encoding
type MuveraConfig struct {
	KSim         int
	NumClusters  int // Number of clusters for K-means or number of bits for SimHash
	Dimensions   int // Dimensions of each vector (128 in the Python implementation)
	DProjections int // Number of projections for D-Projections
	Repetitions  int // Number of repetitions (20 in the Python implementation)
}

// DefaultMuveraConfig returns a default configuration matching the Python implementation
func DefaultMuveraConfig() MuveraConfig {
	return MuveraConfig{
		KSim:         3,
		NumClusters:  8,
		Dimensions:   128,
		DProjections: 8,
		Repetitions:  20,
	}
}

// MuveraEncoder handles the encoding process
type MuveraEncoder struct {
	config    MuveraConfig
	gaussians [][]float32 // Random Gaussian vectors for SimHash projection
}

// NewMuveraEncoder creates a new Muvera encoder
func NewMuveraEncoder(config MuveraConfig) *MuveraEncoder {
	encoder := &MuveraEncoder{
		config: config,
	}

	// Initialize Gaussian vectors for each repetition
	encoder.gaussians = make([][]float32, config.Repetitions)
	for i := 0; i < config.Repetitions; i++ {
		encoder.gaussians[i] = make([]float32, config.KSim*config.DProjections)
	}

	return encoder
}

func (e *MuveraEncoder) InitSimHash() error {
	// Initialize random Gaussian vectors for each repetition
	for rep := 0; rep < e.config.Repetitions; rep++ {
		for i := 0; i < e.config.KSim*e.config.DProjections; i++ {
			// Generate random Gaussian values using Box-Muller transform
			u1 := rand.Float64()
			u2 := rand.Float64()
			z0 := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
			e.gaussians[rep][i] = float32(z0)
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
		for j := 0; j < e.config.DProjections; j++ {
			dotProduct += vec[j] * gaussians[i*e.config.DProjections+j]
		}
		// Set bit based on sign of dot product
		if dotProduct > 0 {
			result |= 1 << uint(i)
		}
	}
	return result
}

func (e *MuveraEncoder) encode(vec [][]float32) []float32 {
	result := make([]float32, e.config.Repetitions*e.config.NumClusters*e.config.DProjections)

	// For each repetition
	for rep := 0; rep < e.config.Repetitions; rep++ {
		// Get SimHash for each query token
		for _, token := range vec {
			cluster := e.simHash(token, e.gaussians[rep])
			startIdx := cluster * uint64(e.config.DProjections) * uint64(rep)
			for i := 0; i < e.config.DProjections; i++ {
				result[startIdx+uint64(i)] += token[i]
			}
		}
	}
	return result
}

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
	// Initialize result vector with zeros
	return e.encode(query)
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(doc [][]float32) []float32 {
	// Initialize result vector with zeros
	result := make([]float32, e.config.NumClusters*e.config.DProjections*e.config.Repetitions)

	// For each repetition
	for rep := 0; rep < e.config.Repetitions; rep++ {
		// Get cluster assignments and counts for each doc token
		clusterCounts := make(map[uint64]int)
		for _, docToken := range doc {
			cluster := e.simHash(docToken, e.gaussians[rep])
			clusterCounts[cluster]++
			// Add doc token to the corresponding cluster position
			startIdx := cluster * uint64(e.config.DProjections) * uint64(rep)
			for i := 0; i < e.config.DProjections; i++ {
				result[startIdx+uint64(i)] += docToken[i]
			}
		}

		// Normalize by cluster counts
		for cluster, count := range clusterCounts {
			startIdx := cluster * uint64(e.config.DProjections) * uint64(rep)
			for i := 0; i < e.config.DProjections; i++ {
				result[startIdx+uint64(i)] /= float32(count)
			}
		}

		// Handle empty clusters by finding nearest non-empty cluster
		for cluster := uint64(0); cluster < uint64(e.config.NumClusters); cluster++ {
			if clusterCounts[cluster] == 0 {
				// Find nearest non-empty cluster
				minHamming := math.MaxInt32
				nearestCluster := uint64(0)
				for c, count := range clusterCounts {
					if count > 0 {
						hamming := hammingDistance(cluster, c)
						if hamming < minHamming {
							minHamming = hamming
							nearestCluster = c
						}
					}
				}
				// Copy values from nearest cluster
				startIdx := cluster * uint64(e.config.DProjections) * uint64(rep)
				nearestStartIdx := nearestCluster * uint64(e.config.DProjections) * uint64(rep)
				for i := 0; i < e.config.DProjections; i++ {
					result[startIdx+uint64(i)] = result[nearestStartIdx+uint64(i)]
				}
			}
		}
	}

	return result
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
