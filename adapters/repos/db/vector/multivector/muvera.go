package multivector

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// TODO:
// keeping track of the trainingLimit
// preprocessVectors
// insertVectors
// insertVectorsMulti
// search
// searchMulti

// EncodingType represents the type of encoding to use
type EncodingType int

const (
	EncodingKMeans EncodingType = iota
	EncodingSimHash
)

// MuveraConfig holds the configuration for Muvera encoding
type MuveraConfig struct {
	EncodingType EncodingType // Type of encoding to use (K-means or SimHash)
	NumClusters  int          // Number of clusters for K-means or number of bits for SimHash
	Dimensions   int          // Dimensions of each vector (128 in the Python implementation)
	Repetitions  int          // Number of repetitions (20 in the Python implementation)
}

// DefaultMuveraConfig returns a default configuration matching the Python implementation
func DefaultMuveraConfig() MuveraConfig {
	return MuveraConfig{
		EncodingType: EncodingSimHash,
		NumClusters:  8,
		Dimensions:   128,
		Repetitions:  20,
	}
}

// MuveraEncoder handles the encoding process
type MuveraEncoder struct {
	config    MuveraConfig
	kmeans    []*compressionhelpers.KMeans
	gaussians [][]float32 // Random Gaussian vectors for SimHash projection
}

// NewMuveraEncoder creates a new Muvera encoder
func NewMuveraEncoder(config MuveraConfig) *MuveraEncoder {
	encoder := &MuveraEncoder{
		config: config,
	}

	if config.EncodingType == EncodingKMeans {
		encoder.kmeans = make([]*compressionhelpers.KMeans, config.Repetitions)
	} else {
		// Initialize Gaussian vectors for each repetition
		encoder.gaussians = make([][]float32, config.Repetitions)
		for i := 0; i < config.Repetitions; i++ {
			encoder.gaussians[i] = make([]float32, config.NumClusters*config.Dimensions)
		}
	}

	return encoder
}

// Fit initializes the encoder based on the selected encoding type
func (e *MuveraEncoder) Fit(vecs [][][]float32) error {
	fmt.Println("Fitting Muvera encoder")
	fmt.Println("config", e.config)

	if e.config.EncodingType == EncodingKMeans {
		// Concatenate all vectors for training
		var allVectors [][]float32
		for _, doc := range vecs {
			for _, vec := range doc {
				allVectors = append(allVectors, vec)
			}
		}

		// Train k-means for each repetition
		for i := 0; i < e.config.Repetitions; i++ {
			kmeans := compressionhelpers.NewKMeans(e.config.NumClusters, e.config.Dimensions, 0)
			if err := kmeans.Fit(allVectors); err != nil {
				return err
			}
			e.kmeans[i] = kmeans
		}
	} else {
		// Initialize random Gaussian vectors for each repetition
		for rep := 0; rep < e.config.Repetitions; rep++ {
			for i := 0; i < e.config.NumClusters*e.config.Dimensions; i++ {
				// Generate random Gaussian values using Box-Muller transform
				u1 := rand.Float64()
				u2 := rand.Float64()
				z0 := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
				e.gaussians[rep][i] = float32(z0)
			}
		}
	}

	fmt.Println("Muvera encoder fitted")
	return nil
}

// simHash computes the SimHash of a vector using random Gaussian projections
func (e *MuveraEncoder) simHash(vec []float32, gaussians []float32) uint64 {
	var result uint64
	for i := 0; i < e.config.NumClusters; i++ {
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

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
	if e.config.EncodingType == EncodingKMeans {
		// Initialize result vector with zeros
		result := make([]float32, e.config.NumClusters*e.config.Dimensions*e.config.Repetitions)

		// For each repetition
		for rep := 0; rep < e.config.Repetitions; rep++ {
			// Get cluster assignments for each query token
			for _, queryToken := range query {
				cluster := e.kmeans[rep].Nearest(queryToken)
				// Add query token to the corresponding cluster position
				startIdx := cluster * uint64(e.config.Dimensions) * uint64(rep)
				for i := 0; i < e.config.Dimensions; i++ {
					result[startIdx+uint64(i)] += queryToken[i]
				}
			}
		}
		return result
	} else {
		// Initialize result vector with zeros
		result := make([]float32, e.config.NumClusters*e.config.Repetitions)

		// For each repetition
		for rep := 0; rep < e.config.Repetitions; rep++ {
			// Get SimHash for each query token
			for _, queryToken := range query {
				hash := e.simHash(queryToken, e.gaussians[rep])
				// Add hash bits to the result vector
				for i := 0; i < e.config.NumClusters; i++ {
					if (hash & (1 << uint(i))) != 0 {
						result[rep*e.config.NumClusters+i] += 1
					} else {
						result[rep*e.config.NumClusters+i] -= 1
					}
				}
			}
		}
		return result
	}
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(doc [][]float32) []float32 {
	if e.config.EncodingType == EncodingKMeans {
		// Initialize result vector with zeros
		result := make([]float32, e.config.NumClusters*e.config.Dimensions*e.config.Repetitions)

		// For each repetition
		for rep := 0; rep < e.config.Repetitions; rep++ {
			// Get cluster assignments and counts for each doc token
			clusterCounts := make(map[uint64]int)
			for _, docToken := range doc {
				cluster := e.kmeans[rep].Nearest(docToken)
				clusterCounts[cluster]++
				// Add doc token to the corresponding cluster position
				startIdx := cluster * uint64(e.config.Dimensions) * uint64(rep)
				for i := 0; i < e.config.Dimensions; i++ {
					result[startIdx+uint64(i)] += docToken[i]
				}
			}

			// Normalize by cluster counts
			for cluster, count := range clusterCounts {
				startIdx := cluster * uint64(e.config.Dimensions) * uint64(rep)
				for i := 0; i < e.config.Dimensions; i++ {
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
					startIdx := cluster * uint64(e.config.Dimensions) * uint64(rep)
					nearestStartIdx := nearestCluster * uint64(e.config.Dimensions) * uint64(rep)
					for i := 0; i < e.config.Dimensions; i++ {
						result[startIdx+uint64(i)] = result[nearestStartIdx+uint64(i)]
					}
				}
			}
		}
		return result
	} else {
		// Initialize result vector with zeros
		result := make([]float32, e.config.NumClusters*e.config.Repetitions)

		// For each repetition
		for rep := 0; rep < e.config.Repetitions; rep++ {
			// Get SimHash for each doc token
			for _, docToken := range doc {
				hash := e.simHash(docToken, e.gaussians[rep])
				// Add hash bits to the result vector
				for i := 0; i < e.config.NumClusters; i++ {
					if (hash & (1 << uint(i))) != 0 {
						result[rep*e.config.NumClusters+i] += 1
					} else {
						result[rep*e.config.NumClusters+i] -= 1
					}
				}
			}

			// Normalize by number of tokens
			numTokens := float32(len(doc))
			for i := 0; i < e.config.NumClusters; i++ {
				result[rep*e.config.NumClusters+i] /= numTokens
			}
		}
		return result
	}
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

// ProcessingMuvera processes vectors using Muvera encoding
func ProcessingMuvera(encoder *MuveraEncoder, vectors [][][]float32) [][]float32 {
	// Fit the encoder
	if err := encoder.Fit(vectors); err != nil {
		panic(err)
	}

	// Process all vectors
	result := make([][]float32, len(vectors))
	for i, v := range vectors {
		result[i] = encoder.EncodeDoc(v)
	}

	return result
}
