package multivector

import (
	"fmt"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// TODO:
// keeping track of the trainingLimit
// preprocessVectors
// insertVectors
// insertVectorsMulti
// search
// searchMulti

// MuveraConfig holds the configuration for Muvera encoding
type MuveraConfig struct {
	NumClusters int // Number of clusters (2^3 = 8 in the Python implementation)
	Dimensions  int // Dimensions of each vector (128 in the Python implementation)
	Repetitions int // Number of repetitions (20 in the Python implementation)
}

// DefaultMuveraConfig returns a default configuration matching the Python implementation
func DefaultMuveraConfig() MuveraConfig {
	return MuveraConfig{
		NumClusters: 8,
		Dimensions:  128,
		Repetitions: 20,
	}
}

// MuveraEncoder handles the encoding process
type MuveraEncoder struct {
	config MuveraConfig
	kmeans []*compressionhelpers.KMeans
}

// NewMuveraEncoder creates a new Muvera encoder
func NewMuveraEncoder(config MuveraConfig) *MuveraEncoder {
	return &MuveraEncoder{
		config: config,
		kmeans: make([]*compressionhelpers.KMeans, config.Repetitions),
	}
}

// Fit trains the k-means models on the concatenated data
func (e *MuveraEncoder) Fit(vecs [][][]float32) error {
	fmt.Println("Fitting Muvera encoder")
	// Concatenate all vectors for training
	var allVectors [][]float32
	for _, doc := range vecs {
		for _, vec := range doc {
			allVectors = append(allVectors, vec)
		}
	}
	fmt.Println("config", e.config)
	// Train k-means for each repetition
	for i := 0; i < e.config.Repetitions; i++ {
		kmeans := compressionhelpers.NewKMeans(e.config.NumClusters, e.config.Dimensions, 0) // TODO: check segments is correct to be 1
		if err := kmeans.Fit(allVectors); err != nil {
			return err
		}
		e.kmeans[i] = kmeans
	}
	fmt.Println("Muvera encoder fitted")
	return nil
}

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
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
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(doc [][]float32) []float32 {
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
