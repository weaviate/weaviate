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
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"

	"github.com/tphakala/simd/f32"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/byteops"
)

type MuveraConfig struct {
	KSim         int
	NumClusters  int // Number of clusters for K-means or number of bits for SimHash
	Dimensions   int // Dimensions of each vector
	DProjections int // Number of projections for D-Projections
	DFinal       int // Number of projections for final projection
	Repetitions  int // Number of repetitions
}

type MuveraEncoder struct {
	config    MuveraConfig
	gaussians [][][]float32 // Random Gaussian vectors for SimHash projection
	S         [][][]float32 // Random projection matrix with ±1 entries

	// Flattened row-major copies for SIMD dot kernels; never persisted
	gaussiansAllFlat []float32
	gaussiansFlat    [][]float32
	sFlat            [][]float32

	muveraStore *lsmkv.Store
}

const (
	DefaultMuveraSeed = uint64(0x532ca5105169b1df)
)

func NewMuveraEncoder(config ent.MuveraConfig, muveraStore *lsmkv.Store) *MuveraEncoder {
	encoder := &MuveraEncoder{
		config: MuveraConfig{
			KSim:         config.KSim,
			NumClusters:  int(math.Pow(2, float64(config.KSim))),
			DProjections: config.DProjections,
			Repetitions:  config.Repetitions,
		},
		muveraStore: muveraStore,
	}

	return encoder
}

func (encoder *MuveraEncoder) InitEncoder(dimensions int) {
	rng := rand.New(rand.NewPCG(DefaultMuveraSeed, 0x385ab5285169b1ac))
	encoder.config.Dimensions = dimensions
	encoder.gaussians = make([][][]float32, encoder.config.Repetitions)
	encoder.S = make([][][]float32, encoder.config.Repetitions)
	for rep := 0; rep < encoder.config.Repetitions; rep++ {
		// Initialize random Gaussian vectors
		encoder.gaussians[rep] = make([][]float32, encoder.config.KSim)
		for i := 0; i < encoder.config.KSim; i++ {
			encoder.gaussians[rep][i] = make([]float32, encoder.config.Dimensions)
			for j := 0; j < encoder.config.Dimensions; j++ {
				u1 := rng.Float64()
				u2 := rng.Float64()
				encoder.gaussians[rep][i][j] = float32(math.Sqrt(-2.0*math.Log(u1)) * math.Cos(2*math.Pi*u2))
			}
		}

		encoder.S[rep] = initProjectionMatrix(encoder.config.DProjections, encoder.config.Dimensions, rng)
	}
	encoder.buildFlatMatrices()
}

func (e *MuveraEncoder) buildFlatMatrices() {
	blockLen := e.config.KSim * e.config.Dimensions
	e.gaussiansAllFlat = make([]float32, len(e.gaussians)*blockLen)
	e.gaussiansFlat = make([][]float32, len(e.gaussians))
	for rep := range e.gaussians {
		block := e.gaussiansAllFlat[rep*blockLen : (rep+1)*blockLen]
		for i, row := range e.gaussians[rep] {
			copy(block[i*e.config.Dimensions:(i+1)*e.config.Dimensions], row)
		}
		e.gaussiansFlat[rep] = block
	}
	e.sFlat = flattenMatrices(e.S, e.config.Dimensions)
}

func flattenMatrices(matrices [][][]float32, cols int) [][]float32 {
	out := make([][]float32, len(matrices))
	for rep := range matrices {
		flat := make([]float32, len(matrices[rep])*cols)
		for i, row := range matrices[rep] {
			copy(flat[i*cols:(i+1)*cols], row)
		}
		out[rep] = flat
	}
	return out
}

func initProjectionMatrix(rows int, cols int, rng *rand.Rand) [][]float32 {
	matrix := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		matrix[i] = make([]float32, cols)
		for j := 0; j < cols; j++ {
			matrix[i][j] = float32(rng.IntN(2)*2 - 1)
		}
	}
	return matrix
}

// simHash computes the SimHash of a vector using random Gaussian projections.
// gaussiansFlat is one repetition's row-major KSim×Dimensions matrix; dots is
// a caller-provided scratch of at least KSim entries.
func (e *MuveraEncoder) simHash(vec []float32, gaussiansFlat []float32, dots []float32) uint64 {
	dots = dots[:e.config.KSim]
	f32.DotProductStrided(dots, gaussiansFlat, vec, e.config.KSim, e.config.Dimensions, e.config.Dimensions)
	var result uint64
	for i, dot := range dots {
		// Set bit based on sign of dot product
		if dot > 0 {
			result |= 1 << uint(i)
		}
	}
	return result
}

func (e *MuveraEncoder) encode(fullVec [][]float32, isDoc bool) []float32 {
	if len(fullVec) == 0 {
		return nil
	}
	dims := e.config.Dimensions
	numClusters := e.config.NumClusters
	dProjections := e.config.DProjections

	encodedVec := make([]float32, e.config.Repetitions*numClusters*dProjections)

	numHashRows := e.config.Repetitions * e.config.KSim
	dots := make([]float32, numHashRows)
	allClusterMappings := make([]uint64, e.config.Repetitions*len(fullVec))
	for relative, token := range fullVec {
		f32.DotProductStrided(dots, e.gaussiansAllFlat, token, numHashRows, dims, dims)
		for rep := 0; rep < e.config.Repetitions; rep++ {
			var cluster uint64
			repDots := dots[rep*e.config.KSim : (rep+1)*e.config.KSim]
			for i, dot := range repDots {
				// Set bit based on sign of dot product
				if dot > 0 {
					cluster |= 1 << uint(i)
				}
			}
			allClusterMappings[rep*len(fullVec)+relative] = cluster
		}
	}

	tmpVec := make([]float32, numClusters*dims)
	repetitionClusterCounts := make([]uint16, numClusters)
	for rep := 0; rep < e.config.Repetitions; rep++ {
		if rep > 0 {
			clear(tmpVec)
			clear(repetitionClusterCounts)
		}
		clusterMappings := allClusterMappings[rep*len(fullVec) : (rep+1)*len(fullVec)]
		for relative, token := range fullVec {
			cluster := clusterMappings[relative]
			repetitionClusterCounts[cluster]++
			startIdx := cluster * uint64(dims)
			dst := tmpVec[startIdx : startIdx+uint64(dims)]
			f32.Add(dst, dst, token)
		}

		// doc ONLY operations
		if isDoc {
			for cluster, count := range repetitionClusterCounts {
				// count == 0 is overwritten below, count == 1 is an exact
				// no-op (1/1 * x == x)
				if count > 1 {
					startIdx := cluster * dims
					sl := tmpVec[startIdx : startIdx+dims]
					f32.Scale(sl, sl, 1/float32(count))
				}
			}
			for cluster := uint64(0); cluster < uint64(numClusters); cluster++ {
				if repetitionClusterCounts[cluster] == 0 {
					// Find nearest non-empty cluster by Hamming distance on
					// the simhash bits
					minHamming := 65 // more than the 64 bits of a hash
					nearestPoint := 0
					for docIdx, clusterMapped := range clusterMappings {
						hamming := bits.OnesCount64(cluster ^ clusterMapped)
						if hamming < minHamming {
							minHamming = hamming
							nearestPoint = docIdx
						}
					}
					startIdx := cluster * uint64(dims)
					copy(tmpVec[startIdx:startIdx+uint64(dims)], fullVec[nearestPoint])
				}
			}
		}
		// doc ONLY operations ended

		projOffset := rep * numClusters * dProjections
		sFlat := e.sFlat[rep]
		// Project each cluster's aggregated vector through this repetition's
		// DProjections×Dimensions ±1 matrix
		for j := 0; j < numClusters; j++ {
			srcStart := j * dims
			dstStart := projOffset + (j * dProjections)
			f32.DotProductStrided(encodedVec[dstStart:dstStart+dProjections],
				sFlat, tmpVec[srcStart:srcStart+dims], dProjections, dims, dims)
		}
	}

	scale := 1.0 / float32(math.Sqrt(float64(dProjections)))
	f32.Scale(encodedVec, encodedVec, scale)

	return encodedVec
}

// EncodeQuery encodes a query vector using Muvera
func (e *MuveraEncoder) EncodeQuery(query [][]float32) []float32 {
	return e.encode(query, false)
}

// EncodeDoc encodes a document vector using Muvera
func (e *MuveraEncoder) EncodeDoc(fullDoc [][]float32) []float32 {
	return e.encode(fullDoc, true)
}

func (e *MuveraEncoder) Dimensions() int {
	return e.config.Dimensions
}

func MuveraBytesFromFloat32(vec []float32) []byte {
	slice := make([]byte, len(vec)*4)
	byteops.CopySliceToBytes(slice, vec)
	return slice
}

func MuveraFromBytes(bytes []byte) []float32 {
	vec := make([]float32, len(bytes)/4)
	byteops.CopyBytesToSlice(vec, bytes)
	return vec
}

func (e *MuveraEncoder) GetMuveraVectorForID(id uint64, bucket string) ([]float32, error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	muveraBytes, err := e.muveraStore.Bucket(bucket).Get(idBytes)
	if err != nil {
		return nil, fmt.Errorf("getting vector for id: %w", err)
	}
	if len(muveraBytes) == 0 {
		return nil, storobj.NewErrNotFoundf(id, "GetMuveraVectorForID")
	}

	return MuveraFromBytes(muveraBytes), nil
}

// MuveraData is an alias for the MuveraData type in entities/vectorindex/compression.
type MuveraData = compression.MuveraData

type CommitLogger interface {
	AddMuvera(MuveraData) error
}

func (e *MuveraEncoder) PersistMuvera(logger CommitLogger) error {
	return logger.AddMuvera(MuveraData{
		KSim:         uint32(e.config.KSim),
		NumClusters:  uint32(e.config.NumClusters),
		Dimensions:   uint32(e.config.Dimensions),
		DProjections: uint32(e.config.DProjections),
		Repetitions:  uint32(e.config.Repetitions),
		Gaussians:    e.gaussians,
		S:            e.S,
	})
}

func (e *MuveraEncoder) LoadMuveraConfig(data MuveraData) {
	e.config.KSim = int(data.KSim)
	e.config.NumClusters = int(data.NumClusters)
	e.config.Dimensions = int(data.Dimensions)
	e.config.DProjections = int(data.DProjections)
	e.config.Repetitions = int(data.Repetitions)
	e.gaussians = data.Gaussians
	e.S = data.S
	e.buildFlatMatrices()
}
