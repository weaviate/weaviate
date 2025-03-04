package compressionhelpers

import (
	"encoding/binary"
	"math"
	"math/rand"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type LSHQuantizer struct {
	numBands      int
	projPerBand   int
	dimensions    int
	compressedDim int
	projections   [][]float32
	distancer     distancer.Provider
}

func NewLSHQuantizer(projPerBand, numBands, dimensions int) *LSHQuantizer {
	lsh := &LSHQuantizer{
		numBands:      numBands,
		projPerBand:   projPerBand,
		dimensions:    dimensions,
		compressedDim: projPerBand * numBands,
		projections:   make([][]float32, projPerBand*numBands),
		distancer:     distancer.NewCosineDistanceProvider(),
	}

	// Generate random projections
	for i := 0; i < projPerBand*numBands; i++ {
		lsh.projections[i] = lsh.generateRandomProjection()
	}

	return lsh
}

func (lshq LSHQuantizer) Encode(vec []float32) []uint64 {
	total := lshq.compressedDim / 64
	if lshq.compressedDim%64 != 0 {
		total++
	}
	code := make([]uint64, total)
	pos := 0
	dimsPerBand := lshq.dimensions / lshq.numBands
	for band := 0; band < lshq.numBands; band++ {
		for proj := 0; proj < lshq.projPerBand; proj++ {
			d, _ := lshq.distancer.SingleDist(vec[band*dimsPerBand:(band+1)*dimsPerBand], lshq.projections[band*lshq.projPerBand+proj])
			if d >= 1 {
				code[pos/64] += uint64(math.Pow(2, float64(pos%64)))
			}
			pos++
		}
	}

	return code
}

func (lshq LSHQuantizer) Encode8(vec []float32) []byte {
	code := lshq.Encode(vec)
	code8 := make([]byte, 8*len(code))
	for i := range code {
		binary.LittleEndian.PutUint64(code8[i*8:], code[i])
	}
	return code8
}

func (lshq LSHQuantizer) Encode32(vec []float32) []uint32 {
	code := lshq.Encode(vec)
	code8 := make([]byte, 8*len(code))
	for i := range code {
		binary.LittleEndian.PutUint64(code8[i*2:], code[i])
	}
	code32 := make([]uint32, 2*len(code))
	for i := range code32 {
		code32[i] = binary.LittleEndian.Uint32(code8[i*4:])
	}
	return code32
}

func (lshq LSHQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	return distancer.HammingBitwise(x, y)
}

// generateRandomProjection creates a random unit vector for projection
func (lshq *LSHQuantizer) generateRandomProjection() []float32 {
	// Generate a random unit vector
	vector := make([]float32, lshq.dimensions/lshq.numBands)

	// Use Gaussian distribution for random projection
	sumSquares := float32(0.0)
	for i := range vector {
		vector[i] = float32(rand.NormFloat64())
		sumSquares += vector[i] * vector[i]
	}

	// Normalize to unit vector
	magnitude := float32(math.Sqrt(float64(sumSquares)))
	for i := range vector {
		vector[i] /= magnitude
	}

	return vector
}
