//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package pni

import (
	"math"
	"math/rand"
)

// Encoder handles RQ1 (Rotational Quantization 1-bit) encoding.
// It rotates vectors using a random orthogonal matrix, then quantizes
// each dimension to 1 bit based on sign.
type Encoder struct {
	dims       int
	segments   int
	rotation   [][]float32 // dims x dims orthogonal matrix
	rotatedBuf []float32   // reusable buffer for rotation
}

// NewEncoder creates a new RQ1 encoder.
func NewEncoder(dims int, seed uint64) *Encoder {
	segments := (dims + 63) / 64

	enc := &Encoder{
		dims:       dims,
		segments:   segments,
		rotation:   generateOrthogonalMatrix(dims, seed),
		rotatedBuf: make([]float32, dims),
	}

	return enc
}

// Dims returns the dimensionality.
func (e *Encoder) Dims() int {
	return e.dims
}

// Segments returns the number of 64-bit segments.
func (e *Encoder) Segments() int {
	return e.segments
}

// Encode encodes a vector into segments of uint64 codes.
// Returns a slice of length e.segments.
func (e *Encoder) Encode(vec []float32) []uint64 {
	codes := make([]uint64, e.segments)
	e.EncodeInto(vec, codes)
	return codes
}

// EncodeInto encodes a vector into the provided codes slice.
// codes must have length >= e.segments.
func (e *Encoder) EncodeInto(vec []float32, codes []uint64) {
	// Rotate the vector
	rotated := e.rotatedBuf
	for i := 0; i < e.dims; i++ {
		var sum float32
		for j := 0; j < len(vec); j++ {
			sum += e.rotation[i][j] * vec[j]
		}
		rotated[i] = sum
	}

	// Quantize to bits: positive -> 1, negative -> 0
	for seg := 0; seg < e.segments; seg++ {
		var code uint64
		start := seg * 64
		end := start + 64
		if end > e.dims {
			end = e.dims
		}

		for i := start; i < end; i++ {
			if rotated[i] > 0 {
				code |= 1 << (i - start)
			}
		}
		codes[seg] = code
	}
}

// generateOrthogonalMatrix generates a random orthogonal matrix using
// QR decomposition of a random Gaussian matrix.
func generateOrthogonalMatrix(dims int, seed uint64) [][]float32 {
	rng := rand.New(rand.NewSource(int64(seed)))

	// Generate random Gaussian matrix
	A := make([][]float64, dims)
	for i := range A {
		A[i] = make([]float64, dims)
		for j := range A[i] {
			A[i][j] = rng.NormFloat64()
		}
	}

	// QR decomposition using Gram-Schmidt
	Q := make([][]float64, dims)
	for i := range Q {
		Q[i] = make([]float64, dims)
	}

	for j := 0; j < dims; j++ {
		// Copy column j of A to v
		v := make([]float64, dims)
		for i := 0; i < dims; i++ {
			v[i] = A[i][j]
		}

		// Subtract projections onto previous Q columns
		for k := 0; k < j; k++ {
			dot := 0.0
			for i := 0; i < dims; i++ {
				dot += v[i] * Q[i][k]
			}
			for i := 0; i < dims; i++ {
				v[i] -= dot * Q[i][k]
			}
		}

		// Normalize
		norm := 0.0
		for i := 0; i < dims; i++ {
			norm += v[i] * v[i]
		}
		norm = math.Sqrt(norm)
		if norm > 1e-10 {
			for i := 0; i < dims; i++ {
				Q[i][j] = v[i] / norm
			}
		}
	}

	// Convert to float32
	result := make([][]float32, dims)
	for i := range result {
		result[i] = make([]float32, dims)
		for j := range result[i] {
			result[i][j] = float32(Q[i][j])
		}
	}

	return result
}
