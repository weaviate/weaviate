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
	"fmt"
)

// Index is the Progressive Neighborhood Index.
// It stores vectors in a columnar compressed format for progressive elimination.
type Index struct {
	// segments stores compressed codes in segment-major layout.
	// segments[segmentID][docID] = uint64 code for that segment.
	segments [][]uint64

	// vectors stores original float vectors for final rescore.
	vectors [][]float32

	// encoder handles RQ1 encoding.
	encoder *Encoder

	// dims is the vector dimensionality.
	dims int

	// segmentCount is the number of 64-bit segments.
	segmentCount int

	// docCount is the number of documents in the index.
	docCount int
}

// NewIndex creates a new empty Progressive Neighborhood Index.
func NewIndex(dims int, seed uint64) *Index {
	encoder := NewEncoder(dims, seed)

	return &Index{
		encoder:      encoder,
		dims:         dims,
		segmentCount: encoder.Segments(),
		docCount:     0,
	}
}

// Build builds the index from a slice of vectors.
// This replaces any existing data in the index.
func (idx *Index) Build(vectors [][]float32) error {
	if len(vectors) == 0 {
		return fmt.Errorf("cannot build index with 0 vectors")
	}

	// Validate dimensions
	for i, vec := range vectors {
		if len(vec) != idx.dims {
			return fmt.Errorf("vector %d has %d dims, expected %d", i, len(vec), idx.dims)
		}
	}

	docCount := len(vectors)

	// Allocate segment-major storage
	segments := make([][]uint64, idx.segmentCount)
	for seg := 0; seg < idx.segmentCount; seg++ {
		segments[seg] = make([]uint64, docCount)
	}

	// Encode all vectors
	codes := make([]uint64, idx.segmentCount)
	for docID, vec := range vectors {
		idx.encoder.EncodeInto(vec, codes)
		for seg := 0; seg < idx.segmentCount; seg++ {
			segments[seg][docID] = codes[seg]
		}
	}

	// Store original vectors for rescore
	storedVectors := make([][]float32, docCount)
	for i, vec := range vectors {
		storedVectors[i] = make([]float32, len(vec))
		copy(storedVectors[i], vec)
	}

	idx.segments = segments
	idx.vectors = storedVectors
	idx.docCount = docCount

	return nil
}

// DocCount returns the number of documents in the index.
func (idx *Index) DocCount() int {
	return idx.docCount
}

// Dims returns the dimensionality.
func (idx *Index) Dims() int {
	return idx.dims
}

// SegmentCount returns the number of segments.
func (idx *Index) SegmentCount() int {
	return idx.segmentCount
}

// MemoryFootprint returns the approximate memory usage in bytes.
func (idx *Index) MemoryFootprint() int64 {
	// Segments: segmentCount * docCount * 8 bytes
	segmentBytes := int64(idx.segmentCount) * int64(idx.docCount) * 8

	// Vectors: docCount * dims * 4 bytes
	vectorBytes := int64(idx.docCount) * int64(idx.dims) * 4

	// Rotation matrix: dims * dims * 4 bytes
	rotationBytes := int64(idx.dims) * int64(idx.dims) * 4

	return segmentBytes + vectorBytes + rotationBytes
}

// BytesPerVector returns the average bytes per vector.
func (idx *Index) BytesPerVector() float64 {
	if idx.docCount == 0 {
		return 0
	}
	return float64(idx.MemoryFootprint()) / float64(idx.docCount)
}
