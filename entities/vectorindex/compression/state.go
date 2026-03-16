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

package compression

// State holds all compression-related data for an HNSW index.
// This structure is optional - if nil, the index is uncompressed.
type State struct {
	// Compressed indicates if the index uses compression.
	Compressed bool

	// PQData holds Product Quantization compression data.
	PQData *PQData

	// SQData holds Scalar Quantization compression data.
	SQData *SQData

	// RQData holds Rotational Quantization compression data.
	RQData *RQData

	// BRQData holds Binary Rotational Quantization compression data.
	BRQData *BRQData

	// MuveraEnabled indicates if Muvera multi-vector encoding is enabled.
	MuveraEnabled bool

	// MuveraData holds Muvera multi-vector encoding data.
	MuveraData *MuveraData
}

// HasCompression returns true if any compression is enabled.
func (s *State) HasCompression() bool {
	if s == nil {
		return false
	}
	return s.Compressed
}

// HasMuvera returns true if Muvera encoding is enabled.
func (s *State) HasMuvera() bool {
	if s == nil {
		return false
	}
	return s.MuveraEnabled
}
