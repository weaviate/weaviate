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

package datasets

import (
	"fmt"
	"slices"

	"github.com/parquet-go/parquet-go"
)

type VectorBuilderFloat32 struct {
	vectors [][]float32
	n       int // Number of vectors
	d       int // Dimensionality of vectors to insert, updated after first insertion
	i       int // Index into the vector collection of the current vector under construction
	j       int // Index where to add the next value into the vector being constructed
}

func NewVectorBuilderFloat32(n int) *VectorBuilderFloat32 {
	const maxDimension = (1 << 16) - 1 // Weaviates max dimensionality is 65535
	return &VectorBuilderFloat32{
		vectors: make([][]float32, 0, n),
		n:       n,
		d:       maxDimension,
	}
}

func (vb *VectorBuilderFloat32) Add(value parquet.Value) {
	// Values arrive in sequence. A value with a repetition level of zero
	// indicates that the value is the first entry in a new list.
	if value.RepetitionLevel() == 0 {
		// Learn the dimensionality from the first vector
		if len(vb.vectors) == 1 {
			vb.d = vb.j
			vb.vectors[0] = slices.Clip(vb.vectors[0][:vb.d])
		}
		vb.vectors = append(vb.vectors, make([]float32, vb.d))
		vb.i = len(vb.vectors) - 1
		vb.j = 0
	}
	vb.vectors[vb.i][vb.j] = value.Float()
	vb.j++
}

func (vb *VectorBuilderFloat32) AllVectors() ([][]float32, error) {
	if len(vb.vectors) != vb.n {
		return nil, fmt.Errorf("expected %d vectors, but only %d were constructed", vb.n, len(vb.vectors))
	}
	// This check has already been carried out when we added values, except for
	// the last vector. We do the full check here to keep the code explicit.
	for i, v := range vb.vectors {
		if len(v) != vb.d {
			return nil, fmt.Errorf("the length of the vector at index %d was %d, expected %d", i, len(v), vb.d)
		}
	}
	return vb.vectors, nil
}
