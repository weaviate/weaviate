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

package types

import (
	"encoding/json"
	"fmt"
)

// Just a temporary interface, there should be defined one Embedding interface
// in models to define the Embedding type
type Embedding interface {
	[]float32 | [][]float32
}

type Vector any

func AsVectors(vectors []VectorAdapter) []Vector {
	if len(vectors) > 1 {
		asVectors := make([]Vector, len(vectors))
		for i := range vectors {
			asVectors[i] = vectors[i].Vector
		}
		return asVectors
	}
	return nil
}

type VectorAdapter struct {
	Vector any
}

func (va *VectorAdapter) UnmarshalJSON(data []byte) error {
	var float32Slice []float32
	if err := json.Unmarshal(data, &float32Slice); err == nil {
		va.Vector = float32Slice
		return nil
	}
	var float32Matrix [][]float32
	if err := json.Unmarshal(data, &float32Matrix); err == nil {
		va.Vector = float32Matrix
		return nil
	}
	return fmt.Errorf("searchVectors: cannot unmarshal into either []float32 or [][]float32: %v", string(data))
}
