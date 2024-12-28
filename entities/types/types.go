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
	"fmt"
)

// Just a temporary interface, there should be defined one Embedding interface
// in models to define the Embedding type
type Embedding interface {
	[]float32 | [][]float32
}

type Vector any

func IsVectorEmpty(vector Vector) (bool, error) {
	switch v := vector.(type) {
	case nil:
		return true, nil
	case []float32:
		return len(v) == 0, nil
	case [][]float32:
		return len(v) == 0, nil
	default:
		return false, fmt.Errorf("unrecognized vector type: %T", vector)
	}
}
