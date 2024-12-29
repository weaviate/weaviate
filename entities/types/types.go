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

	"github.com/weaviate/weaviate/entities/models"
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

func IsEmptyVector(vector models.Vector) bool {
	switch v := vector.(type) {
	case nil:
		return true
	case []float32:
		return len(v) == 0
	case [][]float32:
		return len(v) == 0
	default:
		return false
	}
}

func GetVectors(in models.Vectors) (map[string][]float32, map[string][][]float32, error) {
	var vectors map[string][]float32
	var multiVectors map[string][][]float32
	if len(in) > 0 {
		for targetVector, vector := range in {
			switch vec := vector.(type) {
			case []interface{}:
				if vectors == nil {
					vectors = make(map[string][]float32)
				}
				asVectorArray := make([]float32, len(vec))
				for i := range vec {
					switch v := vec[i].(type) {
					case json.Number:
						asFloat, err := v.Float64()
						if err != nil {
							return nil, nil, fmt.Errorf("parse []interface{} as vector for target vector: %s: %w", targetVector, err)
						}
						asVectorArray[i] = float32(asFloat)
					case float64:
						asVectorArray[i] = float32(v)
					case float32:
						asVectorArray[i] = v
					default:
						return nil, nil, fmt.Errorf("parse []interface{} as vector for target vector: %s, unrecognized type: %T", targetVector, vec[i])
					}
				}
				vectors[targetVector] = asVectorArray
			case [][]interface{}:
				if multiVectors == nil {
					multiVectors = make(map[string][][]float32)
				}
				asMultiVectorArray := make([][]float32, len(vec))
				for i := range vec {
					asMultiVectorArray[i] = make([]float32, len(vec[i]))
					for j := range vec[i] {
						switch v := vec[i][j].(type) {
						case json.Number:
							asFloat, err := v.Float64()
							if err != nil {
								return nil, nil, fmt.Errorf("parse []interface{} as multi vector for target vector: %s: %w", targetVector, err)
							}
							asMultiVectorArray[i][j] = float32(asFloat)
						case float64:
							asMultiVectorArray[i][j] = float32(v)
						case float32:
							asMultiVectorArray[i][j] = v
						default:
							return nil, nil, fmt.Errorf("parse []interface{} as multi vector for target vector: %s, unrecognized type: %T", targetVector, vec[i])
						}
					}
				}
				multiVectors[targetVector] = asMultiVectorArray
			case []float32:
				if vectors == nil {
					vectors = make(map[string][]float32)
				}
				vectors[targetVector] = vec
			case [][]float32:
				if multiVectors == nil {
					multiVectors = make(map[string][][]float32)
				}
				multiVectors[targetVector] = vec
			default:
				return nil, nil, fmt.Errorf("unrecognized vector type: %T for target vector: %s", vector, targetVector)
			}
		}
	}
	return vectors, multiVectors, nil
}
