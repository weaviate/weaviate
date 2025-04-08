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

package dto

import (
	"encoding/json"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

type GroupParams struct {
	Strategy string
	Force    float32
}

type TargetCombinationType int

const (
	Sum TargetCombinationType = iota
	Average
	Minimum
	ManualWeights
	RelativeScore
)

// no weights are set for default, needs to be added if this is changed to something else
const DefaultTargetCombinationType = Minimum

type TargetCombination struct {
	// just one of these can be set, precedence in order
	Type    TargetCombinationType
	Weights []float32
}

type GetParams struct {
	Filters                 *filters.LocalFilter
	ClassName               string
	Pagination              *filters.Pagination
	Cursor                  *filters.Cursor
	Sort                    []filters.Sort
	Properties              search.SelectProperties
	NearVector              *searchparams.NearVector
	NearObject              *searchparams.NearObject
	KeywordRanking          *searchparams.KeywordRanking
	HybridSearch            *searchparams.HybridSearch
	GroupBy                 *searchparams.GroupBy
	TargetVector            string
	TargetVectorCombination *TargetCombination
	Group                   *GroupParams
	ModuleParams            map[string]interface{}
	AdditionalProperties    additional.Properties
	ReplicationProperties   *additional.ReplicationProperties
	Tenant                  string
	IsRefOrigin             bool // is created by ref filter
}

type Embedding interface {
	[]float32 | [][]float32
}

func IsVectorEmpty(vector models.Vector) (bool, error) {
	switch v := vector.(type) {
	case nil:
		return true, nil
	case models.C11yVector:
		return len(v) == 0, nil
	case []float32:
		return len(v) == 0, nil
	case [][]float32:
		return len(v) == 0, nil
	default:
		return false, fmt.Errorf("unrecognized vector type: %T", vector)
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
