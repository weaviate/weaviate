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

package common_filters

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// ExtractNearVector arguments, such as "vector" and "distance"
func ExtractNearVector(source map[string]interface{}, targetVectorsFromOtherLevel []string) (searchparams.NearVector, *dto.TargetCombination, error) {
	var args searchparams.NearVector

	vectorGQL, okVec := source["vector"]
	vectorPerTarget, okVecPerTarget := source["vectorPerTarget"].(map[string]interface{})
	if (!okVec && !okVecPerTarget) || (okVec && okVecPerTarget) {
		return searchparams.NearVector{}, nil,
			fmt.Errorf("vector or vectorPerTarget is required field")
	}

	certainty, certaintyOK := source["certainty"]
	if certaintyOK {
		args.Certainty = certainty.(float64)
	}

	distance, distanceOK := source["distance"]
	if distanceOK {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	if certaintyOK && distanceOK {
		return searchparams.NearVector{}, nil,
			fmt.Errorf("cannot provide distance and certainty")
	}

	var targetVectors []string
	var combination *dto.TargetCombination
	if targetVectorsFromOtherLevel == nil {
		var err error
		targetVectors, combination, err = ExtractTargets(source)
		if err != nil {
			return searchparams.NearVector{}, nil, err
		}
		args.TargetVectors = targetVectors
	} else {
		targetVectors = targetVectorsFromOtherLevel
	}

	if okVec {
		if len(targetVectors) == 0 {
			args.Vectors = []models.Vector{vectorGQL}
		} else {
			args.Vectors = make([]models.Vector, len(targetVectors))
			for i := range targetVectors {
				args.Vectors[i] = vectorGQL
			}
		}
	}

	if okVecPerTarget {
		var vectors []models.Vector
		// needs to handle the case of targetVectors being empty (if you only provide a near vector with targets)
		if len(targetVectors) == 0 {
			targets := make([]string, 0, len(vectorPerTarget))
			vectors = make([]models.Vector, 0, len(vectorPerTarget))

			for target := range vectorPerTarget {
				single, ok := vectorPerTarget[target].([]float32)
				if ok {
					vectors = append(vectors, single)
					targets = append(targets, target)
				} else {
					if normalVectors, ok := vectorPerTarget[target].([][]float32); ok {
						for j := range normalVectors {
							vectors = append(vectors, normalVectors[j])
							targets = append(targets, target)
						}
					} else if multiVectors, ok := vectorPerTarget[target].([][][]float32); ok {
						// NOTE the type of multiVectors is [][][]float32 (vs normalVectors which is [][]float32),
						// so there are two similar loops here to handle the different types, if there is a simpler
						// way to handle this, feel free to change it
						for j := range multiVectors {
							vectors = append(vectors, multiVectors[j])
							targets = append(targets, target)
						}
					} else {
						return searchparams.NearVector{}, nil,
							fmt.Errorf(
								"vectorPerTarget should be a map with strings as keys and a normal vector, list of vectors, "+
									"or list of multi-vectors as values. Received %T", vectorPerTarget[target])
					}
				}
			}
			args.TargetVectors = targets
		} else {
			// map provided targetVectors to the provided searchvectors
			vectors = make([]models.Vector, len(targetVectors))
			handled := make(map[string]struct{})
			for i, target := range targetVectors {
				if _, ok := handled[target]; ok {
					continue
				} else {
					handled[target] = struct{}{}
				}
				vectorPerTargetParsed, ok := vectorPerTarget[target]
				if !ok {
					return searchparams.NearVector{}, nil, fmt.Errorf("vectorPerTarget for target %s is not provided", target)
				}
				if vectorIn, ok := vectorPerTargetParsed.([]float32); ok {
					vectors[i] = vectorIn
				} else if vectorsIn, ok := vectorPerTargetParsed.([][]float32); ok {
					// if one target vector has multiple search vectors, the target vector needs to be repeated multiple times
					for j, w := range vectorsIn {
						if !targetVectorOrderMatches(i, j, targetVectors, target) {
							return searchparams.NearVector{}, nil, fmt.Errorf("target %s is not in the correct order", target)
						}
						vectors[i+j] = w
					}
				} else if multiVectorsIn, ok := vectorPerTargetParsed.([][][]float32); ok {
					// NOTE the type of multiVectorsIn is [][][]float32 (vs vectorsIn which is [][]float32),
					// so there are two similar loops here to handle the different types, if there is a simpler
					// way to handle this, feel free to change it
					for j, w := range multiVectorsIn {
						if !targetVectorOrderMatches(i, j, targetVectors, target) {
							return searchparams.NearVector{}, nil, fmt.Errorf("multivector target %s is not in the correct order", target)
						}
						vectors[i+j] = w
					}
				} else {
					return searchparams.NearVector{}, nil, fmt.Errorf("could not handle type of near vector for target %s, got %v", target, vectorPerTargetParsed)
				}
			}
		}
		args.Vectors = vectors
	}

	return args, combination, nil
}

func targetVectorOrderMatches(i, j int, targetVectors []string, target string) bool {
	return i+j < len(targetVectors) && targetVectors[i+j] == target
}
