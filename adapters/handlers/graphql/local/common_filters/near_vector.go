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

	"github.com/weaviate/weaviate/entities/searchparams"
)

// ExtractNearVector arguments, such as "vector" and "distance"
func ExtractNearVector(source map[string]interface{}) (searchparams.NearVector, error) {
	var args searchparams.NearVector

	// vector is a required argument, so we don't need to check for its existing
	vector := source["vector"].([]interface{})
	args.Vector = make([]float32, len(vector))
	for i, value := range vector {
		args.Vector[i] = float32(value.(float64))
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
		return searchparams.NearVector{},
			fmt.Errorf("cannot provide distance and certainty")
	}

	return args, nil
}
