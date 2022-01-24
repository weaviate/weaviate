//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package common_filters

import (
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// ExtractNearVector arguments, such as "vector" and "certainty"
func ExtractNearVector(source map[string]interface{}) traverser.NearVectorParams {
	var args traverser.NearVectorParams

	// vector is a required argument, so we don't need to check for its existing
	vector := source["vector"].([]interface{})
	args.Vector = make([]float32, len(vector))
	for i, value := range vector {
		args.Vector[i] = float32(value.(float64))
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	return args
}
