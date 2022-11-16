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

import "github.com/semi-technologies/weaviate/entities/searchparams"

// ExtractHybrid
func ExtractHybrid(source map[string]interface{}) searchparams.HybridSearch {
	var args searchparams.HybridSearch

	alpha, ok := source["alpha"]
	if ok {
		args.Alpha = alpha.(float64)
	}

	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	if _, ok := source["vector"]; ok {
		vector := source["vector"].([]interface{})
		args.Vector = make([]float32, len(vector))
		for i, value := range vector {
			args.Vector[i] = float32(value.(float64))
		}
	}

	args.Type = "hybrid"

	return args
}
