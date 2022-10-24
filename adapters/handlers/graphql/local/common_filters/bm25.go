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

// ExtractBM25
func ExtractBM25(source map[string]interface{}) searchparams.KeywordRanking {
	var args searchparams.KeywordRanking

	p, ok := source["properties"]
	if ok {
		rawSlice := p.([]interface{})
		args.Properties = make([]string, len(rawSlice))
		for i, raw := range rawSlice {
			args.Properties[i] = raw.(string)
		}
	}

	
	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	args.Type = "bm25"

	return args
}
