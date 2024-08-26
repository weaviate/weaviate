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

package ask

import "github.com/weaviate/weaviate/entities/dto"

func (g *GraphQLArgumentsProvider) extractAskFn(source map[string]interface{}) (interface{}, *dto.TargetCombination, error) {
	var args AskParams

	question, ok := source["question"].(string)
	if ok {
		args.Question = question
	}

	properties, ok := source["properties"].([]interface{})
	if ok {
		args.Properties = make([]string, len(properties))
		for i, value := range properties {
			args.Properties[i] = value.(string)
		}
	}

	return &args, nil, nil
}
