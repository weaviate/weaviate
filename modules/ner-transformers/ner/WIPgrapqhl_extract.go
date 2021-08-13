//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ask

func extractAskFn(source map[string]interface{}) interface{} {
	var args AskParams

	question, ok := source["question"].(string)
	if ok {
		args.Question = question
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	properties, ok := source["properties"].([]interface{})
	if ok {
		args.Properties = make([]string, len(properties))
		for i, value := range properties {
			args.Properties[i] = value.(string)
		}
	}

	return &args
}
