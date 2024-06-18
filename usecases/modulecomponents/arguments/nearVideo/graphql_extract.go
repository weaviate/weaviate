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

package nearVideo

import "github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"

// extractNearVideoFn arguments, such as "video" and "certainty"
func extractNearVideoFn(source map[string]interface{}) interface{} {
	var args NearVideoParams

	video, ok := source["video"].(string)
	if ok {
		args.Video = video
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	distance, ok := source["distance"]
	if ok {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	targetVectors, combination, _ := common_filters.ExtractTargets(source)
	args.TargetVectors = targetVectors
	args.targetCombination = combination

	return &args
}
