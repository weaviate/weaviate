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

package nearAudio

import "github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"

// extractNearAudioFn arguments, such as "audio" and "certainty"
func extractNearAudioFn(source map[string]interface{}) interface{} {
	var args nearAudio.NearAudioParams

	audio, ok := source["audio"].(string)
	if ok {
		args.Audio = audio
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

	return &args
}
