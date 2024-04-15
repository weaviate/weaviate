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

package nearImu

// extractNearIMUFn arguments, such as "imu" and "certainty"
func extractNearIMUFn(source map[string]interface{}) interface{} {
	var args NearIMUParams

	video, ok := source["imu"].(string)
	if ok {
		args.IMU = video
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

	// targetVectors is an optional argument, so it could be nil
	targetVectors, ok := source["targetVectors"]
	if ok {
		targetVectorsArray := targetVectors.([]interface{})
		args.TargetVectors = make([]string, len(targetVectorsArray))
		for i, value := range targetVectorsArray {
			args.TargetVectors[i] = value.(string)
		}
	}

	return &args
}
