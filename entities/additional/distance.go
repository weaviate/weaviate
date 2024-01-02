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

package additional

func CertaintyToDistPtr(maybeCertainty *float64) (distPtr *float64) {
	if maybeCertainty != nil {
		dist := (1 - *maybeCertainty) * 2
		distPtr = &dist
	}
	return
}

func CertaintyToDist(certainty float64) (dist float64) {
	dist = (1 - certainty) * 2
	return
}

func DistToCertainty(dist float64) (certainty float64) {
	certainty = 1 - (dist / 2)
	return
}
