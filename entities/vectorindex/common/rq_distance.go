//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import "fmt"

// RQSupportsDistance reports whether rotational quantization can compress
// vectors under distance. Every bit width (1, 4 and 8, centered or not, hnsw
// and flat) shares one gate, compressionhelpers.distancerIndicatorsAndError,
// which accepts cosine, dot and l2-squared.
func RQSupportsDistance(distance string) bool {
	switch distance {
	case DistanceCosine, DistanceDot, DistanceL2Squared:
		return true
	}
	return false
}

// ValidateRQDistance rejects enabled RQ on a distance the quantizers cannot
// compress. Accepted, such a config fails at activation (1-bit: retried on
// every queue tick) or on every distance call (4- and 8-bit). It applies to
// schema writes only: persisted configs are re-parsed on startup and restore,
// and an existing class must keep loading.
func ValidateRQDistance(rqEnabled bool, distance string) error {
	if !rqEnabled || RQSupportsDistance(distance) {
		return nil
	}
	return fmt.Errorf("RQ does not support distance %q, use cosine, dot or l2-squared", distance)
}
