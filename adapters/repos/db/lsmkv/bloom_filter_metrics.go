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

package lsmkv

type bloomFilterMetrics struct {
	trueNegative  TimeObserver
	falsePositive TimeObserver
	truePositive  TimeObserver
}

// newBloomFilterMetrics curries the prometheus metrics just once at
// initialization to prevent further allocs on the hot path
func newBloomFilterMetrics(metrics *Metrics) *bloomFilterMetrics {
	return &bloomFilterMetrics{
		trueNegative:  metrics.BloomFilterObserver("replace", "get_true_negative"),
		falsePositive: metrics.BloomFilterObserver("replace", "get_false_positive"),
		truePositive:  metrics.BloomFilterObserver("replace", "get_true_positive"),
	}
}
