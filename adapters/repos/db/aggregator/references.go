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

package aggregator

import (
	"sync"

	"github.com/weaviate/weaviate/entities/aggregation"
)

func addReferenceAggregations(prop *aggregation.Property,
	aggs []aggregation.Aggregator, agg *refAggregator,
) {
	prop.ReferenceAggregation = aggregation.Reference{}
	prop.ReferenceAggregation.PointingTo = agg.PointingTo()

	for _, aProp := range aggs {
		switch aProp {
		case aggregation.PointingToAggregator:
			prop.ReferenceAggregation.PointingTo = agg.PointingTo()
		default:
			continue
		}
	}
}

func newRefAggregator() *refAggregator {
	return &refAggregator{valueCounter: map[string]uint64{}}
}

type refAggregator struct {
	sync.Mutex
	valueCounter map[string]uint64
}

func (a *refAggregator) AddReference(beacon string) {
	a.Lock()
	defer a.Unlock()
	a.valueCounter[beacon]++
}

func (a *refAggregator) PointingTo() []string {
	keys := make([]string, 0, len(a.valueCounter))
	for pointingTo := range a.valueCounter {
		keys = append(keys, pointingTo)
	}
	return keys
}
