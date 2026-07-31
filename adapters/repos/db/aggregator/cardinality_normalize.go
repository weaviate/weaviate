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

import "github.com/weaviate/weaviate/entities/aggregation"

// NormalizeCardinalityOnlyProperties drops per-type aggregation data that a
// shard returned for a property the request asked cardinality-only for.
//
// Cluster-internal aggregation params travel as plain JSON, so a shard node on
// a pre-feature build silently drops ParamProperty.ApproximateCardinality and
// sees an ordinary zero-aggregator property, which it answers with a full
// per-type aggregation. Left alone, the combiner would merge those
// old-shards-only numbers into the reply as if they covered the collection.
// On a homogeneous cluster this is a no-op: an up-to-date shard node never
// sets a Type for such a property.
//
// Must run before ShardCombiner.Do — it mutates the per-shard results in place.
func NormalizeCardinalityOnlyProperties(props []aggregation.ParamProperty, results []*aggregation.Result) {
	var names []string
	for _, p := range props {
		if p.ApproximateCardinality && len(p.Aggregators) == 0 {
			names = append(names, p.Name.String())
		}
	}
	if len(names) == 0 {
		return
	}

	for _, res := range results {
		if res == nil {
			continue
		}
		for gi := range res.Groups {
			for _, name := range names {
				prop, ok := res.Groups[gi].Properties[name]
				if !ok {
					continue
				}
				res.Groups[gi].Properties[name] = aggregation.Property{
					ApproximateCardinality: prop.ApproximateCardinality,
				}
			}
		}
	}
}
