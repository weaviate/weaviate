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

// NormalizeCardinalityOnlyProperties drops per-type aggregation data returned
// for a cardinality-only property. A shard node that does not know the flag
// answers with a full per-type aggregation, which the combiner would otherwise
// merge in as authoritative.
//
// Mutates the per-shard results in place; must run before ShardCombiner.Do.
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
