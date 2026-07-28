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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// A local shard reaches the grouper with models.MultipleRef, whose Beacon is a
// strfmt.URI; a remote shard's reply has been through JSON and arrives as a
// plain string. g.values is keyed by the boxed interface, and the two are
// unequal despite identical bytes — so one ref target used to land in two
// groups, each carrying part of the count.
func TestGrouperRefBeaconMergesAcrossShardShapes(t *testing.T) {
	const beacon = "weaviate://localhost/SomeClass/6c2f5a1e-0000-4000-8000-000000000001"

	g := &grouper{
		Aggregator: &Aggregator{
			params: aggregation.Params{
				GroupBy: &filters.Path{Property: schema.PropertyName("refProp")},
			},
		},
		values: map[interface{}]map[uint64]struct{}{},
	}

	local := models.PropertySchema(map[string]interface{}{
		"refProp": models.MultipleRef{{Beacon: strfmt.URI(beacon)}},
	})
	require.NoError(t, g.addElementById(&local, 1))

	// What a JSON-decoded remote shard reply yields for the same target.
	g.addItem(beacon, 2)

	require.Len(t, g.values, 1,
		"the same beacon from a local and a remote shard must group together, not split the count across two buckets")
	require.Len(t, g.values[beacon], 2, "both doc IDs belong to the single merged group")
}
