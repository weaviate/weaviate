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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

// Pins that a ref group-by emits its beacon as a plain string. The grouped
// value is an interface{} that JSON-collapses strfmt.URI → string when it
// crosses shard→coordinator on multi-node clusters. If the grouper emitted
// the named strfmt.URI type instead, a local shard's bucket and a remote
// shard's bucket for the same target would be unequal interface keys in
// ShardCombiner.getPosOfGroup and split into two groups with halved counts.
func TestGrouper_RefBeaconEmittedAsString(t *testing.T) {
	const beacon = "weaviate://localhost/Animal/11111111-2222-3333-4444-555555555555"
	g := newGrouper(&Aggregator{
		params: aggregation.Params{
			GroupBy: &filters.Path{Property: "hasAnimals"},
		},
	}, 10)

	var schema models.PropertySchema = map[string]interface{}{
		"hasAnimals": models.MultipleRef{{Beacon: strfmt.URI(beacon)}},
	}
	require.NoError(t, g.addElementById(&schema, 0))

	require.Len(t, g.values, 1)
	for key := range g.values {
		_, ok := key.(string)
		assert.Truef(t, ok, "ref group-by key must be string, got %T", key)
		assert.Equal(t, beacon, key)
	}
}
