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
	"github.com/weaviate/weaviate/entities/storobj"
)

// Pins that a ref group-by emits each beacon as a plain string. Remote shards
// JSON-collapse strfmt.URI → string; if the grouper kept the named type,
// local and remote buckets for the same target would be unequal interface
// keys in ShardCombiner and split into two groups with halved counts.
func TestGrouper_RefBeaconEmittedAsString(t *testing.T) {
	const (
		beaconA = "weaviate://localhost/Animal/11111111-2222-3333-4444-555555555555"
		beaconB = "weaviate://localhost/Animal/66666666-7777-8888-9999-000000000000"
	)

	tests := []struct {
		name  string
		value func(t *testing.T) interface{}
	}{
		{
			// groupAll decodes through parseObject
			name: "unfiltered",
			value: func(t *testing.T) interface{} {
				return models.MultipleRef{{Beacon: strfmt.URI(beaconA)}, {Beacon: strfmt.URI(beaconB)}}
			},
		},
		{
			// groupFiltered decodes through docid.ScanObjectsLSM
			name: "filtered",
			value: func(t *testing.T) interface{} {
				props := map[string]interface{}{}
				require.NoError(t, storobj.UnmarshalProperties(
					[]byte(`{"hasAnimals":[{"beacon":"`+beaconA+`"},{"beacon":"`+beaconB+`"}]}`),
					props, [][]string{{"hasAnimals"}}))
				return props["hasAnimals"]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newGrouper(&Aggregator{
				params: aggregation.Params{
					GroupBy: &filters.Path{Property: "hasAnimals"},
				},
			}, 10)

			var schema models.PropertySchema = map[string]interface{}{"hasAnimals": tt.value(t)}
			require.NoError(t, g.addElementById(&schema, 0))

			keys := make([]interface{}, 0, len(g.values))
			for key := range g.values {
				keys = append(keys, key)
			}
			assert.ElementsMatch(t, []interface{}{beaconA, beaconB}, keys)
		})
	}
}
