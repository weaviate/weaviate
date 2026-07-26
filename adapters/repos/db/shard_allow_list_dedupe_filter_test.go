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

package db

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// roundTripFilter puts a filter through the real cluster-internal marshalling
// a remote shard receives, producing the independent-deserialization shape
// sameFilter has to compare correctly.
func roundTripFilter(t *testing.T, f *filters.LocalFilter) *filters.LocalFilter {
	t.Helper()
	payload, err := shared.IndicesPayloads.SearchParams.Marshal(
		[]models.Vector{[]float32{1, 2, 3}}, []string{""}, 0, 10, f,
		nil, nil, nil, nil, additional.Properties{}, &dto.TargetCombination{}, nil, nil)
	require.NoError(t, err)

	_, _, _, _, out, _, _, _, _, _, _, _, _, err := shared.IndicesPayloads.SearchParams.Unmarshal(payload)
	require.NoError(t, err)
	return out
}

func leaf(op filters.Operator, prop string, value *filters.Value) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: op,
		On:       &filters.Path{Class: "Thing", Property: schema.PropertyName(prop)},
		Value:    value,
	}}
}

// TestSameFilterSurvivesTheWire pins that every shape below round-trips to an
// equal filter. Measured on a 4-node rig: Date and GeoCoordinates were suspected
// to break the reflective comparison after marshalling and didn't —
// Value.UnmarshalJSON restores the concrete type from Value.Type.
//
// Kept exhaustive so a future serialization change can't quietly turn dedupe
// into a no-op, which would only show up as filter_mismatch rising.
func TestSameFilterSurvivesTheWire(t *testing.T) {
	geo := func(lat, lon float32) *filters.GeoRange {
		return &filters.GeoRange{
			GeoCoordinates: &models.GeoCoordinates{Latitude: &lat, Longitude: &lon},
			Distance:       500,
		}
	}
	when := time.Date(2026, 7, 26, 9, 30, 0, 0, time.UTC)

	tests := []struct {
		name  string
		build func() *filters.LocalFilter
		// other, when non-nil, must NOT compare equal to build()'s filter.
		other func() *filters.LocalFilter
	}{
		{
			name: "text equal",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorEqual, "name", &filters.Value{Value: "alpha", Type: schema.DataTypeText})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorEqual, "name", &filters.Value{Value: "beta", Type: schema.DataTypeText})
			},
		},
		{
			name: "int greater than",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "score", &filters.Value{Value: 100, Type: schema.DataTypeInt})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "score", &filters.Value{Value: 101, Type: schema.DataTypeInt})
			},
		},
		{
			name: "number less than equal",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorLessThanEqual, "ratio", &filters.Value{Value: 3.25, Type: schema.DataTypeNumber})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorLessThanEqual, "ratio", &filters.Value{Value: 3.5, Type: schema.DataTypeNumber})
			},
		},
		{
			name: "boolean equal",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorEqual, "available", &filters.Value{Value: true, Type: schema.DataTypeBoolean})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorEqual, "available", &filters.Value{Value: false, Type: schema.DataTypeBoolean})
			},
		},
		{
			name: "date greater than",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: when, Type: schema.DataTypeDate})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: when.Add(time.Hour), Type: schema.DataTypeDate})
			},
		},
		{
			name: "date carrying a monotonic reading and a non-UTC location",
			// Monotonic reading and non-UTC *Location don't survive marshalling; both
			// legs lose them identically, so this still compares equal.
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: when.In(time.FixedZone("CEST", 2*3600)), Type: schema.DataTypeDate})
			},
		},
		{
			name: "date as string",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: "2026-07-26T09:30:00Z", Type: schema.DataTypeDate})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: "2026-07-27T09:30:00Z", Type: schema.DataTypeDate})
			},
		},
		{
			name: "uuid equal",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorEqual, "id", &filters.Value{Value: strfmt.UUID("5f5b2b0e-0f4a-4e3d-9a1f-2c3d4e5f6071"), Type: schema.DataTypeUUID})
			},
		},
		{
			name: "geo coordinates within range",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorWithinGeoRange, "location", &filters.Value{Value: geo(52.37, 4.89), Type: schema.DataTypeGeoCoordinates})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.OperatorWithinGeoRange, "location", &filters.Value{Value: geo(48.85, 2.35), Type: schema.DataTypeGeoCoordinates})
			},
		},
		{
			name: "text array contains any",
			build: func() *filters.LocalFilter {
				return leaf(filters.ContainsAny, "tags", &filters.Value{Value: []string{"a", "b"}, Type: schema.DataTypeText})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.ContainsAny, "tags", &filters.Value{Value: []string{"a", "c"}, Type: schema.DataTypeText})
			},
		},
		{
			name: "int array contains all",
			build: func() *filters.LocalFilter {
				return leaf(filters.ContainsAll, "codes", &filters.Value{Value: []int{1, 2, 3}, Type: schema.DataTypeInt})
			},
			other: func() *filters.LocalFilter {
				return leaf(filters.ContainsAll, "codes", &filters.Value{Value: []int{1, 2, 4}, Type: schema.DataTypeInt})
			},
		},
		{
			name: "is null",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorIsNull, "note", &filters.Value{Value: true, Type: schema.DataTypeBoolean})
			},
		},
		{
			name: "like",
			build: func() *filters.LocalFilter {
				return leaf(filters.OperatorLike, "name", &filters.Value{Value: "alph*", Type: schema.DataTypeText})
			},
		},
		{
			name: "nested path through a reference",
			build: func() *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class: "Thing", Property: "ofBrand",
						Child: &filters.Path{Class: "Brand", Property: "name"},
					},
					Value: &filters.Value{Value: "acme", Type: schema.DataTypeText},
				}}
			},
		},
		{
			name: "production shape: Or over four predicates of mixed type",
			build: func() *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						*leaf(filters.OperatorEqual, "available", &filters.Value{Value: true, Type: schema.DataTypeBoolean}).Root,
						*leaf(filters.OperatorGreaterThan, "score", &filters.Value{Value: 100, Type: schema.DataTypeInt}).Root,
						*leaf(filters.OperatorEqual, "currency", &filters.Value{Value: "EUR", Type: schema.DataTypeText}).Root,
						*leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: when, Type: schema.DataTypeDate}).Root,
					},
				}}
			},
			other: func() *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						*leaf(filters.OperatorEqual, "available", &filters.Value{Value: true, Type: schema.DataTypeBoolean}).Root,
						*leaf(filters.OperatorGreaterThan, "score", &filters.Value{Value: 100, Type: schema.DataTypeInt}).Root,
						*leaf(filters.OperatorEqual, "currency", &filters.Value{Value: "EUR", Type: schema.DataTypeText}).Root,
						*leaf(filters.OperatorGreaterThan, "created", &filters.Value{Value: when, Type: schema.DataTypeDate}).Root,
					},
				}}
			},
		},
		{
			name: "nested And of Ors",
			build: func() *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorAnd,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorOr,
							Operands: []filters.Clause{
								*leaf(filters.OperatorEqual, "a", &filters.Value{Value: "x", Type: schema.DataTypeText}).Root,
								*leaf(filters.OperatorEqual, "b", &filters.Value{Value: "y", Type: schema.DataTypeText}).Root,
							},
						},
						*leaf(filters.OperatorGreaterThanEqual, "n", &filters.Value{Value: 7, Type: schema.DataTypeInt}).Root,
					},
				}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legA := roundTripFilter(t, tt.build())
			legB := roundTripFilter(t, tt.build())
			require.NotNil(t, legA)
			require.NotSame(t, legA, legB, "the two legs must be independent trees")

			assert.True(t, sameFilter(legA, legB),
				"two deserializations of one filter must compare equal, or dedupe never fires multi-node")

			// The local shape, where both legs share one pointer.
			local := tt.build()
			assert.True(t, sameFilter(local, local))

			if tt.other != nil {
				assert.False(t, sameFilter(legA, roundTripFilter(t, tt.other())),
					"different filters must not share a build")
			}
		})
	}
}
