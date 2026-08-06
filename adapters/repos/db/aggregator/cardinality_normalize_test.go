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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
)

func cardEst(v uint32) *uint32 { return &v }

// upToDateShard is what a shard node that understands the flag returns for a
// cardinality-only property: an estimate and no type.
func upToDateShard(est uint32) *aggregation.Result {
	return &aggregation.Result{Groups: []aggregation.Group{{
		Count: 3,
		Properties: map[string]aggregation.Property{
			"name": {ApproximateCardinality: cardEst(est)},
		},
	}}}
}

// preFeatureShard is what a shard node that dropped the flag from the JSON
// payload returns: a full text aggregation covering only its own objects.
func preFeatureShard() *aggregation.Result {
	return &aggregation.Result{Groups: []aggregation.Group{{
		Count: 2,
		Properties: map[string]aggregation.Property{
			"name": {
				Type:       aggregation.PropertyTypeText,
				SchemaType: "text",
				TextAggregation: aggregation.Text{
					Count: 2,
					Items: []aggregation.TextOccurrence{{Value: "alpha", Occurs: 2}},
				},
			},
		},
	}}}
}

func TestNormalizeCardinalityOnlyProperties(t *testing.T) {
	tests := []struct {
		name    string
		props   []aggregation.ParamProperty
		results []*aggregation.Result
		want    []*aggregation.Result
	}{
		{
			name: "cardinality-only property stripped, estimate kept, sibling untouched",
			props: []aggregation.ParamProperty{
				{Name: "name", ApproximateCardinality: true},
				{Name: "city"},
			},
			results: []*aggregation.Result{{Groups: []aggregation.Group{{
				Count: 2,
				Properties: map[string]aggregation.Property{
					"name": {
						Type: aggregation.PropertyTypeText, SchemaType: "text",
						TextAggregation:        aggregation.Text{Count: 2},
						ApproximateCardinality: cardEst(4),
					},
					"city": {Type: aggregation.PropertyTypeText, TextAggregation: aggregation.Text{Count: 2}},
				},
			}}}},
			want: []*aggregation.Result{{Groups: []aggregation.Group{{
				Count: 2,
				Properties: map[string]aggregation.Property{
					"name": {ApproximateCardinality: cardEst(4)},
					"city": {Type: aggregation.PropertyTypeText, TextAggregation: aggregation.Text{Count: 2}},
				},
			}}}},
		},
		{
			name: "property that also requested aggregators keeps its aggregation",
			props: []aggregation.ParamProperty{{
				Name:                   "name",
				Aggregators:            []aggregation.Aggregator{aggregation.CountAggregator},
				ApproximateCardinality: true,
			}},
			results: []*aggregation.Result{preFeatureShard()},
			want:    []*aggregation.Result{preFeatureShard()},
		},
		{
			name:    "nil shard result and missing property are skipped",
			props:   []aggregation.ParamProperty{{Name: "name", ApproximateCardinality: true}},
			results: []*aggregation.Result{nil, {Groups: []aggregation.Group{{Count: 1}}}},
			want:    []*aggregation.Result{nil, {Groups: []aggregation.Group{{Count: 1}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			NormalizeCardinalityOnlyProperties(tt.props, tt.results)
			assert.Equal(t, tt.want, tt.results)
		})
	}
}

// Pins the reason the strip exists: without it the combiner presents a
// pre-feature shard's partial text aggregation as the collection-wide answer.
func TestNormalizeCardinalityOnlyPropertiesBeforeCombine(t *testing.T) {
	props := []aggregation.ParamProperty{{Name: "name", ApproximateCardinality: true}}

	leaked := NewShardCombiner().Do([]*aggregation.Result{upToDateShard(9), preFeatureShard()})
	require.Len(t, leaked.Groups, 1)
	assert.Equal(t, aggregation.PropertyTypeText, leaked.Groups[0].Properties["name"].Type,
		"without normalization the pre-feature shard's type wins")

	results := []*aggregation.Result{upToDateShard(9), preFeatureShard()}
	NormalizeCardinalityOnlyProperties(props, results)
	combined := NewShardCombiner().Do(results)

	require.Len(t, combined.Groups, 1)
	assert.Equal(t, 5, combined.Groups[0].Count)
	prop := combined.Groups[0].Properties["name"]
	assert.Equal(t, aggregation.PropertyType(""), prop.Type)
	assert.Zero(t, prop.TextAggregation.Count)
	assert.Empty(t, prop.TextAggregation.Items)
	require.NotNil(t, prop.ApproximateCardinality)
	assert.Equal(t, uint32(9), *prop.ApproximateCardinality)
}
