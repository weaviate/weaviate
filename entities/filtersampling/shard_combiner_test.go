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

package filtersampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardCombiner_EmptyResults(t *testing.T) {
	combiner := NewShardCombiner()
	result := combiner.Do([]*Result{}, 10)

	require.NotNil(t, result)
	assert.Empty(t, result.Samples)
	assert.Equal(t, uint64(0), result.TotalObjects)
	assert.Equal(t, 0.0, result.EstimatedPercentCovered)
}

func TestShardCombiner_SingleShard(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: "apple", Cardinality: 100},
				{Value: "banana", Cardinality: 50},
				{Value: "cherry", Cardinality: 25},
			},
			TotalObjects:            1000,
			EstimatedPercentCovered: 17.5,
		},
	}

	result := combiner.Do(input, 10)

	require.NotNil(t, result)
	assert.Len(t, result.Samples, 3)
	assert.Equal(t, uint64(1000), result.TotalObjects)
	// Samples should be sorted by cardinality descending
	assert.Equal(t, "apple", result.Samples[0].Value)
	assert.Equal(t, uint64(100), result.Samples[0].Cardinality)
	assert.Equal(t, "banana", result.Samples[1].Value)
	assert.Equal(t, "cherry", result.Samples[2].Value)
}

func TestShardCombiner_MultipleShards_AggregateCardinalities(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: "apple", Cardinality: 100},
				{Value: "banana", Cardinality: 50},
			},
			TotalObjects: 500,
		},
		{
			Samples: []Sample{
				{Value: "apple", Cardinality: 80},
				{Value: "cherry", Cardinality: 30},
			},
			TotalObjects: 500,
		},
	}

	result := combiner.Do(input, 10)

	require.NotNil(t, result)
	assert.Len(t, result.Samples, 3)
	assert.Equal(t, uint64(1000), result.TotalObjects)

	// Apple should have combined cardinality of 180
	assert.Equal(t, "apple", result.Samples[0].Value)
	assert.Equal(t, uint64(180), result.Samples[0].Cardinality)

	// Banana should be second with 50
	assert.Equal(t, "banana", result.Samples[1].Value)
	assert.Equal(t, uint64(50), result.Samples[1].Cardinality)

	// Cherry should be third with 30
	assert.Equal(t, "cherry", result.Samples[2].Value)
	assert.Equal(t, uint64(30), result.Samples[2].Cardinality)
}

func TestShardCombiner_LimitSampleCount(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: "a", Cardinality: 100},
				{Value: "b", Cardinality: 90},
				{Value: "c", Cardinality: 80},
				{Value: "d", Cardinality: 70},
				{Value: "e", Cardinality: 60},
			},
			TotalObjects: 500,
		},
	}

	result := combiner.Do(input, 3)

	require.NotNil(t, result)
	assert.Len(t, result.Samples, 3)
	// Should return top 3 by cardinality
	assert.Equal(t, "a", result.Samples[0].Value)
	assert.Equal(t, "b", result.Samples[1].Value)
	assert.Equal(t, "c", result.Samples[2].Value)
}

func TestShardCombiner_NilResultsIgnored(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: "apple", Cardinality: 100},
			},
			TotalObjects: 500,
		},
		nil, // nil result should be ignored
		{
			Samples: []Sample{
				{Value: "banana", Cardinality: 50},
			},
			TotalObjects: 300,
		},
	}

	result := combiner.Do(input, 10)

	require.NotNil(t, result)
	assert.Len(t, result.Samples, 2)
	assert.Equal(t, uint64(800), result.TotalObjects)
}

func TestShardCombiner_PercentCoveredCalculation(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: "a", Cardinality: 100},
				{Value: "b", Cardinality: 100},
			},
			TotalObjects: 500,
		},
		{
			Samples: []Sample{
				{Value: "a", Cardinality: 100},
				{Value: "c", Cardinality: 100},
			},
			TotalObjects: 500,
		},
	}

	result := combiner.Do(input, 10)

	require.NotNil(t, result)
	assert.Equal(t, uint64(1000), result.TotalObjects)
	// Total covered: a=200, b=100, c=100 = 400
	// Percent: 400/1000 * 100 = 40%
	assert.Equal(t, 40.0, result.EstimatedPercentCovered)
}

func TestShardCombiner_NumericValues(t *testing.T) {
	combiner := NewShardCombiner()
	input := []*Result{
		{
			Samples: []Sample{
				{Value: int64(42), Cardinality: 100},
				{Value: int64(100), Cardinality: 50},
			},
			TotalObjects: 500,
		},
		{
			Samples: []Sample{
				{Value: int64(42), Cardinality: 80},
				{Value: int64(200), Cardinality: 30},
			},
			TotalObjects: 500,
		},
	}

	result := combiner.Do(input, 10)

	require.NotNil(t, result)
	assert.Len(t, result.Samples, 3)
	// int64(42) should have combined cardinality of 180
	assert.Equal(t, int64(42), result.Samples[0].Value)
	assert.Equal(t, uint64(180), result.Samples[0].Cardinality)
}
