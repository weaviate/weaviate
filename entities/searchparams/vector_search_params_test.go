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

package searchparams

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorSearchParams_ForTarget(t *testing.T) {
	requestWide := VectorSearchParams{EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn}

	tests := []struct {
		name   string
		params *VectorSearchParams
		target string
		want   *VectorSearchParams
		same   bool // result must be the receiver itself
	}{
		{
			name:   "nil receiver",
			params: nil,
			target: "title",
			want:   nil,
		},
		{
			name:   "no per-target map returns the receiver",
			params: &requestWide,
			target: "title",
			want:   &requestWide,
			same:   true,
		},
		{
			name: "map without an entry for the target strips the map",
			params: &VectorSearchParams{
				EF: 64, PerTarget: map[string]VectorSearchParams{"body": {EF: 8}},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 64},
		},
		{
			name: "entry overrides ef only",
			params: &VectorSearchParams{
				EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorSearchParams{"title": {EF: 128}},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 128, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn},
		},
		{
			name: "entry overrides search probe only",
			params: &VectorSearchParams{
				EF: 64, SearchProbe: 16,
				PerTarget: map[string]VectorSearchParams{"body": {SearchProbe: 32}},
			},
			target: "body",
			want:   &VectorSearchParams{EF: 64, SearchProbe: 32},
		},
		{
			name: "entry overrides filter strategy only",
			params: &VectorSearchParams{
				EF: 64, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorSearchParams{"title": {FilterStrategy: FilterStrategyPathseer}},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 64, FilterStrategy: FilterStrategyPathseer},
		},
		{
			name: "entry overrides every field",
			params: &VectorSearchParams{
				EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorSearchParams{
					"title": {EF: 8, SearchProbe: 4, FilterStrategy: FilterStrategySweeping},
				},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 8, SearchProbe: 4, FilterStrategy: FilterStrategySweeping},
		},
		{
			name: "per-target only, no request-wide values",
			params: &VectorSearchParams{
				PerTarget: map[string]VectorSearchParams{"title": {EF: 128}},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 128},
		},
		{
			name: "zero entry keeps request-wide values",
			params: &VectorSearchParams{
				EF: 64, PerTarget: map[string]VectorSearchParams{"title": {}},
			},
			target: "title",
			want:   &VectorSearchParams{EF: 64},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.params.ForTarget(tt.target)
			assert.Equal(t, tt.want, got)
			if tt.same {
				assert.Same(t, tt.params, got)
			}
			if got != nil {
				assert.Nil(t, got.PerTarget, "ForTarget must never return a PerTarget map")
			}
		})
	}
}

func TestVectorSearchParams_Accessors(t *testing.T) {
	var nilParams *VectorSearchParams
	assert.Equal(t, 0, nilParams.EFOrZero())
	assert.Equal(t, 0, nilParams.SearchProbeOrZero())
	assert.Equal(t, "", nilParams.FilterStrategyOrEmpty())
	assert.True(t, nilParams.IsZero())

	assert.True(t, (&VectorSearchParams{}).IsZero())
	assert.False(t, (&VectorSearchParams{EF: 1}).IsZero())
	assert.False(t, (&VectorSearchParams{SearchProbe: 1}).IsZero())
	assert.False(t, (&VectorSearchParams{FilterStrategy: FilterStrategySweeping}).IsZero())
	assert.False(t, (&VectorSearchParams{PerTarget: map[string]VectorSearchParams{"t": {EF: 1}}}).IsZero())

	p := &VectorSearchParams{EF: 3, SearchProbe: 5, FilterStrategy: FilterStrategyPathseer}
	assert.Equal(t, 3, p.EFOrZero())
	assert.Equal(t, 5, p.SearchProbeOrZero())
	assert.Equal(t, FilterStrategyPathseer, p.FilterStrategyOrEmpty())
}

func TestVectorSearchParams_JSONRoundTrip(t *testing.T) {
	t.Run("full struct round-trips", func(t *testing.T) {
		in := VectorSearchParams{
			EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
			PerTarget: map[string]VectorSearchParams{
				"title": {EF: 128, FilterStrategy: FilterStrategySweeping},
				"body":  {SearchProbe: 32},
			},
		}
		raw, err := json.Marshal(in)
		require.NoError(t, err)
		var out VectorSearchParams
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, in, out)
	})

	t.Run("zero fields are omitted", func(t *testing.T) {
		raw, err := json.Marshal(VectorSearchParams{EF: 64})
		require.NoError(t, err)
		assert.JSONEq(t, `{"ef":64}`, string(raw))
	})

	t.Run("old ef-only payload decodes into the new struct", func(t *testing.T) {
		var out VectorSearchParams
		require.NoError(t, json.Unmarshal([]byte(`{"ef":64}`), &out))
		assert.Equal(t, VectorSearchParams{EF: 64}, out)
	})

	t.Run("new payload decoded by a node that only knows ef keeps ef", func(t *testing.T) {
		// Shape of the struct on a node that predates the new fields.
		type oldVectorSearchParams struct {
			EF int `json:"ef,omitempty"`
		}
		raw, err := json.Marshal(VectorSearchParams{
			EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
			PerTarget: map[string]VectorSearchParams{"title": {EF: 128}},
		})
		require.NoError(t, err)
		var out oldVectorSearchParams
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, oldVectorSearchParams{EF: 64}, out)
	})
}
