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

func TestVectorIndexParams_ForTarget(t *testing.T) {
	requestWide := VectorIndexParams{EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn}

	tests := []struct {
		name   string
		params *VectorIndexParams
		target string
		want   *VectorIndexParams
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
			params: &VectorIndexParams{
				EF: 64, PerTarget: map[string]VectorIndexParams{"body": {EF: 8}},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 64},
		},
		{
			name: "entry overrides ef only",
			params: &VectorIndexParams{
				EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorIndexParams{"title": {EF: 128}},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 128, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn},
		},
		{
			name: "entry overrides search probe only",
			params: &VectorIndexParams{
				EF: 64, SearchProbe: 16,
				PerTarget: map[string]VectorIndexParams{"body": {SearchProbe: 32}},
			},
			target: "body",
			want:   &VectorIndexParams{EF: 64, SearchProbe: 32},
		},
		{
			name: "entry overrides filter strategy only",
			params: &VectorIndexParams{
				EF: 64, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorIndexParams{"title": {FilterStrategy: FilterStrategyPathseer}},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 64, FilterStrategy: FilterStrategyPathseer},
		},
		{
			name: "entry overrides every field",
			params: &VectorIndexParams{
				EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
				PerTarget: map[string]VectorIndexParams{
					"title": {EF: 8, SearchProbe: 4, FilterStrategy: FilterStrategySweeping},
				},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 8, SearchProbe: 4, FilterStrategy: FilterStrategySweeping},
		},
		{
			name: "per-target only, no request-wide values",
			params: &VectorIndexParams{
				PerTarget: map[string]VectorIndexParams{"title": {EF: 128}},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 128},
		},
		{
			name: "zero entry keeps request-wide values",
			params: &VectorIndexParams{
				EF: 64, PerTarget: map[string]VectorIndexParams{"title": {}},
			},
			target: "title",
			want:   &VectorIndexParams{EF: 64},
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

func TestVectorIndexParams_Accessors(t *testing.T) {
	var nilParams *VectorIndexParams
	assert.Equal(t, 0, nilParams.EFOrZero())
	assert.Equal(t, 0, nilParams.SearchProbeOrZero())
	assert.Equal(t, "", nilParams.FilterStrategyOrEmpty())
	assert.True(t, nilParams.IsZero())

	assert.True(t, (&VectorIndexParams{}).IsZero())
	assert.False(t, (&VectorIndexParams{EF: 1}).IsZero())
	assert.False(t, (&VectorIndexParams{SearchProbe: 1}).IsZero())
	assert.False(t, (&VectorIndexParams{FilterStrategy: FilterStrategySweeping}).IsZero())
	assert.False(t, (&VectorIndexParams{PerTarget: map[string]VectorIndexParams{"t": {EF: 1}}}).IsZero())

	p := &VectorIndexParams{EF: 3, SearchProbe: 5, FilterStrategy: FilterStrategyPathseer}
	assert.Equal(t, 3, p.EFOrZero())
	assert.Equal(t, 5, p.SearchProbeOrZero())
	assert.Equal(t, FilterStrategyPathseer, p.FilterStrategyOrEmpty())
}

func TestVectorIndexParams_JSONRoundTrip(t *testing.T) {
	t.Run("full struct round-trips", func(t *testing.T) {
		in := VectorIndexParams{
			EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
			PerTarget: map[string]VectorIndexParams{
				"title": {EF: 128, FilterStrategy: FilterStrategySweeping},
				"body":  {SearchProbe: 32},
			},
		}
		raw, err := json.Marshal(in)
		require.NoError(t, err)
		var out VectorIndexParams
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, in, out)
	})

	t.Run("zero fields are omitted", func(t *testing.T) {
		raw, err := json.Marshal(VectorIndexParams{EF: 64})
		require.NoError(t, err)
		assert.JSONEq(t, `{"ef":64}`, string(raw))
	})

	t.Run("old ef-only payload decodes into the new struct", func(t *testing.T) {
		var out VectorIndexParams
		require.NoError(t, json.Unmarshal([]byte(`{"ef":64}`), &out))
		assert.Equal(t, VectorIndexParams{EF: 64}, out)
	})

	t.Run("new payload decoded by a node that only knows ef keeps ef", func(t *testing.T) {
		// Shape of the struct on a node that predates the new fields.
		type oldVectorIndexParams struct {
			EF int `json:"ef,omitempty"`
		}
		raw, err := json.Marshal(VectorIndexParams{
			EF: 64, SearchProbe: 16, FilterStrategy: FilterStrategyAcorn,
			PerTarget: map[string]VectorIndexParams{"title": {EF: 128}},
		})
		require.NoError(t, err)
		var out oldVectorIndexParams
		require.NoError(t, json.Unmarshal(raw, &out))
		assert.Equal(t, oldVectorIndexParams{EF: 64}, out)
	})
}

func TestVectorIndexParams_Validate(t *testing.T) {
	tests := []struct {
		name    string
		params  *VectorIndexParams
		names   FieldNames // JSONFieldNames when zero
		maxEF   int
		maxProb int
		wantErr string
	}{
		{name: "nil", params: nil, maxEF: 10, maxProb: 10},
		{name: "within caps", params: &VectorIndexParams{EF: 10, SearchProbe: 10, FilterStrategy: FilterStrategyAcorn}, maxEF: 10, maxProb: 10},
		{name: "ef over cap", params: &VectorIndexParams{EF: 11}, maxEF: 10, maxProb: 10, wantErr: "ef 11 exceeds HNSW_MAX_SEARCH_EF (10)"},
		{name: "probe over cap", params: &VectorIndexParams{SearchProbe: 11}, maxEF: 10, maxProb: 10, wantErr: "searchProbe 11 exceeds HFRESH_MAX_SEARCH_PROBE (10)"},
		{name: "negative ef", params: &VectorIndexParams{EF: -1}, maxEF: 10, maxProb: 10, wantErr: "ef must be >= 0, got -1"},
		{name: "unknown strategy", params: &VectorIndexParams{FilterStrategy: "fast"}, maxEF: 10, maxProb: 10, wantErr: `filterStrategy must be one of sweeping, acorn, pathseer, got "fast"`},
		{name: "unset caps are unbounded", params: &VectorIndexParams{EF: 50000, SearchProbe: 50000}},
		{
			name:   "per-target entries are not walked, their caller names them",
			params: &VectorIndexParams{PerTarget: map[string]VectorIndexParams{"title": {EF: 11}}},
			maxEF:  10, maxProb: 10,
		},
		{name: "proto spelling", names: ProtoFieldNames, params: &VectorIndexParams{SearchProbe: 11}, maxEF: 10, maxProb: 10, wantErr: "search_probe 11 exceeds HFRESH_MAX_SEARCH_PROBE (10)"},
		{name: "proto spelling for the strategy", names: ProtoFieldNames, params: &VectorIndexParams{FilterStrategy: "fast"}, maxEF: 10, maxProb: 10, wantErr: `filter_strategy must be one of sweeping, acorn, pathseer, got "fast"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			names := tt.names
			if names == (FieldNames{}) {
				names = JSONFieldNames
			}
			err := tt.params.Validate(tt.maxEF, tt.maxProb, names)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
