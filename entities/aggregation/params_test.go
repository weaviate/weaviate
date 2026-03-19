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

package aggregation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func Test_Params_Unmarshal(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		isMultiVector bool
	}{
		{
			name: "regular vector",
			payload: `{
				"targetVector": "vector1",
				"searchVector": [1.0, 2.0]
			}`,
			isMultiVector: false,
		},
		{
			name: "multi vector",
			payload: `{
				"targetVector": "vector1",
				"searchVector": [[1.0, 2.0], [2.0]]
			}`,
			isMultiVector: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var par Params
			err := json.Unmarshal([]byte(tt.payload), &par)
			require.NoError(t, err)
			require.NotNil(t, par.SearchVector)
			if tt.isMultiVector {
				vector, ok := par.SearchVector.([][]float32)
				assert.True(t, ok)
				assert.True(t, len(vector) > 0)
			} else {
				vector, ok := par.SearchVector.([]float32)
				assert.True(t, ok)
				assert.True(t, len(vector) > 0)
			}
		})
	}
}

func TestIsCountStar(t *testing.T) {
	limit := 92
	for _, tt := range []struct {
		name   string
		params Params
		want   bool
	}{
		{
			name: "count(*)",
			params: Params{
				IncludeMetaCount: true,
			},
			want: true,
		},
		{
			name: "filtered",
			params: Params{
				IncludeMetaCount: true,
				Filters:          new(filters.LocalFilter),
			},
		},
		{
			name: "requests properties",
			params: Params{
				IncludeMetaCount: true,
				Properties:       make([]ParamProperty, 5),
			},
		},
		{
			name: "has limit",
			params: Params{
				IncludeMetaCount: true,
				Limit:            &limit,
			},
		},
		{
			name: "has object limit",
			params: Params{
				IncludeMetaCount: true,
				ObjectLimit:      &limit,
			},
		},
		{
			name: "has vector",
			params: Params{
				IncludeMetaCount: true,
				SearchVector:     []float32{1, 2, 3},
			},
		},
		{
			name: "has target vector",
			params: Params{
				IncludeMetaCount: true,
				TargetVector:     "v1",
			},
		},
		{
			name: "has module params",
			params: Params{
				IncludeMetaCount: true,
				ModuleParams: map[string]interface{}{
					"nearText": "sunshine",
				},
			},
		},
		{
			name: "has near vector",
			params: Params{
				IncludeMetaCount: true,
				NearVector:       new(searchparams.NearVector),
			},
		},
		{
			name: "has near object",
			params: Params{
				IncludeMetaCount: true,
				NearObject:       new(searchparams.NearObject),
			},
		},
		{
			name: "has hybrid",
			params: Params{
				IncludeMetaCount: true,
				Hybrid:           new(searchparams.HybridSearch),
			},
		},
		{
			name: "grouped",
			params: Params{
				IncludeMetaCount: true,
				GroupBy:          new(filters.Path),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsCountStar(&tt.params))
		})
	}
}
