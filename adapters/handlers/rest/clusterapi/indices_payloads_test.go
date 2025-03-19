//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clusterapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/searchparams"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
)

func Test_objectListPayload_Marshal(t *testing.T) {
	now := time.Now()
	vec1 := []float32{1, 2, 3, 4, 5}
	vec2 := []float32{10, 20, 30, 40, 50}
	id1 := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	id2 := strfmt.UUID("88750a99-a72d-46c2-a582-89f02654391d")

	objs := []*storobj.Object{
		{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 id1,
				Class:              "SomeClass",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
				Properties: map[string]interface{}{
					"propA":    "this is prop A",
					"propB":    "this is prop B",
					"someDate": now.Format(time.RFC3339Nano),
					"aNumber":  1e+06,
					"crossRef": models.MultipleRef{
						crossref.NewLocalhost("OtherClass", id1).
							SingleRef(),
					},
				},
				Additional: map[string]interface{}{
					"score": 0.055465422484,
				},
			},
			Vector:    vec1,
			VectorLen: 5,
		},
		nil,
		{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:                 id2,
				Class:              "SomeClass",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
				Properties: map[string]interface{}{
					"propA":    "this is prop A",
					"propB":    "this is prop B",
					"someDate": now.Format(time.RFC3339Nano),
					"aNumber":  1e+06,
					"crossRef": models.MultipleRef{
						crossref.NewLocalhost("OtherClass", id2).
							SingleRef(),
					},
				},
				Additional: map[string]interface{}{
					"score": 0.055465422484,
				},
			},
			Vector:    vec2,
			VectorLen: 5,
		},
	}

	payload := objectListPayload{}
	b, err := payload.Marshal(objs)
	require.Nil(t, err)

	received, err := payload.Unmarshal(b)
	require.Nil(t, err)
	assert.Len(t, received, 2)
	assert.EqualValues(t, objs[0].Object, received[0].Object)
	assert.EqualValues(t, objs[0].ID(), received[0].ID())
	assert.EqualValues(t, objs[2].Object, received[1].Object)
	assert.EqualValues(t, objs[2].ID(), received[1].ID())
}

type searchParamsPayloadOld struct{}

func (p searchParamsPayloadOld) Marshal(vector []float32, targetVector string, limit int,
	filter *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking,
	sort []filters.Sort, cursor *filters.Cursor, groupBy *searchparams.GroupBy,
	addP additional.Properties,
) ([]byte, error) {
	type params struct {
		SearchVector   []float32                    `json:"searchVector"`
		TargetVector   string                       `json:"targetVector"`
		Limit          int                          `json:"limit"`
		Filters        *filters.LocalFilter         `json:"filters"`
		KeywordRanking *searchparams.KeywordRanking `json:"keywordRanking"`
		Sort           []filters.Sort               `json:"sort"`
		Cursor         *filters.Cursor              `json:"cursor"`
		GroupBy        *searchparams.GroupBy        `json:"groupBy"`
		Additional     additional.Properties        `json:"additional"`
	}

	par := params{vector, targetVector, limit, filter, keywordRanking, sort, cursor, groupBy, addP}
	return json.Marshal(par)
}

func (p searchParamsPayloadOld) Unmarshal(in []byte) ([]float32, string, float32, int,
	*filters.LocalFilter, *searchparams.KeywordRanking, []filters.Sort,
	*filters.Cursor, *searchparams.GroupBy, additional.Properties, error,
) {
	type searchParametersPayload struct {
		SearchVector   []float32                    `json:"searchVector"`
		TargetVector   string                       `json:"targetVector"`
		Distance       float32                      `json:"distance"`
		Limit          int                          `json:"limit"`
		Filters        *filters.LocalFilter         `json:"filters"`
		KeywordRanking *searchparams.KeywordRanking `json:"keywordRanking"`
		Sort           []filters.Sort               `json:"sort"`
		Cursor         *filters.Cursor              `json:"cursor"`
		GroupBy        *searchparams.GroupBy        `json:"groupBy"`
		Additional     additional.Properties        `json:"additional"`
	}
	var par searchParametersPayload
	err := json.Unmarshal(in, &par)
	return par.SearchVector, par.TargetVector, par.Distance, par.Limit,
		par.Filters, par.KeywordRanking, par.Sort, par.Cursor, par.GroupBy, par.Additional, err
}

// This tests the backward compatibility of the searchParamsPayload with the old version in 1.25 and before (copied from the old code above)
func TestBackwardCompatibilitySearch(t *testing.T) {
	payload := searchParamsPayload{}
	tests := []struct {
		SearchVectors []models.Vector
		Targets       []string
		compatible    bool
	}{
		{
			SearchVectors: []models.Vector{[]float32{1, 2, 3}, []float32{4, 5, 6}},
			Targets:       []string{"target1", "target2"},
			compatible:    false,
		},
		{
			SearchVectors: []models.Vector{[]float32{1, 2, 3}},
			Targets:       []string{"target1"},
			compatible:    true,
		},
	}

	for _, tt := range tests {
		t.Run("test", func(t *testing.T) {
			b126, err := payload.Marshal(tt.SearchVectors, tt.Targets, 0.7, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
			require.Nil(t, err)

			vecs, targets, _, _, _, _, _, _, _, _, _, _, err := payload.Unmarshal(b126)
			require.Nil(t, err)
			assert.Equal(t, tt.SearchVectors, vecs)
			assert.Equal(t, tt.Targets, targets)

			if tt.compatible {
				payloadOld := searchParamsPayloadOld{}
				b125, err := payloadOld.Marshal(tt.SearchVectors[0].([]float32), tt.Targets[0], 10, nil, nil, nil, nil, nil, additional.Properties{})
				require.Nil(t, err)
				vecsOld, targetsOld, _, _, _, _, _, _, _, _, err := payloadOld.Unmarshal(b126)
				require.Nil(t, err)
				assert.Equal(t, tt.SearchVectors[0], vecsOld)
				assert.Equal(t, tt.Targets[0], targetsOld)

				vecs, targets, _, _, _, _, _, _, _, _, _, _, err := payload.Unmarshal(b125)
				require.Nil(t, err)
				assert.Equal(t, tt.SearchVectors, vecs)
				assert.Equal(t, tt.Targets, targets)

			}
		})
	}
}

func Test_searchParametersPayload_Unmarshal(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		isMultiVector bool
	}{
		{
			name: "regular vectors",
			payload: `{
				"limit": 10,
				"TargetVectors": ["vector1", "vector2"],
				"searchVectors": [[1.0, 2.0], [3.0, 4.0]]
			}`,
			isMultiVector: false,
		},
		{
			name: "multi vectors",
			payload: `{
				"limit": 10,
				"TargetVectors": ["vector1", "vector2"],
				"searchVectors": [[[1.0, 2.0], [3.0, 4.0]], [[11.0], [33.0]]]
			}`,
			isMultiVector: true,
		},
		{
			name: "empty search vectors",
			payload: `{
				"searchVector": [1,2,3],
				"targetVector": "target1",
				"limit": 10,
				"filters": null,
				"keywordRanking": null,
				"sort": null,
				"cursor": null,
				"groupBy": null,
				"additional": {
					"classification": false,
					"refMeta": false,
					"vector": false,
					"vectors": null,
					"certainty": false,
					"id": false,
					"creationTimeUnix": false,
					"lastUpdateTimeUnix": false,
					"moduleParams": null,
					"distance": false,
					"score": false,
					"explainScore": false,
					"isConsistent": false,
					"group": false,
					"noProps": true
				}
			}`,
			isMultiVector: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var par searchParametersPayload
			err := json.Unmarshal([]byte(tt.payload), &par)
			require.NoError(t, err)
			if par.SearchVectors != nil {
				require.Len(t, par.SearchVectors, 2)
				if tt.isMultiVector {
					for _, vec := range par.SearchVectors {
						vector, ok := vec.([][]float32)
						assert.True(t, ok)
						assert.True(t, len(vector) > 0)
					}
				} else {
					for _, vec := range par.SearchVectors {
						vector, ok := vec.([]float32)
						assert.True(t, ok)
						assert.True(t, len(vector) > 0)
					}
				}
			} else {
				require.NotNil(t, par.Additional)
				assert.True(t, par.Additional.NoProps)
			}
		})
	}
}
