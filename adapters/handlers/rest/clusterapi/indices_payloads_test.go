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

package clusterapi

import (
	"encoding/binary"
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
	"github.com/weaviate/weaviate/usecases/objects"
)

func Test_objectListPayload_Marshal(t *testing.T) {
	now := time.Now()
	vec1 := []float32{1, 2, 3, 4, 5}
	vec2 := []float32{10, 20, 30, 40, 50}
	id1 := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	id2 := strfmt.UUID("88750a99-a72d-46c2-a582-89f02654391d")

	for _, tt := range []struct {
		method string
		objs   []*storobj.Object
		assert func(t *testing.T, in, out []*storobj.Object)
	}{
		{
			method: MethodGet,
			objs: []*storobj.Object{
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
			},
			assert: func(t *testing.T, in, out []*storobj.Object) {
				assert.Len(t, out, 2)
				assert.EqualValues(t, in[0].Object, out[0].Object)
				assert.EqualValues(t, in[0].ID(), out[0].ID())
				assert.EqualValues(t, in[2].Object, out[1].Object)
				assert.EqualValues(t, in[2].ID(), out[1].ID())
			},
		},
		{
			method: MethodPut,
			objs: []*storobj.Object{
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
						Vector: vec1,
					},
					Vector:    vec1,
					VectorLen: 5,
				},
			},
			assert: func(t *testing.T, in, out []*storobj.Object) {
				assert.Len(t, out, 1)
				assert.EqualValues(t, in[0].Object, out[0].Object)
				assert.EqualValues(t, in[0].ID(), out[0].ID())
			},
		},
	} {
		t.Run(tt.method, func(t *testing.T) {
			payload := objectListPayload{}
			b, err := payload.Marshal(tt.objs, tt.method)
			require.Nil(t, err)

			received, err := payload.Unmarshal(b, tt.method)
			require.Nil(t, err)
			tt.assert(t, tt.objs, received)
		})
	}
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
			b126, err := payload.Marshal(tt.SearchVectors, tt.Targets, 0.7, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil, nil)
			require.Nil(t, err)

			vecs, targets, _, _, _, _, _, _, _, _, _, _, _, err := payload.Unmarshal(b126)
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

				vecs, targets, _, _, _, _, _, _, _, _, _, _, _, err := payload.Unmarshal(b125)
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

func TestVersionedObjectListPayloadV2RoundTrip(t *testing.T) {
	now := time.Now()

	testCases := []struct {
		name  string
		input []*objects.VObject
	}{
		{
			name: "single object with primary vector",
			input: []*objects.VObject{
				{
					LatestObject: &models.Object{
						ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"),
						Class:              "C1",
						LastUpdateTimeUnix: now.UnixMilli(),
					},
					Vector:          []float32{0.1, 0.2, 0.3},
					StaleUpdateTime: now.UnixMilli(),
					Version:         1,
				},
			},
		},
		{
			name: "multiple objects with named and multi-vectors",
			input: []*objects.VObject{
				{
					LatestObject: &models.Object{
						ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242"),
						Class: "C2",
					},
					Vectors: map[string][]float32{
						"text": {0.1, 0.2, 0.3},
					},
					MultiVectors: map[string][][]float32{
						"colbert": {{0.4, 0.5}, {0.6, 0.7}},
					},
					StaleUpdateTime: now.UnixMilli(),
				},
				{
					ID:              strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168243"),
					Deleted:         true,
					StaleUpdateTime: now.Add(time.Hour).UnixMilli(),
					Version:         5,
				},
			},
		},
		{
			name:  "empty list",
			input: []*objects.VObject{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := IndicesPayloads.VersionedObjectList.MarshalV2(tc.input)
			require.NoError(t, err)

			got, err := IndicesPayloads.VersionedObjectList.UnmarshalV2(data)
			require.NoError(t, err)
			require.Len(t, got, len(tc.input))

			for i, want := range tc.input {
				assert.Equal(t, want.Deleted, got[i].Deleted)
				assert.Equal(t, want.StaleUpdateTime, got[i].StaleUpdateTime)
				assert.Equal(t, want.Version, got[i].Version)
				assert.Equal(t, want.Vector, got[i].Vector)
				assert.Equal(t, want.Vectors, got[i].Vectors)
				assert.Equal(t, want.MultiVectors, got[i].MultiVectors)
				if want.LatestObject != nil {
					require.NotNil(t, got[i].LatestObject)
					assert.Equal(t, want.LatestObject.ID, got[i].LatestObject.ID)
					assert.Equal(t, want.LatestObject.Class, got[i].LatestObject.Class)
				}
				if want.ID != "" {
					assert.Equal(t, want.ID, got[i].ID)
				}
			}
		})
	}
}

func TestVersionedObjectListUnmarshalV2FramingErrors(t *testing.T) {
	p := IndicesPayloads.VersionedObjectList

	// Build a valid two-record payload to use as a base.
	input := []*objects.VObject{
		{ID: strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241"), Version: 1},
		{ID: strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168242"), Deleted: true},
	}
	full, err := p.MarshalV2(input)
	require.NoError(t, err)

	t.Run("empty input is not an error", func(t *testing.T) {
		got, err := p.UnmarshalV2([]byte{})
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("truncated length prefix is an error", func(t *testing.T) {
		// Feed only 4 of the 8 length-prefix bytes.
		_, err := p.UnmarshalV2(full[:4])
		assert.Error(t, err)
	})

	t.Run("length prefix present but payload truncated", func(t *testing.T) {
		// 8-byte prefix is intact but only 4 bytes of payload follow.
		_, err := p.UnmarshalV2(full[:12])
		assert.Error(t, err)
	})

	t.Run("length prefix claims more bytes than remain", func(t *testing.T) {
		// Build a buffer whose length prefix is larger than the rest of the buffer.
		buf := make([]byte, 8+4)
		binary.LittleEndian.PutUint64(buf[:8], 1_000_000) // claims 1 MB
		copy(buf[8:], []byte{0x01, 0x02, 0x03, 0x04})     // only 4 bytes follow
		_, err := p.UnmarshalV2(buf)
		assert.Error(t, err)
	})

	t.Run("all truncated prefixes of a valid payload return errors", func(t *testing.T) {
		single := []*objects.VObject{{Version: 7}}
		data, err := p.MarshalV2(single)
		require.NoError(t, err)

		// Every prefix shorter than the full payload must be an error.
		for i := 1; i < len(data); i++ {
			_, err := p.UnmarshalV2(data[:i])
			assert.Errorf(t, err, "expected error for prefix length %d", i)
		}
	})
}

// TestVersionedObjectListPayloadV2LargeList verifies that the 8-byte length-prefix
// framing in MarshalV2/UnmarshalV2 is correct when applied to many objects. An
// off-by-one in the write cursor would corrupt subsequent entries and accumulate
// over a large number of objects.
func TestVersionedObjectListPayloadV2LargeList(t *testing.T) {
	const n = 1000
	input := make([]*objects.VObject, n)
	for i := range input {
		input[i] = &objects.VObject{
			Version: uint64(i),
			Vector:  []float32{float32(i), float32(i) + 0.5},
		}
	}

	data, err := IndicesPayloads.VersionedObjectList.MarshalV2(input)
	require.NoError(t, err)

	got, err := IndicesPayloads.VersionedObjectList.UnmarshalV2(data)
	require.NoError(t, err)
	require.Len(t, got, n)

	// Spot-check first, middle, and last entries.
	for _, idx := range []int{0, n / 2, n - 1} {
		assert.Equalf(t, input[idx].Version, got[idx].Version, "version mismatch at index %d", idx)
		assert.Equalf(t, input[idx].Vector, got[idx].Vector, "vector mismatch at index %d", idx)
	}
}
