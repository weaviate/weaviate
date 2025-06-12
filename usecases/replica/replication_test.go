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

package replica_test

import (
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/replica"

	"github.com/weaviate/weaviate/usecases/objects"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
)

func Test_VObject_MarshalBinary(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name    string
		vector  []float32
		vectors models.Vectors
	}{
		{
			name:   "with vector",
			vector: []float32{1, 2, 3, 4, 5},
		},
		{
			name: "with vectors",
			vectors: models.Vectors{
				"vec1": []float32{0.1, 0.2, 0.3, 0.4},
				"vec2": []float32{1, 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := models.Object{
				ID:                 strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336"),
				Class:              "SomeClass",
				CreationTimeUnix:   now.UnixMilli(),
				LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
				Properties: map[string]interface{}{
					"propA":    "this is prop A",
					"propB":    "this is prop B",
					"someDate": now.Format(time.RFC3339Nano),
					"aNumber":  1e+06,
					"crossRef": map[string]interface{}{
						"beacon": "weaviate://localhost/OtherClass/c82d011c-f05a-43de-8a8a-ee9c814d4cfb",
					},
				},
				Vector: tt.vector,
				Additional: map[string]interface{}{
					"score": 0.055465422484,
				},
			}
			if tt.vectors != nil {
				obj.Vectors = tt.vectors
			}

			t.Run("when object is present", func(t *testing.T) {
				expected := objects.VObject{
					LatestObject:    &obj,
					StaleUpdateTime: now.UnixMilli(),
					Version:         1,
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received objects.VObject
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.EqualValues(t, expected, received)
			})

			t.Run("when object is present", func(t *testing.T) {
				expected := objects.VObject{
					LatestObject:    &obj,
					StaleUpdateTime: now.UnixMilli(),
					Version:         1,
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received objects.VObject
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.EqualValues(t, expected, received)
			})

			t.Run("when object is nil", func(t *testing.T) {
				expected := objects.VObject{
					LatestObject:    nil,
					StaleUpdateTime: now.UnixMilli(),
					Version:         1,
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received objects.VObject
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.EqualValues(t, expected, received)
			})
		})
	}
}

func Test_Replica_MarshalBinary(t *testing.T) {
	now := time.Now()
	id := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	tests := []struct {
		name    string
		vector  []float32
		vectors map[string][]float32
	}{
		{
			name:   "with vector",
			vector: []float32{1, 2, 3, 4, 5},
		},
		{
			name: "with vectors",
			vectors: map[string][]float32{
				"vec1": {0.1, 0.2, 0.3, 0.4},
				"vec2": {1, 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:                 id,
					Class:              "SomeClass",
					CreationTimeUnix:   now.UnixMilli(),
					LastUpdateTimeUnix: now.Add(time.Hour).UnixMilli(), // time-traveling ;)
					Properties: map[string]interface{}{
						"propA":    "this is prop A",
						"propB":    "this is prop B",
						"someDate": now.Format(time.RFC3339Nano),
						"aNumber":  1e+06,
						"crossRef": models.MultipleRef{
							crossref.NewLocalhost("OtherClass", id).
								SingleRef(),
						},
					},
					Additional: map[string]interface{}{
						"score": 0.055465422484,
					},
				},
			}
			if tt.vector != nil {
				obj.Vector = tt.vector
				obj.VectorLen = len(tt.vector)
			}
			if tt.vectors != nil {
				obj.Vector = []float32{}
				obj.Vectors = tt.vectors
			}

			t.Run("when object is present", func(t *testing.T) {
				expected := replica.Replica{
					Object: &obj,
					ID:     obj.ID(),
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received replica.Replica
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.EqualValues(t, expected.Object, received.Object)
				assert.EqualValues(t, expected.ID, received.ID)
				assert.EqualValues(t, expected.Deleted, received.Deleted)
			})

			t.Run("when object is nil", func(t *testing.T) {
				expected := replica.Replica{
					Object: nil,
					ID:     obj.ID(),
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received replica.Replica
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.EqualValues(t, expected.Object, received.Object)
				assert.EqualValues(t, expected.ID, received.ID)
				assert.EqualValues(t, expected.Deleted, received.Deleted)
			})
		})
	}
}

func Test_Replicas_MarshalBinary(t *testing.T) {
	now := time.Now()
	id1 := strfmt.UUID("c6f85bf5-c3b7-4c1d-bd51-e899f9605336")
	id2 := strfmt.UUID("88750a99-a72d-46c2-a582-89f02654391d")
	tests := []struct {
		name               string
		vec1, vec2         []float32
		vectors1, vectors2 map[string][]float32
	}{
		{
			name: "with vector",
			vec1: []float32{1, 2, 3, 4, 5},
			vec2: []float32{10, 20, 30, 40, 50},
		},
		{
			name: "with vectors",
			vectors1: map[string][]float32{
				"vec1": {0.1, 0.2, 0.3, 0.4},
				"vec2": {1, 2},
			},
			vectors2: map[string][]float32{
				"vec1": {0.11, 0.22, 0.33, 0.44},
				"vec2": {11, 22},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj1 := storobj.Object{
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
				Vector: []float32{},
			}
			if tt.vec1 != nil {
				obj1.Vector = tt.vec1
				obj1.VectorLen = len(tt.vec1)
			}
			if tt.vectors1 != nil {
				obj1.Vector = []float32{}
				obj1.Vectors = tt.vectors2
			}

			obj2 := storobj.Object{
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
			}
			if tt.vec2 != nil {
				obj2.Vector = tt.vec2
				obj2.VectorLen = len(tt.vec2)
			}
			if tt.vectors2 != nil {
				obj2.Vector = []float32{}
				obj2.Vectors = tt.vectors2
			}

			t.Run("when objects are present", func(t *testing.T) {
				expected := replica.Replicas{
					{
						Object: &obj1,
						ID:     obj1.ID(),
					},
					{
						Object: &obj2,
						ID:     obj2.ID(),
					},
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received replica.Replicas
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.Len(t, received, 2)
				assert.EqualValues(t, expected[0].Object, received[0].Object)
				assert.EqualValues(t, expected[0].ID, received[0].ID)
				assert.EqualValues(t, expected[0].Deleted, received[0].Deleted)
				assert.EqualValues(t, expected[1].Object, received[1].Object)
				assert.EqualValues(t, expected[1].ID, received[1].ID)
				assert.EqualValues(t, expected[1].Deleted, received[1].Deleted)
			})

			t.Run("when there is a nil object", func(t *testing.T) {
				expected := replica.Replicas{
					{
						Object: &obj1,
						ID:     obj1.ID(),
					},
					{
						Object: nil,
						ID:     obj2.ID(),
					},
				}

				b, err := expected.MarshalBinary()
				require.Nil(t, err)

				var received replica.Replicas
				err = received.UnmarshalBinary(b)
				require.Nil(t, err)

				assert.Len(t, received, 2)
				assert.EqualValues(t, expected[0].Object, received[0].Object)
				assert.EqualValues(t, expected[0].ID, received[0].ID)
				assert.EqualValues(t, expected[0].Deleted, received[0].Deleted)
				assert.EqualValues(t, expected[1].Object, received[1].Object)
				assert.EqualValues(t, expected[1].ID, received[1].ID)
				assert.EqualValues(t, expected[1].Deleted, received[1].Deleted)
			})
		})
	}
}
