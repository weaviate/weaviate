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

package storobj

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestStorageObjectMarshalling(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Additional: models.AdditionalProperties{
				"classification": &additional.Classification{
					BasedOn: []string{"some", "fields"},
				},
				"interpretation": map[string]interface{}{
					"Source": []interface{}{
						map[string]interface{}{
							"concept":    "foo",
							"occurrence": float64(7),
							"weight":     float64(3),
						},
					},
				},
			},
			Properties: map[string]interface{}{
				"name": "MyName",
				"foo":  float64(17),
			},
		},
		[]float32{1, 2, 0.7},
	)
	before.SetDocID(7)

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, err := DocIDFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
	})

	t.Run("extract single text prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractTextProp(asBinary, "name")
		require.Nil(t, err)
		require.True(t, ok)
		require.NotEmpty(t, prop)
		assert.Equal(t, "MyName", prop[0])
	})

	t.Run("extract non-existing text prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractTextProp(asBinary, "IDoNotExist")
		require.Nil(t, err)
		require.True(t, ok)
		require.Empty(t, prop)
	})
}

func TestFilteringNilProperty(t *testing.T) {
	object := FromObject(
		&models.Object{
			Class: "MyFavoriteClass",
			ID:    "73f2eb5f-5abf-447a-81ca-74b1dd168247",
			Properties: map[string]interface{}{
				"IWillBeRemoved": nil,
				"IWillStay":      float64(17),
			},
		},
		[]float32{1, 2, 0.7},
	)
	props := object.Properties()
	propsTyped, ok := props.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, propsTyped["IWillStay"], float64(17))

	elem, ok := propsTyped["IWillBeRemoved"]
	require.False(t, ok)
	require.Nil(t, elem)
}

func TestStorageObjectUnmarshallingSpecificProps(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Additional: models.AdditionalProperties{
				"classification": &additional.Classification{
					BasedOn: []string{"some", "fields"},
				},
				"interpretation": map[string]interface{}{
					"Source": []interface{}{
						map[string]interface{}{
							"concept":    "foo",
							"occurrence": float64(7),
							"weight":     float64(3),
						},
					},
				},
			},
			Properties: map[string]interface{}{
				"name": "MyName",
				"foo":  float64(17),
			},
		},
		[]float32{1, 2, 0.7},
	)
	before.SetDocID(7)

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	t.Run("without any optional", func(t *testing.T) {
		after, err := FromBinaryOptional(asBinary, additional.Properties{})
		require.Nil(t, err)

		t.Run("compare", func(t *testing.T) {
			// modify before to match expectations of after
			before.Object.Additional = nil
			before.Vector = nil
			before.VectorLen = 3
			assert.Equal(t, before, after)

			assert.Equal(t, before.docID, after.docID)

			// The vector length should always be returned (for usage metrics
			// purposes) even if the vector itself is skipped
			assert.Equal(t, after.VectorLen, 3)
		})
	})
}

func TestNewStorageObject(t *testing.T) {
	t.Run("objects", func(t *testing.T) {
		so := New(12)

		t.Run("check index id", func(t *testing.T) {
			assert.Equal(t, uint64(12), so.docID)
		})

		t.Run("is invalid without required params", func(t *testing.T) {
			assert.False(t, so.Valid())
		})

		t.Run("reassign index id", func(t *testing.T) {
			so.SetDocID(13)
			assert.Equal(t, uint64(13), so.docID)
		})

		t.Run("assign class", func(t *testing.T) {
			so.SetClass("MyClass")
			assert.Equal(t, schema.ClassName("MyClass"), so.Class())
		})

		t.Run("assign uuid", func(t *testing.T) {
			id := strfmt.UUID("bf706904-8618-463f-899c-4a2aafd48d56")
			so.SetID(id)
			assert.Equal(t, id, so.ID())
		})

		t.Run("assign uuid", func(t *testing.T) {
			schema := map[string]interface{}{
				"foo": "bar",
			}
			so.SetProperties(schema)
			assert.Equal(t, schema, so.Properties())
		})

		t.Run("must now be valid", func(t *testing.T) {
			assert.True(t, so.Valid())
		})

		t.Run("make sure it's identical with an object created from an existing object",
			func(t *testing.T) {
				alt := FromObject(&models.Object{
					Class: "MyClass",
					ID:    "bf706904-8618-463f-899c-4a2aafd48d56",
					Properties: map[string]interface{}{
						"foo": "bar",
					},
				}, nil)
				alt.SetDocID(13)

				assert.Equal(t, so, alt)
			})
	})

	t.Run("objects", func(t *testing.T) {
		so := New(12)

		t.Run("check index id", func(t *testing.T) {
			assert.Equal(t, uint64(12), so.docID)
		})

		t.Run("is invalid without required params", func(t *testing.T) {
			assert.False(t, so.Valid())
		})

		t.Run("reassign index id", func(t *testing.T) {
			so.SetDocID(13)
			assert.Equal(t, uint64(13), so.docID)
		})

		t.Run("assign class", func(t *testing.T) {
			so.SetClass("MyClass")
			assert.Equal(t, schema.ClassName("MyClass"), so.Class())
		})

		t.Run("assign uuid", func(t *testing.T) {
			id := strfmt.UUID("bf706904-8618-463f-899c-4a2aafd48d56")
			so.SetID(id)
			assert.Equal(t, id, so.ID())
		})

		t.Run("assign uuid", func(t *testing.T) {
			schema := map[string]interface{}{
				"foo": "bar",
			}
			so.SetProperties(schema)
			assert.Equal(t, schema, so.Properties())
		})

		t.Run("must now be valid", func(t *testing.T) {
			assert.True(t, so.Valid())
		})

		t.Run("make sure it's identical with an object created from an existing action",
			func(t *testing.T) {
				alt := FromObject(&models.Object{
					Class: "MyClass",
					ID:    "bf706904-8618-463f-899c-4a2aafd48d56",
					Properties: map[string]interface{}{
						"foo": "bar",
					},
				}, nil)
				alt.SetDocID(13)

				assert.Equal(t, so, alt)
			})
	})
}

func TestStorageArrayObjectMarshalling(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Additional: models.AdditionalProperties{
				"classification": &additional.Classification{
					BasedOn: []string{"some", "fields"},
				},
				"interpretation": map[string]interface{}{
					"Source": []interface{}{
						map[string]interface{}{
							"concept":    "foo",
							"occurrence": float64(7),
							"weight":     float64(3),
						},
					},
				},
			},
			Properties: map[string]interface{}{
				"textArray":   []string{"c", "d"},
				"numberArray": []float64{1.1, 2.1},
				"foo":         float64(17),
			},
		},
		[]float32{1, 2, 0.7},
	)
	before.SetDocID(7)

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, err := DocIDFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
	})

	t.Run("extract text array prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractTextProp(asBinary, "textArray")
		require.Nil(t, err)
		require.True(t, ok)
		assert.Equal(t, []string{"c", "d"}, prop)
	})

	t.Run("extract number array prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractNumberArrayProp(asBinary, "numberArray")
		require.Nil(t, err)
		require.True(t, ok)
		assert.Equal(t, []float64{1.1, 2.1}, prop)
	})
}

func TestExtractionOfSingleProperties(t *testing.T) {
	expected := map[string]interface{}{
		"numberArray":  []interface{}{1.1, 2.1},
		"intArray":     []interface{}{1., 2., 5000.},
		"textArrayUTF": []interface{}{"語", "b"},
		"textArray":    []interface{}{"hello", ",", "I", "am", "a", "veeery", "long", "Array", "with some text."},
		"foo":          float64(17),
		"text":         "single string",
		"bool":         true,
		"time":         "2011-11-23T01:52:23.000004234Z",
		"boolArray":    []interface{}{true, false, true},
		"beacon":       []interface{}{map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247"}},
		"ref":          []interface{}{map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247"}},
	}
	properties := map[string]interface{}{
		"numberArray":  []float64{1.1, 2.1},
		"intArray":     []int32{1, 2, 5000},
		"textArrayUTF": []string{"語", "b"},
		"textArray":    []string{"hello", ",", "I", "am", "a", "veeery", "long", "Array", "with some text."},
		"foo":          float64(17),
		"text":         "single string",
		"bool":         true,
		"time":         time.Date(2011, 11, 23, 1, 52, 23, 4234, time.UTC),
		"boolArray":    []bool{true, false, true},
		"beacon":       []map[string]interface{}{{"beacon": "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247"}},
		"ref":          []models.SingleRef{{Beacon: "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247", Class: "OtherClass", Href: "/v1/f81bfe5e-16ba-4615-a516-46c2ae2e5a80"}},
	}
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Properties:         properties,
		},
		[]float32{1, 2, 0.7},
	)

	before.SetDocID(7)
	byteObject, err := before.MarshalBinary()
	require.Nil(t, err)

	var propertyNames []string
	var propStrings [][]string
	for key := range properties {
		propertyNames = append(propertyNames, key)
		propStrings = append(propStrings, []string{key})
	}

	extractedProperties := map[string]interface{}{}

	// test with reused property map
	for i := 0; i < 2; i++ {
		require.Nil(t, UnmarshalPropertiesFromObject(byteObject, &extractedProperties, propertyNames, propStrings))
		for key := range expected {
			require.Equal(t, expected[key], extractedProperties[key])
		}

	}
}

func TestStorageObjectMarshallingWithGroup(t *testing.T) {
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Additional: models.AdditionalProperties{
				"classification": &additional.Classification{
					BasedOn: []string{"some", "fields"},
				},
				"interpretation": map[string]interface{}{
					"Source": []interface{}{
						map[string]interface{}{
							"concept":    "foo",
							"occurrence": float64(7),
							"weight":     float64(3),
						},
					},
				},
				"group": &additional.Group{
					ID: 100,
					GroupedBy: &additional.GroupedBy{
						Value: "group-by-some-property",
						Path:  []string{"property-path"},
					},
					MaxDistance: 0.1,
					MinDistance: 0.2,
					Count:       200,
					Hits: []map[string]interface{}{
						{
							"property1": "value1",
							"_additional": &additional.GroupHitAdditional{
								ID:       "2c76ca18-2073-4c48-aa52-7f444d2f5b80",
								Distance: 0.24,
							},
						},
						{
							"property1": "value2",
						},
					},
				},
			},
			Properties: map[string]interface{}{
				"name": "MyName",
				"foo":  float64(17),
			},
		},
		[]float32{1, 2, 0.7},
	)
	before.SetDocID(7)

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, err := DocIDFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
	})

	t.Run("extract single text prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractTextProp(asBinary, "name")
		require.Nil(t, err)
		require.True(t, ok)
		require.NotEmpty(t, prop)
		assert.Equal(t, "MyName", prop[0])
	})

	t.Run("extract non-existing text prop", func(t *testing.T) {
		prop, ok, err := ParseAndExtractTextProp(asBinary, "IDoNotExist")
		require.Nil(t, err)
		require.True(t, ok)
		require.Empty(t, prop)
	})

	t.Run("extract group additional property", func(t *testing.T) {
		require.NotNil(t, after.AdditionalProperties())
		require.NotNil(t, after.AdditionalProperties()["group"])
		group, ok := after.AdditionalProperties()["group"].(*additional.Group)
		require.True(t, ok)
		assert.Equal(t, 100, group.ID)
		assert.NotNil(t, group.GroupedBy)
		assert.Equal(t, "group-by-some-property", group.GroupedBy.Value)
		assert.Equal(t, []string{"property-path"}, group.GroupedBy.Path)
		assert.Equal(t, 200, group.Count)
		assert.Equal(t, float32(0.1), group.MaxDistance)
		assert.Equal(t, float32(0.2), group.MinDistance)
		require.Len(t, group.Hits, 2)
		require.NotNil(t, group.Hits[0]["_additional"])
		groupHitAdditional, ok := group.Hits[0]["_additional"].(*additional.GroupHitAdditional)
		require.True(t, ok)
		assert.Equal(t, strfmt.UUID("2c76ca18-2073-4c48-aa52-7f444d2f5b80"), groupHitAdditional.ID)
		assert.Equal(t, float32(0.24), groupHitAdditional.Distance)
		assert.Equal(t, "value1", group.Hits[0]["property1"])
		require.Nil(t, group.Hits[1]["_additional"])
		assert.Equal(t, "value2", group.Hits[1]["property1"])
	})
}

func TestStorageMaxVectorDimensionsObjectMarshalling(t *testing.T) {
	generateVector := func(dims uint16) []float32 {
		vector := make([]float32, dims)
		for i := range vector {
			vector[i] = 0.1
		}
		return vector
	}
	// 65535 is max uint16 number
	edgeVectorLengths := []uint16{0, 1, 768, 50000, 65535}
	for _, vectorLength := range edgeVectorLengths {
		t.Run(fmt.Sprintf("%v vector dimensions", vectorLength), func(t *testing.T) {
			t.Run("marshal binary", func(t *testing.T) {
				vector := generateVector(vectorLength)
				before := FromObject(
					&models.Object{
						Class:            "MyFavoriteClass",
						CreationTimeUnix: 123456,
						ID:               strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
						Properties: map[string]interface{}{
							"name": "myName",
						},
					},
					vector,
				)
				before.SetDocID(7)

				asBinary, err := before.MarshalBinary()
				require.Nil(t, err)

				after, err := FromBinary(asBinary)
				require.Nil(t, err)

				t.Run("compare", func(t *testing.T) {
					assert.Equal(t, before, after)
				})

				t.Run("try to extract a property", func(t *testing.T) {
					prop, ok, err := ParseAndExtractTextProp(asBinary, "name")
					require.Nil(t, err)
					require.True(t, ok)
					assert.Equal(t, []string{"myName"}, prop)
				})
			})

			t.Run("marshal optional binary", func(t *testing.T) {
				vector := generateVector(vectorLength)
				before := FromObject(
					&models.Object{
						Class:            "MyFavoriteClass",
						CreationTimeUnix: 123456,
						ID:               strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
						Properties: map[string]interface{}{
							"name": "myName",
						},
					},
					vector,
				)
				before.SetDocID(7)

				asBinary, err := before.MarshalBinary()
				require.Nil(t, err)

				t.Run("get without additional properties", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{})
					require.Nil(t, err)
					// modify before to match expectations of after
					before.Object.Additional = nil
					before.Vector = nil
					before.VectorLen = int(vectorLength)
					assert.Equal(t, before, after)

					assert.Equal(t, before.docID, after.docID)

					// The vector length should always be returned (for usage metrics
					// purposes) even if the vector itself is skipped
					assert.Equal(t, after.VectorLen, int(vectorLength))
				})

				t.Run("get with additional property vector", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{Vector: true})
					require.Nil(t, err)
					// modify before to match expectations of after
					before.Object.Additional = nil
					before.Vector = vector
					before.VectorLen = int(vectorLength)
					assert.Equal(t, before, after)

					assert.Equal(t, before.docID, after.docID)

					// The vector length should always be returned (for usage metrics
					// purposes) even if the vector itself is skipped
					assert.Equal(t, after.VectorLen, int(vectorLength))
					assert.Equal(t, vector, after.Vector)
				})
			})
		})
	}
}
