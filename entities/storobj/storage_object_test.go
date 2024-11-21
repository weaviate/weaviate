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
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, updateTime, err := DocIDAndTimeFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
		assert.Equal(t, before.LastUpdateTimeUnix(), updateTime)
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

func TestStorageObjectMarshallingMultiVector(t *testing.T) {
	before := FromObjectMulti(
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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
		},
		map[string][][]float32{
			"vector3": {{7, 8, 9}, {10, 11, 12}},
			"vector4": {{13, 14, 15}, {16, 17, 18}},
			"vector5": {{19, 20, 21}, {22, 23, 24}},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, updateTime, err := DocIDAndTimeFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
		assert.Equal(t, before.LastUpdateTimeUnix(), updateTime)
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


func TestStorageObjectUnMarshallingMultiVector(t *testing.T) {
	before := FromObjectMulti(
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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
		},
		map[string][][]float32{
			"vector3": {{7, 8, 9}, {10, 11, 12}},
			"vector4": {{13, 14, 15}, {16, 17, 18}},
			"vector5": {{19, 20, 21}, {22, 23, 24}},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after:= &Object{}
	after.UnmarshalBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, updateTime, err := DocIDAndTimeFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
		assert.Equal(t, before.LastUpdateTimeUnix(), updateTime)
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
		nil,
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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
			"vector3": {7, 8, 9},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	t.Run("without any optional", func(t *testing.T) {
		after, err := FromBinaryOptional(asBinary, additional.Properties{}, nil)
		require.Nil(t, err)

		t.Run("compare", func(t *testing.T) {
			// modify before to match expectations of after
			before.Object.Additional = nil
			before.Vector = nil
			before.VectorLen = 3
			before.Vectors = nil
			assert.Equal(t, before, after)

			assert.Equal(t, before.DocID, after.DocID)

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
			assert.Equal(t, uint64(12), so.DocID)
		})

		t.Run("is invalid without required params", func(t *testing.T) {
			assert.False(t, so.Valid())
		})

		t.Run("reassign index id", func(t *testing.T) {
			so.DocID = 13
			assert.Equal(t, uint64(13), so.DocID)
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
				}, nil, nil)
				alt.DocID = 13

				assert.Equal(t, so, alt)
			})
	})

	t.Run("objects", func(t *testing.T) {
		so := New(12)

		t.Run("check index id", func(t *testing.T) {
			assert.Equal(t, uint64(12), so.DocID)
		})

		t.Run("is invalid without required params", func(t *testing.T) {
			assert.False(t, so.Valid())
		})

		t.Run("reassign index id", func(t *testing.T) {
			so.DocID = 13
			assert.Equal(t, uint64(13), so.DocID)
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
				}, nil, nil)
				alt.DocID = 13

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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
			"vector3": {7, 8, 9},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, updateTime, err := DocIDAndTimeFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
		assert.Equal(t, before.LastUpdateTimeUnix(), updateTime)
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
		"ref": []interface{}{
			map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247"},
			map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168248"},
		},
		"nested":      map[string]interface{}{"test": map[string]interface{}{"innerInt": float64(3), "innerStr": "avc"}},
		"nestedArray": []interface{}{map[string]interface{}{"test": map[string]interface{}{"innerArray": float64(3), "innerStr": "avc"}}},
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
		"ref": []models.SingleRef{
			{Beacon: "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168247", Class: "OtherClass", Href: "/v1/f81bfe5e-16ba-4615-a516-46c2ae2e5a80"},
			{Beacon: "weaviate://localhost/SomeClass/3453/73f4eb5f-5abf-447a-81ca-74b1dd168248", Class: "OtherClass", Href: "/v1/f81bfe5e-16ba-4615-a516-46c2ae2e5a81"},
		},
		"nested":      map[string]interface{}{"test": map[string]interface{}{"innerInt": 3, "innerStr": "avc"}},
		"nestedArray": []interface{}{map[string]interface{}{"test": map[string]interface{}{"innerArray": float64(3), "innerStr": "avc"}}},
	}
	before := FromObject(
		&models.Object{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 "73f2eb5f-5abf-447a-81ca-74b1dd168247",
			Properties:         properties,
		},
		[]float32{1, 2, 0.7},
		nil,
	)

	before.DocID = 7
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
		models.Vectors{
			"vector1": {1, 2, 3},
			"vector2": {4, 5, 6},
			"vector3": {7, 8, 9},
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	after, err := FromBinary(asBinary)
	require.Nil(t, err)

	t.Run("compare", func(t *testing.T) {
		assert.Equal(t, before, after)
	})

	t.Run("extract only doc id and compare", func(t *testing.T) {
		id, updateTime, err := DocIDAndTimeFromBinary(asBinary)
		require.Nil(t, err)
		assert.Equal(t, uint64(7), id)
		assert.Equal(t, before.LastUpdateTimeUnix(), updateTime)
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
					nil,
				)
				before.DocID = 7

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
							"name":   "myName",
							"second": "entry",
						},
					},
					vector,
					nil,
				)
				before.DocID = 7

				asBinary, err := before.MarshalBinary()
				require.Nil(t, err)

				t.Run("get without additional properties", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{}, nil)
					require.Nil(t, err)
					// modify before to match expectations of after
					before.Object.Additional = nil
					before.Vector = nil
					before.VectorLen = int(vectorLength)
					assert.Equal(t, before, after)

					assert.Equal(t, before.DocID, after.DocID)

					// The vector length should always be returned (for usage metrics
					// purposes) even if the vector itself is skipped
					assert.Equal(t, after.VectorLen, int(vectorLength))
				})

				t.Run("get with additional property vector", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{Vector: true}, nil)
					require.Nil(t, err)
					// modify before to match expectations of after
					before.Object.Additional = nil
					before.Vector = vector
					before.VectorLen = int(vectorLength)
					assert.Equal(t, before, after)

					assert.Equal(t, before.DocID, after.DocID)

					// The vector length should always be returned (for usage metrics
					// purposes) even if the vector itself is skipped
					assert.Equal(t, after.VectorLen, int(vectorLength))
					assert.Equal(t, vector, after.Vector)
				})

				t.Run("with explicit properties", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{},
						&PropertyExtraction{PropStrings: []string{"name"}, PropStringsList: [][]string{{"name"}}},
					)
					require.Nil(t, err)

					assert.Equal(t, before.DocID, after.DocID)
					// second property is not included
					assert.Equal(t, map[string]interface{}{"name": "myName"}, after.Properties())
				})

				t.Run("test no props and moduleparams", func(t *testing.T) {
					after, err := FromBinaryOptional(asBinary, additional.Properties{
						NoProps:      true,
						ModuleParams: map[string]interface{}{"foo": "bar"}, // this causes the property extraction code to run
					},
						&PropertyExtraction{PropStrings: nil, PropStringsList: nil},
					)
					require.Nil(t, err)

					assert.Equal(t, before.DocID, after.DocID)
					var emptyProps map[string]interface{}
					assert.Equal(t, emptyProps, after.Properties())
				})
			})
		})
	}
}

func TestVectorFromBinary(t *testing.T) {
	vector1 := []float32{1, 2, 3}
	vector2 := []float32{4, 5, 6}
	vector3 := []float32{7, 8, 9}
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
		models.Vectors{
			"vector1": vector1,
			"vector2": vector2,
			"vector3": vector3,
		},
	)
	before.DocID = 7

	asBinary, err := before.MarshalBinary()
	require.Nil(t, err)

	outVector1, err := VectorFromBinary(asBinary, nil, "vector1")
	require.Nil(t, err)
	assert.Equal(t, vector1, outVector1)

	outVector2, err := VectorFromBinary(asBinary, nil, "vector2")
	require.Nil(t, err)
	assert.Equal(t, vector2, outVector2)

	outVector3, err := VectorFromBinary(asBinary, nil, "vector3")
	require.Nil(t, err)
	assert.Equal(t, vector3, outVector3)
}

func TestStorageInvalidObjectMarshalling(t *testing.T) {
	t.Run("invalid className", func(t *testing.T) {
		invalidClassName := make([]byte, maxClassNameLength+1)
		cryptorand.Read(invalidClassName[:])

		invalidObj := FromObject(
			&models.Object{
				Class:              string(invalidClassName),
				CreationTimeUnix:   123456,
				LastUpdateTimeUnix: 56789,
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			},
			nil,
			nil,
		)

		_, err := invalidObj.MarshalBinary()
		require.ErrorContains(t, err, "could not marshal 'className' max length exceeded")
	})

	t.Run("invalid vector", func(t *testing.T) {
		invalidObj := FromObject(
			&models.Object{
				Class:              "classA",
				CreationTimeUnix:   123456,
				LastUpdateTimeUnix: 56789,
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			},
			make([]float32, maxVectorLength+1),
			nil,
		)

		_, err := invalidObj.MarshalBinary()
		require.ErrorContains(t, err, "could not marshal 'vector' max length exceeded")
	})

	t.Run("invalid named vector size", func(t *testing.T) {
		invalidObj := FromObject(
			&models.Object{
				Class:              "classA",
				CreationTimeUnix:   123456,
				LastUpdateTimeUnix: 56789,
				ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			},
			nil,
			models.Vectors{
				"vector1": make(models.Vector, maxVectorLength+1),
			},
		)

		_, err := invalidObj.MarshalBinary()
		require.ErrorContains(t, err, "could not marshal 'vector' max length exceeded")
	})
}

// Test that using the same buffer as source for unmarshalling does not cause problems due to memoy reuse
func TestMemoryReuse(t *testing.T) {
	props := []map[string]interface{}{
		{
			"numberArray":  []interface{}{1.1, 2.1},
			"intArray":     []interface{}{1., 2., 5000.},
			"textArrayUTF": []interface{}{"語", "b"},
			"boolArray":    []interface{}{true, false},
			"textArray":    []interface{}{"hello", ",", "I", "am", "a", "veeery", "long", "Array", "with some text."},
			"ref":          []interface{}{map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/73f4eb5f-5abf-447a-81ca-74b1dd168247"}},
			"foo":          float64(17),
		},
		{
			"numberArray":  []interface{}{1.4, 6.1},
			"intArray":     []interface{}{4., 3., 9000.},
			"textArrayUTF": []interface{}{"a", "c"},
			"boolArray":    []interface{}{true, true},
			"textArray":    []interface{}{"I", "contain", "other", "text"},
			"ref":          []interface{}{map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/73f4eb5f-5abf-447a-81ca-74b1dd168248"}},
			"foo":          float64(5),
		},
		{
			"numberArray":  []interface{}{1.4, 6.1, 8.9},
			"intArray":     []interface{}{4., 3., 9000., 1.},
			"textArrayUTF": []interface{}{"a", "c", "d"},
			"boolArray":    []interface{}{true, true, false},
			"textArray":    []interface{}{"I", "contain", "other", "text", "too"},
			"ref":          []interface{}{map[string]interface{}{"beacon": "weaviate://localhost/SomeClass/73f4eb5f-5abf-447a-81ca-74b1dd168249"}},
			"foo":          float64(9),
		},
	}

	largestSize := 0
	for i, prop := range props {
		obj := models.Object{
			Class:      "something",
			ID:         strfmt.UUID(fmt.Sprintf("73f4eb5f-5abf-447a-81ca-74b1dd16824%v", i)),
			Properties: prop,
		}
		before := FromObject(&obj, nil, nil)
		asBinary, err := before.MarshalBinary()
		require.Nil(t, err)
		if len(asBinary) > largestSize {
			largestSize = len(asBinary)
		}
	}

	reuseableBuff := make([]byte, largestSize)
	afterProps := []map[string]interface{}{}
	for i, beforeProp := range props {
		obj := models.Object{
			Class:      "something",
			ID:         strfmt.UUID(fmt.Sprintf("73f4eb5f-5abf-447a-81ca-74b1dd16824%v", i)),
			Properties: beforeProp,
		}

		propStrings := make([]string, 0, len(beforeProp))
		propStringsList := make([][]string, 0, len(beforeProp))

		for j := range beforeProp {
			propStrings = append(propStrings, j)
			propStringsList = append(propStringsList, []string{j})
		}

		before := FromObject(&obj, nil, nil)
		asBinary, err := before.MarshalBinary()
		require.Nil(t, err)
		reuseableBuff = reuseableBuff[:len(asBinary)]
		copy(reuseableBuff, asBinary)

		afterProp := map[string]interface{}{}
		require.Nil(t, UnmarshalProperties(reuseableBuff, &afterProp, propStrings, propStringsList))
		afterProps = append(afterProps, afterProp)
	}

	for i, afterProp := range afterProps {
		assert.Equal(t, props[i], afterProp)
	}
}

func BenchmarkUnmarshalPropertiesFullObject(b *testing.B) {
	benchmarkExtraction(b, nil)
}

func BenchmarkUnmarshalPropertiesExplicitOnlySome(b *testing.B) {
	benchmarkExtraction(b, []string{"name", "second"})
}

func BenchmarkUnmarshalPropertiesExplicitAll(b *testing.B) {
	benchmarkExtraction(b, []string{"name", "second", "number", "bool", "array"})
}

func benchmarkExtraction(b *testing.B, propStrings []string) {
	beforeProps := map[string]interface{}{
		"name":   "some long string",
		"second": "other very long string",
		"number": float64(17),
		"bool":   false,
		"array":  []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
	}
	before := FromObject(
		&models.Object{
			Class:            "MyFavoriteClass",
			CreationTimeUnix: 123456,
			ID:               "73f2eb5f-5abf-447a-81ca-74b1dd168247",
			Properties:       beforeProps,
		},
		nil,
		nil,
	)
	before.DocID = 7
	var props *PropertyExtraction

	if len(propStrings) > 0 {
		propStringsList := make([][]string, len(propStrings))
		for i, prop := range propStrings {
			propStringsList[i] = []string{prop}
		}

		props = &PropertyExtraction{
			PropStrings:     propStrings,
			PropStringsList: propStringsList,
		}
	}

	asBinary, err := before.MarshalBinary()
	require.Nil(b, err)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		after, err := FromBinaryOptional(asBinary, additional.Properties{}, props)
		require.Nil(b, err)
		require.NotNil(b, after)
	}
}

func TestObjectsByDocID(t *testing.T) {
	// the main variable is the input length here which has an effect on chunking
	// and parallelization
	tests := []struct {
		name     string
		inputIDs []uint64
		// there is no flag for expected output as that is deterministic based on
		// the doc ID, we use a convention for the UUID and set a specific prop
		// exactly to the doc ID.
	}{
		{
			name:     "1 object - sequential code path",
			inputIDs: []uint64{0},
		},
		{
			name:     "2 objects - concurrent code path",
			inputIDs: []uint64{0, 1},
		},
		{
			name:     "10 objects - consecutive from beginning",
			inputIDs: []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name: "30 objects - consecutive from beginning",
			inputIDs: []uint64{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
				17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
			},
		},
		{
			name:     "100 objects - random - should perfectly match to divide into groups",
			inputIDs: pickRandomIDsBetween(0, 1000, 100),
		},
		{
			name:     "99 objects - random - uneven groups slightly below perfect chunk size",
			inputIDs: pickRandomIDsBetween(0, 1000, 99),
		},
		{
			name:     "101 objects - random - uneven groups slightly above perfect chunk size",
			inputIDs: pickRandomIDsBetween(0, 1000, 101),
		},
		{
			name:     "117 objects - random - because why not",
			inputIDs: pickRandomIDsBetween(0, 1000, 117),
		},
	}

	logger, _ := test.NewNullLogger()

	bucket := genFakeBucket(t, 1000)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := ObjectsByDocID(bucket, test.inputIDs, additional.Properties{}, nil, logger)
			require.Nil(t, err)
			require.Len(t, res, len(test.inputIDs))

			for i, obj := range res {
				expectedDocID := test.inputIDs[i]
				assert.Equal(t, expectedDocID, uint64(obj.Properties().(map[string]any)["i"].(float64)))
				expectedUUID := strfmt.UUID(fmt.Sprintf("73f2eb5f-5abf-447a-81ca-74b1dd1%05d", expectedDocID))
				assert.Equal(t, expectedUUID, obj.ID())
			}
		})
	}
}

func TestSkipMissingObjects(t *testing.T) {
	bucket := genFakeBucket(t, 1000)
	logger, _ := test.NewNullLogger()
	ids := pickRandomIDsBetween(0, 1000, 100)
	ids = append(ids, 1001, 1002, 1003)
	objs, err := objectsByDocIDParallel(bucket, ids, additional.Properties{}, nil, logger)
	require.Nil(t, err)
	require.Len(t, objs, 100)
	for _, obj := range objs {
		require.NotNil(t, obj)
	}
}

func BenchmarkObjectsByDocID(b *testing.B) {
	bucket := genFakeBucket(b, 10000)
	logger, _ := test.NewNullLogger()
	ids := pickRandomIDsBetween(0, 10000, 100)

	tests := []struct {
		concurrent bool
		amount     int
	}{
		{concurrent: true, amount: 1},
		{concurrent: false, amount: 1},
		{concurrent: true, amount: 2},
		{concurrent: false, amount: 2},
		{concurrent: true, amount: 10},
		{concurrent: false, amount: 10},
		{concurrent: true, amount: 100},
		{concurrent: false, amount: 100},
	}
	b.ResetTimer()

	for _, tt := range tests {
		b.Run(fmt.Sprintf("Concurrent: %v with amount: %v", tt.concurrent, tt.amount), func(t *testing.B) {
			for i := 0; i < b.N; i++ {
				if tt.concurrent {
					_, err := objectsByDocIDParallel(bucket, ids[:tt.amount], additional.Properties{}, nil, logger)
					require.Nil(t, err)

				} else {
					_, err := objectsByDocIDSequential(bucket, ids[:tt.amount], additional.Properties{}, nil)
					require.Nil(t, err)
				}
			}
		})
	}
}

func intsToBytes(ints ...uint64) []byte {
	byteOps := byteops.NewReadWriter(make([]byte, len(ints)*8))
	for _, i := range ints {
		byteOps.WriteUint64(i)
	}
	return byteOps.Buffer
}

func FuzzObjectGet(f *testing.F) {
	maxSize := uint64(9999)
	logger, _ := test.NewNullLogger()
	bucket := genFakeBucket(f, maxSize)

	readTests := []struct {
		ids []uint64
	}{
		{ids: []uint64{0}},
		{ids: []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{ids: pickRandomIDsBetween(0, 1000, 100)},
	}
	for _, tc := range readTests {
		f.Add(intsToBytes(tc.ids...)) // Use f.Add to provide a seed corpus
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data)%8 != 0 {
			return
		}
		ids := make([]uint64, len(data)/8)
		for i := 0; i < len(ids); i++ {
			ids[i] = binary.LittleEndian.Uint64(data[i*8 : (i+1)*8])
			if ids[i] >= maxSize {
				return
			}
		}

		res, err := ObjectsByDocID(bucket, ids, additional.Properties{}, nil, logger)
		require.Nil(t, err)
		require.Len(t, res, len(ids))
		for i, obj := range res {
			expectedDocID := ids[i]
			assert.Equal(t, expectedDocID, uint64(obj.Properties().(map[string]any)["i"].(float64)))
			expectedUUID := strfmt.UUID(fmt.Sprintf("73f2eb5f-5abf-447a-81ca-74b1dd1%05d", expectedDocID))
			assert.Equal(t, expectedUUID, obj.ID())
		}
	})
}

type fakeBucket struct {
	objects map[uint64][]byte
}

func (f *fakeBucket) GetBySecondary(_ int, _ []byte) ([]byte, error) {
	panic("not implemented")
}

func (f *fakeBucket) GetBySecondaryWithBuffer(indexID int, docIDBytes []byte, lsmBuf []byte) ([]byte, []byte, error) {
	docID := binary.LittleEndian.Uint64(docIDBytes)
	objBytes, ok := f.objects[docID]
	if !ok {
		return nil, nil, nil
	}
	if len(lsmBuf) < len(objBytes) {
		lsmBuf = make([]byte, len(objBytes))
	}

	copy(lsmBuf, objBytes)
	return lsmBuf[:len(objBytes)], lsmBuf, nil
}

func genFakeBucket(t testing.TB, maxSize uint64) *fakeBucket {
	bucket := &fakeBucket{objects: map[uint64][]byte{}}
	for i := uint64(0); i < maxSize; i++ {
		obj := New(i)
		obj.SetProperties(map[string]any{"i": i, "foo": strings.Repeat("bar", int(i))})
		obj.SetClass("MyClass")
		obj.SetID(strfmt.UUID(fmt.Sprintf("73f2eb5f-5abf-447a-81ca-74b1dd1%05d", i)))
		objBytes, err := obj.MarshalBinary()
		require.Nil(t, err)
		bucket.objects[i] = objBytes
	}

	return bucket
}

func pickRandomIDsBetween(start, end uint64, count int) []uint64 {
	ids := make([]uint64, count)
	for i := 0; i < count; i++ {
		ids[i] = start + uint64(rand.Intn(int(end-start)))
	}
	return ids
}
