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

package moduletools

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestVectorizablePropsComparator(t *testing.T) {
	propsSchema := createPropsSchema()
	nextProps := createNextProps()
	prevProps := createPrevProps()
	prevVector := []float32{1, 2, 3}

	t.Run("iterator", func(t *testing.T) {
		comp := NewVectorizablePropsComparator(propsSchema, nextProps, prevProps, prevVector)

		t.Run("returns props in asc order", func(t *testing.T) {
			expectedNames := []string{"blob", "text", "texts"}
			names := []string{}

			it := comp.PropsIterator()
			for propName, _, ok := it.Next(); ok; propName, _, ok = it.Next() {
				names = append(names, propName)
			}

			assert.Equal(t, expectedNames, names)
		})

		t.Run("returns values of next props", func(t *testing.T) {
			expectedValues := []interface{}{
				nextProps["blob"],
				nextProps["text"],
				nextProps["texts"],
			}
			values := []interface{}{}

			it := comp.PropsIterator()
			for _, propValue, ok := it.Next(); ok; _, propValue, ok = it.Next() {
				values = append(values, propValue)
			}

			assert.Equal(t, expectedValues, values)
		})
	})

	t.Run("returns prev vector", func(t *testing.T) {
		comp := NewVectorizablePropsComparator(propsSchema, nextProps, prevProps, prevVector)

		vector := comp.PrevVector()

		assert.Equal(t, prevVector, vector)
	})

	t.Run("considers non-vectorizable props as not changed", func(t *testing.T) {
		t.Run("all different", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nextProps, prevProps, prevVector)

			for _, propSchema := range propsSchema {
				switch propSchema.Name {
				case "blob", "text", "texts":
					assert.True(t, comp.IsChanged(propSchema.Name))
				default:
					assert.False(t, comp.IsChanged(propSchema.Name))
				}
			}
		})

		t.Run("next nils", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, map[string]interface{}{}, prevProps, prevVector)

			for _, propSchema := range propsSchema {
				switch propSchema.Name {
				case "blob", "text", "texts":
					assert.True(t, comp.IsChanged(propSchema.Name))
				default:
					assert.False(t, comp.IsChanged(propSchema.Name))
				}
			}
		})

		t.Run("prev nils", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nextProps, map[string]interface{}{}, prevVector)

			for _, propSchema := range propsSchema {
				switch propSchema.Name {
				case "blob", "text", "texts":
					assert.True(t, comp.IsChanged(propSchema.Name))
				default:
					assert.False(t, comp.IsChanged(propSchema.Name))
				}
			}
		})
	})

	t.Run("considers vectorizable props not changed", func(t *testing.T) {
		missingProps := map[string]interface{}{}
		nilProps := map[string]interface{}{
			"blob":  nil,
			"text":  nil,
			"texts": nil,
		}
		emptyArrayProps := map[string]interface{}{
			"texts": []string{},
		}
		emptyIfArrayProps := map[string]interface{}{
			"texts": []interface{}{},
		}

		t.Run("nil -> nil", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nilProps, nilProps, prevVector)

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("missing -> nil", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nilProps, missingProps, prevVector)

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("nil -> missing", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, missingProps, nilProps, prevVector)

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("missing -> missing", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, missingProps, missingProps, prevVector)

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("missing -> empty array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyArrayProps, missingProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("nil -> empty array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyArrayProps, nilProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty array -> missing", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, missingProps, emptyArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty array -> nil", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nilProps, emptyArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty interface array -> missing", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, missingProps, emptyIfArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty interface array -> nil", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, nilProps, emptyIfArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty interface array -> empty array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyArrayProps, emptyIfArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})
	})

	t.Run("considers vectorizable props changed", func(t *testing.T) {
		textPropsABC := map[string]interface{}{
			"texts": []string{"aaa", "bbb", "ccc"},
		}
		textPropsABCD := map[string]interface{}{
			"texts": []string{"aaa", "bbb", "ccc", "ddd"},
		}
		textPropsCBA := map[string]interface{}{
			"texts": []string{"ccc", "bbb", "aaa"},
		}

		t.Run("different array sizes (1)", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, textPropsABCD, textPropsABC, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("different array sizes (2)", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, textPropsABC, textPropsABCD, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("different array order", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, textPropsCBA, textPropsABC, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		// none of following should happen

		missingProps := map[string]interface{}{}
		nilProps := map[string]interface{}{
			"texts": nil,
		}
		emptyArrayProps := map[string]interface{}{
			"texts": []string{},
		}
		emptyIfArrayProps := map[string]interface{}{
			"texts": []interface{}{},
		}
		arrayProps := map[string]interface{}{
			"texts": []string{"txt1", "txt2"},
		}
		ifArrayProps := map[string]interface{}{
			"texts": []interface{}{"txt1", "txt2"},
		}

		t.Run("interface array -> array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, arrayProps, ifArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("array -> interface array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, ifArrayProps, arrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("missing -> empty interface array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyIfArrayProps, missingProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("nil -> empty interface array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyIfArrayProps, nilProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty array -> empty interface array", func(t *testing.T) {
			comp := NewVectorizablePropsComparator(propsSchema, emptyIfArrayProps, emptyArrayProps, prevVector)

			for _, propName := range []string{"texts"} {
				assert.True(t, comp.IsChanged(propName))
			}
		})
	})
}

func TestVectorizablePropsComparatorDummy(t *testing.T) {
	propsSchema := createPropsSchema()
	nextProps := createNextProps()

	t.Run("iterator", func(t *testing.T) {
		comp := NewVectorizablePropsComparatorDummy(propsSchema, nextProps)

		t.Run("returns props in asc order", func(t *testing.T) {
			expectedNames := []string{"blob", "text", "texts"}
			names := []string{}

			it := comp.PropsIterator()
			for propName, _, ok := it.Next(); ok; propName, _, ok = it.Next() {
				names = append(names, propName)
			}

			assert.Equal(t, expectedNames, names)
		})

		t.Run("returns values of next props", func(t *testing.T) {
			expectedValues := []interface{}{
				nextProps["blob"],
				nextProps["text"],
				nextProps["texts"],
			}
			values := []interface{}{}

			it := comp.PropsIterator()
			for _, propValue, ok := it.Next(); ok; _, propValue, ok = it.Next() {
				values = append(values, propValue)
			}

			assert.Equal(t, expectedValues, values)
		})
	})

	t.Run("returns no prev vector", func(t *testing.T) {
		comp := NewVectorizablePropsComparatorDummy(propsSchema, nextProps)

		vector := comp.PrevVector()

		assert.Nil(t, vector)
	})

	t.Run("considers non-vectorizable props as not changed", func(t *testing.T) {
		comp := NewVectorizablePropsComparatorDummy(propsSchema, nextProps)

		for _, propSchema := range propsSchema {
			switch propSchema.Name {
			case "blob", "text", "texts":
				assert.True(t, comp.IsChanged(propSchema.Name))
			default:
				assert.False(t, comp.IsChanged(propSchema.Name))
			}
		}
	})

	t.Run("considers vectorizable props not changed", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			comp := NewVectorizablePropsComparatorDummy(propsSchema, map[string]interface{}{
				"blob":  nil,
				"text":  nil,
				"texts": nil,
			})

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("missing", func(t *testing.T) {
			comp := NewVectorizablePropsComparatorDummy(propsSchema, map[string]interface{}{})

			for _, propName := range []string{"blob", "text", "texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})

		t.Run("empty array", func(t *testing.T) {
			comp := NewVectorizablePropsComparatorDummy(propsSchema, map[string]interface{}{
				"texts": []string{},
			})

			for _, propName := range []string{"texts"} {
				assert.False(t, comp.IsChanged(propName))
			}
		})
	})
}

func createPropsSchema() []*models.Property {
	return []*models.Property{
		{
			Name:     "text",
			DataType: schema.DataTypeText.PropString(),
		},
		{
			Name:     "texts",
			DataType: schema.DataTypeTextArray.PropString(),
		},
		{
			Name:     "int",
			DataType: schema.DataTypeInt.PropString(),
		},
		{
			Name:     "ints",
			DataType: schema.DataTypeIntArray.PropString(),
		},
		{
			Name:     "number",
			DataType: schema.DataTypeNumber.PropString(),
		},
		{
			Name:     "numbers",
			DataType: schema.DataTypeNumberArray.PropString(),
		},
		{
			Name:     "boolean",
			DataType: schema.DataTypeBoolean.PropString(),
		},
		{
			Name:     "booleans",
			DataType: schema.DataTypeBooleanArray.PropString(),
		},
		{
			Name:     "date",
			DataType: schema.DataTypeDate.PropString(),
		},
		{
			Name:     "dates",
			DataType: schema.DataTypeDateArray.PropString(),
		},
		{
			Name:     "uuid",
			DataType: schema.DataTypeUUID.PropString(),
		},
		{
			Name:     "uuids",
			DataType: schema.DataTypeUUIDArray.PropString(),
		},
		{
			Name:     "phone",
			DataType: schema.DataTypePhoneNumber.PropString(),
		},
		{
			Name:     "geo",
			DataType: schema.DataTypeGeoCoordinates.PropString(),
		},
		{
			Name:     "blob",
			DataType: schema.DataTypeBlob.PropString(),
		},
		{
			Name:     "object",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "n_text",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		},
		{
			Name:     "objects",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "n_text",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		},
	}
}

const (
	image  = "iVBORw0KGgoAAAANSUhEUgAAABQAAAAUCAYAAACNiR0NAAACrElEQVR4nOyUS0wUeRDGv37N9Dx6GJiF3ckOWUhgQ7LsssthN+xlA9k9bDBelJM3n2AkwYOCXhC9GGLiTeOBxKPxYsLBdwwXHzFRTAg4ikh05DGODE73DP36P0z3BEQT9CAXEyvppCtV9ctX1dUlc86xmSZuKu2rBcqAE/58qRsEIH0KKDIy0UHMgbuOvu1ZGbyhCUTvnKSl3vvMGf/H89cC3ldmLJeizvlTwHQX5xUio3mmRkc8lfZGxOJ8Upcjf2ii9B04a7yghPceEuXvX/lAfbnrlijq7WbpMTh3oYZbaKzyqgd0NgJmZ0I6o44mBZKIxP8Go4l7WuJsm99WoThVbVtpEMqgKHEIAXNtDAU385/D9K2eo4iR0bhSdx0AL7kclDAwaw4OG0co+me117EPLLoMKw4DZYDCKWS3hAU323C7ODa8THKdebYESZBQI9bsD0mRK23R9oOGCxACMAb/RWLEV+ADdZdzL6EMJCiaprRnZuiBy6HmaA6UlkepBFSklOT/ztJYR79B5J8UQBLKNUFG3wMLLmAQIG8DBcJQJ1iYzZkqFRmQ54DOAMphaw7SqdeATIIvSkCWAz+GAVUD1PUK3zjJ7OzbJ79aBFBlCsMqQlrQICwWAFcFZwEf6PeXphAaguBVABWAnBNECrWgdnzRm62/h//+fOZARfi3cUECLMbwvGBDegjImSi0pURxoGX7iaOtXUOxfJUhZcIQbwrIGIAaCKE+UQ+TVE80/dDf40v01sZ7XOrIo1PHj3RfCpq9owFeueMw/71vOD3xcu6X1ZypzHxTa8+xSW3LLr7voswHr9VYN56eHHSpHVjNET4+X4tGuvFO5ly3bTQUOpt3no5FQsb6uLFiRS4/GukLVkwn/qrdPZKMNU9+8At9u4dfbO8CAAD///eAU+2FY+BGAAAAAElFTkSuQmCC"
	image2 = "iVBORw0KGgoAAAANSUhEUgAAABUAAAAVCAYAAACpF6WWAAAC1UlEQVR4nNyUS0h8dRTHv/f+7tz/nTv6d2YaXQgq+KSHaQQRLiQyGmnRRmgRSWRtiswWEkUhGNFGKouiRbRrEURgIWS4SBBFy7JclEzqZGaJr3nd9+8Vd6YmEbGFburAvZtzzufc8z1frialxHWHeu3E/wX0SsPONYsoD2bGqPXEkmC77Zd28v0mbj2+JLxPRgGuXwRVOVtPU3d8VWJtWol03Ct49s7LmIxm2ij9sU/I+XeC4uga879+AIBShXrOuy/R4PW5gG51l3Ifg9INiH/xmhBcWrlvUTycAWXZbuZPz7uF918Nc1r4KhY/TYegwC8AIEjq7dDVKlQRYCnK7aYIif2qQjsBILkEnEBCiCNIax43k30g2vpDUWCiDLUDT7GdU3AOKIoKkzkgupSOsBoyzuprVOQeA0hUgfRU1fyoK9r3criIRQEuAME5iJ+HoQaofqnNpCz8VQAIxAILc3Tx0eXc8ocl4d3yC83CEy5qSK3ReaPjKaO4NtQj3dnbKSBFZR1DMKhC/AMtMSBPASHLA+BZLt4rfvPI90iBwQccF/ApoDMsxTegEyPREvw2/GQeaDGBuA7USA6tMkGWoblAkacUOLABi0ncZjhgeQ2gOrTDEEoAoVaENDhYmwC/AbgM2CwAdTpws06Hwiq7lqERfWAhs7/SX7aDApR8B/R3ArIbChYFhBGeu6IPF5A/c4hWDbIViBAViZpG2KIBBD0LVUsNdE5OprveGIMas2wqsZ33QbMKtD9qYR4nvKn7hke3xt9ufPPBkafNXNwhBybYpop9V0U8loJCEk7SHHrh7ubxF8uihlf8+zksZZvfWkzPPvs5kfe8Mih7npnKfLe913u25oedvTt6RyZ+uuu5fvn8Z5r8YOXhL4+sndazNcpFHs+cfNVvB8f1t9YPzhlarX0+7zPb3Dz6YtCMJE87UvcvnM9fCL1q/Pf/p1eKPwMAAP//lJuHtr5ZxxcAAAAASUVORK5CYII="
)

func createPrevProps() map[string]interface{} {
	return map[string]interface{}{
		"text":     "prev text",
		"texts":    []string{"prev texts_1", "prev texts_2"},
		"int":      float64(-1),
		"ints":     []float64{-2, -3},
		"number":   float64(-1.1),
		"numbers":  []float64{-2.2, -3.3},
		"boolean":  false,
		"booleans": []bool{true, true},
		"date":     time.Unix(1707332399, 0),
		"dates":    []time.Time{time.Unix(1707335999, 0), time.Unix(1707339599, 0)},
		"uuid":     uuid.MustParse("018d85e5-eec6-71bb-9a0f-f7cb545784c3"),
		"uuids": []uuid.UUID{
			uuid.MustParse("018d85e6-f7c9-74c4-b960-f1fd74bb2d3c"),
			uuid.MustParse("018d85e7-1b69-7e86-a487-bab1dec5d2f4"),
		},
		"phone": &models.PhoneNumber{
			DefaultCountry: "PL",
			Input:          "123456789",
		},
		"geo": &models.GeoCoordinates{
			Latitude:  ptrFloat32(51.107882),
			Longitude: ptrFloat32(17.038537),
		},
		"blob": image,
		"object": map[string]interface{}{
			"n_text": "prev n_text",
		},
		"objects": []map[string]interface{}{
			{"n_text": "prev n_text_0"},
			{"n_text": "prev n_text_1"},
		},
	}
}

func createNextProps() map[string]interface{} {
	return map[string]interface{}{
		"text":     "text",
		"texts":    []string{"texts_1", "texts_2"},
		"int":      float64(1),
		"ints":     []float64{2, 3},
		"number":   float64(1.1),
		"numbers":  []float64{2.2, 3.3},
		"boolean":  true,
		"booleans": []bool{false, false},
		"date":     time.Unix(1707332400, 0),
		"dates":    []time.Time{time.Unix(1707336000, 0), time.Unix(1707339600, 0)},
		"uuid":     uuid.MustParse("a2578e23-f665-4f04-97f1-5e56f5d4cea4"),
		"uuids": []uuid.UUID{
			uuid.MustParse("037a19a2-aeba-40cf-9a6b-60e36f10b030"),
			uuid.MustParse("6e92f90c-99c9-4579-9711-17015c361e07"),
		},
		"phone": &models.PhoneNumber{
			DefaultCountry: "PL",
			Input:          "100000000",
		},
		"geo": &models.GeoCoordinates{
			Latitude:  ptrFloat32(51.107883),
			Longitude: ptrFloat32(17.038538),
		},
		"blob": image2,
		"object": map[string]interface{}{
			"n_text": "n_text",
		},
		"objects": []map[string]interface{}{
			{"n_text": "n_text_0"},
			{"n_text": "n_text_1"},
		},
	}
}

func ptrFloat32(f float32) *float32 {
	return &f
}
