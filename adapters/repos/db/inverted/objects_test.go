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

package inverted

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestAnalyzeObject(t *testing.T) {
	a := NewAnalyzer(nil)

	t.Run("with multiple properties", func(t *testing.T) {
		id1 := uuid.New()
		id2 := uuid.New()
		sch := map[string]interface{}{
			"description": "I am great!",
			"email":       "john@doe.com",
			"about_me":    "I like reading sci-fi books",
			"profession":  "Mechanical Engineer",
			"id1":         id1,                 // correctly parsed
			"id2":         id2.String(),        // untyped
			"idArray1":    []uuid.UUID{id1},    // correctly parsed
			"idArray2":    []any{id2.String()}, // untyped
		}

		uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
		props := []*models.Property{
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:         "email",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "about_me",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationLowercase,
			},
			{
				Name:         "profession",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationField,
			},
			{
				Name:     "id1",
				DataType: []string{"uuid"},
			},
			{
				Name:     "id2",
				DataType: []string{"uuid"},
			},
			{
				Name:     "idArray1",
				DataType: []string{"uuid[]"},
			},
			{
				Name:     "idArray2",
				DataType: []string{"uuid[]"},
			},
		}
		res, err := a.Object(sch, props, strfmt.UUID(uuid))
		require.Nil(t, err)

		expectedDescription := []Countable{
			{
				Data:          []byte("i"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("am"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("great"),
				TermFrequency: float32(1),
			},
		}

		expectedEmail := []Countable{
			{
				Data:          []byte("john@doe.com"),
				TermFrequency: float32(1),
			},
		}

		expectedAboutMe := []Countable{
			{
				Data:          []byte("i"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("like"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("reading"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("sci-fi"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("books"),
				TermFrequency: float32(1),
			},
		}

		expectedProfession := []Countable{
			{
				Data:          []byte("Mechanical Engineer"),
				TermFrequency: float32(1),
			},
		}

		expectedUUID := []Countable{
			{
				Data:          []byte(uuid),
				TermFrequency: 0,
			},
		}

		expectedID1 := []Countable{
			{
				Data:          []byte(id1[:]),
				TermFrequency: 0,
			},
		}

		expectedID2 := []Countable{
			{
				Data:          []byte(id2[:]),
				TermFrequency: 0,
			},
		}

		expectedIDArray1 := []Countable{
			{
				Data:          []byte(id1[:]),
				TermFrequency: 0,
			},
		}

		expectedIDArray2 := []Countable{
			{
				Data:          []byte(id2[:]),
				TermFrequency: 0,
			},
		}

		require.Len(t, res, 9)
		var actualDescription []Countable
		var actualEmail []Countable
		var actualAboutMe []Countable
		var actualProfession []Countable
		var actualUUID []Countable
		var actualID1 []Countable
		var actualID2 []Countable
		var actualIDArray1 []Countable
		var actualIDArray2 []Countable

		for _, elem := range res {
			if elem.Name == "email" {
				actualEmail = elem.Items
			}

			if elem.Name == "description" {
				actualDescription = elem.Items
			}

			if elem.Name == "about_me" {
				actualAboutMe = elem.Items
			}

			if elem.Name == "profession" {
				actualProfession = elem.Items
			}

			if elem.Name == "_id" {
				actualUUID = elem.Items
			}

			if elem.Name == "id1" {
				actualID1 = elem.Items
			}

			if elem.Name == "id2" {
				actualID2 = elem.Items
			}

			if elem.Name == "idArray1" {
				actualIDArray1 = elem.Items
			}

			if elem.Name == "idArray2" {
				actualIDArray2 = elem.Items
			}
		}

		assert.ElementsMatch(t, expectedEmail, actualEmail, res)
		assert.ElementsMatch(t, expectedDescription, actualDescription, res)
		assert.ElementsMatch(t, expectedAboutMe, actualAboutMe, res)
		assert.ElementsMatch(t, expectedProfession, actualProfession, res)
		assert.ElementsMatch(t, expectedUUID, actualUUID, res)
		assert.ElementsMatch(t, expectedID1, actualID1, res)
		assert.ElementsMatch(t, expectedID2, actualID2, res)
		assert.ElementsMatch(t, expectedIDArray1, actualIDArray1, res)
		assert.ElementsMatch(t, expectedIDArray2, actualIDArray2, res)
	})

	t.Run("with array properties", func(t *testing.T) {
		sch := map[string]interface{}{
			"descriptions": []interface{}{"I am great!", "I am also great!"},
			"emails":       []interface{}{"john@doe.com", "john2@doe.com"},
			"about_me":     []interface{}{"I like reading sci-fi books", "I like playing piano"},
			"professions":  []interface{}{"Mechanical Engineer", "Marketing Analyst"},
			"integers":     []interface{}{int64(1), int64(2), int64(3), int64(4)},
			"numbers":      []interface{}{float64(1.1), float64(2.2), float64(3.0), float64(4)},
		}

		uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
		props := []*models.Property{
			{
				Name:         "descriptions",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:         "emails",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "about_me",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationLowercase,
			},
			{
				Name:         "professions",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationField,
			},
			{
				Name:     "integers",
				DataType: []string{"int[]"},
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
		}
		res, err := a.Object(sch, props, strfmt.UUID(uuid))
		require.Nil(t, err)

		expectedDescriptions := []Countable{
			{
				Data:          []byte("i"),
				TermFrequency: float32(2),
			},
			{
				Data:          []byte("am"),
				TermFrequency: float32(2),
			},
			{
				Data:          []byte("great"),
				TermFrequency: float32(2),
			},
			{
				Data:          []byte("also"),
				TermFrequency: float32(1),
			},
		}

		expectedEmails := []Countable{
			{
				Data:          []byte("john@doe.com"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("john2@doe.com"),
				TermFrequency: float32(1),
			},
		}

		expectedAboutMe := []Countable{
			{
				Data:          []byte("i"),
				TermFrequency: float32(2),
			},
			{
				Data:          []byte("like"),
				TermFrequency: float32(2),
			},
			{
				Data:          []byte("reading"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("sci-fi"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("books"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("playing"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("piano"),
				TermFrequency: float32(1),
			},
		}

		expectedProfessions := []Countable{
			{
				Data:          []byte("Mechanical Engineer"),
				TermFrequency: float32(1),
			},
			{
				Data:          []byte("Marketing Analyst"),
				TermFrequency: float32(1),
			},
		}

		expectedIntegers := []Countable{
			{
				Data: mustGetByteIntNumber(1),
			},
			{
				Data: mustGetByteIntNumber(2),
			},
			{
				Data: mustGetByteIntNumber(3),
			},
			{
				Data: mustGetByteIntNumber(4),
			},
		}

		expectedNumbers := []Countable{
			{
				Data: mustGetByteFloatNumber(1.1),
			},
			{
				Data: mustGetByteFloatNumber(2.2),
			},
			{
				Data: mustGetByteFloatNumber(3.0),
			},
			{
				Data: mustGetByteFloatNumber(4),
			},
		}

		expectedUUID := []Countable{
			{
				Data:          []byte(uuid),
				TermFrequency: 0,
			},
		}

		assert.Len(t, res, 7)
		var actualDescriptions []Countable
		var actualEmails []Countable
		var actualAboutMe []Countable
		var actualProfessions []Countable
		var actualIntegers []Countable
		var actualNumbers []Countable
		var actualUUID []Countable

		for _, elem := range res {
			if elem.Name == "emails" {
				actualEmails = elem.Items
			}

			if elem.Name == "descriptions" {
				actualDescriptions = elem.Items
			}

			if elem.Name == "about_me" {
				actualAboutMe = elem.Items
			}

			if elem.Name == "professions" {
				actualProfessions = elem.Items
			}

			if elem.Name == "integers" {
				actualIntegers = elem.Items
			}

			if elem.Name == "numbers" {
				actualNumbers = elem.Items
			}

			if elem.Name == "_id" {
				actualUUID = elem.Items
			}
		}

		assert.ElementsMatch(t, expectedEmails, actualEmails, res)
		assert.ElementsMatch(t, expectedDescriptions, actualDescriptions, res)
		assert.ElementsMatch(t, expectedAboutMe, actualAboutMe, res)
		assert.ElementsMatch(t, expectedProfessions, actualProfessions, res)
		assert.ElementsMatch(t, expectedIntegers, actualIntegers, res)
		assert.ElementsMatch(t, expectedNumbers, actualNumbers, res)
		assert.ElementsMatch(t, expectedUUID, actualUUID, res)
	})

	t.Run("with refProps", func(t *testing.T) {
		t.Run("with a single ref set in the object schema", func(t *testing.T) {
			beacon := strfmt.URI(
				"weaviate://localhost/c563d7fa-4a36-4eff-9f39-af1e1db276c4")
			schema := map[string]interface{}{
				"myRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: beacon,
					},
				},
			}

			uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
			props := []*models.Property{
				{
					Name:     "myRef",
					DataType: []string{"RefClass"},
				},
			}
			res, err := a.Object(schema, props, strfmt.UUID(uuid))
			require.Nil(t, err)

			expectedRefCount := []Countable{
				{Data: []uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
			}

			expectedRef := []Countable{
				{Data: []byte(beacon)},
			}

			expectedUUID := []Countable{
				{
					Data:          []byte(uuid),
					TermFrequency: 0,
				},
			}

			require.Len(t, res, 3)
			var actualRefCount []Countable
			var actualUUID []Countable
			var actualRef []Countable

			for _, elem := range res {
				switch elem.Name {
				case helpers.MetaCountProp("myRef"):
					actualRefCount = elem.Items
				case "_id":
					actualUUID = elem.Items
				case "myRef":
					actualRef = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
			assert.ElementsMatch(t, expectedUUID, actualUUID, res)
			assert.ElementsMatch(t, expectedRef, actualRef, res)
		})

		t.Run("with multiple refs set in the object schema", func(t *testing.T) {
			beacon1 := strfmt.URI(
				"weaviate://localhost/c563d7fa-4a36-4eff-9f39-af1e1db276c4")
			beacon2 := strfmt.URI(
				"weaviate://localhost/49fe5d33-0b52-4189-8e8d-4268427c4317")

			schema := map[string]interface{}{
				"myRef": models.MultipleRef{
					{Beacon: beacon1},
					{Beacon: beacon2},
				},
			}

			uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
			props := []*models.Property{
				{
					Name:     "myRef",
					DataType: []string{"RefClass"},
				},
			}
			res, err := a.Object(schema, props, strfmt.UUID(uuid))
			require.Nil(t, err)

			expectedRefCount := []Countable{
				{Data: []uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}},
			}

			expectedRef := []Countable{
				{Data: []byte(beacon1)},
				{Data: []byte(beacon2)},
			}

			expectedUUID := []Countable{
				{
					Data:          []byte(uuid),
					TermFrequency: 0,
				},
			}

			require.Len(t, res, 3)
			var actualRefCount []Countable
			var actualUUID []Countable
			var actualRef []Countable

			for _, elem := range res {
				switch elem.Name {
				case helpers.MetaCountProp("myRef"):
					actualRefCount = elem.Items
				case "_id":
					actualUUID = elem.Items
				case "myRef":
					actualRef = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
			assert.ElementsMatch(t, expectedUUID, actualUUID, res)
			assert.ElementsMatch(t, expectedRef, actualRef, res)
		})

		t.Run("with the ref omitted in the object schema", func(t *testing.T) {
			schema := map[string]interface{}{}

			uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
			props := []*models.Property{
				{
					Name:     "myRef",
					DataType: []string{"RefClass"},
				},
			}
			res, err := a.Object(schema, props, strfmt.UUID(uuid))
			require.Nil(t, err)

			expectedRefCount := []Countable{
				{Data: []uint8{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			}

			expectedUUID := []Countable{
				{
					Data:          []byte(uuid),
					TermFrequency: 0,
				},
			}

			require.Len(t, res, 2)
			var actualRefCount []Countable
			var actualUUID []Countable

			for _, elem := range res {
				if elem.Name == helpers.MetaCountProp("myRef") {
					actualRefCount = elem.Items
				}
				if elem.Name == "_id" {
					actualUUID = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
			assert.ElementsMatch(t, expectedUUID, actualUUID, res)
		})

		// due to the fix introduced in https://github.com/weaviate/weaviate/pull/2320,
		// MultipleRef's can appear as empty []interface{} when no actual refs are provided for
		// an object's reference property.
		//
		// this test asserts that reference properties do not break when they are unmarshalled
		// as empty interface{} slices.
		t.Run("when rep prop is stored as empty interface{} slice", func(t *testing.T) {
			uuid := "cf768bb0-03d8-4464-8f54-f787cf174c01"
			name := "Transformers"
			sch := map[string]interface{}{
				"name":      name,
				"reference": []interface{}{},
			}

			props := []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "reference",
					DataType: []string{"SomeClass"},
				},
			}
			res, err := a.Object(sch, props, strfmt.UUID(uuid))
			require.Nil(t, err)

			expectedUUID := []Countable{
				{
					Data:          []byte(uuid),
					TermFrequency: 0,
				},
			}

			expectedName := []Countable{
				{
					Data:          []byte(name),
					TermFrequency: 1,
				},
			}

			require.Len(t, res, 2)
			var actualUUID []Countable
			var actualName []Countable

			for _, elem := range res {
				switch elem.Name {
				case "_id":
					actualUUID = elem.Items
				case "name":
					actualName = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedUUID, actualUUID, res)
			assert.ElementsMatch(t, expectedName, actualName, res)
		})
	})

	t.Run("when objects are indexed by timestamps", func(t *testing.T) {
		sch := map[string]interface{}{
			"description":         "pretty ok if you ask me",
			"_creationTimeUnix":   1650551406404,
			"_lastUpdateTimeUnix": 1650551406404,
		}

		uuid := strfmt.UUID("2609f1bc-7693-48f3-b531-6ddc52cd2501")
		props := []*models.Property{
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		}

		res, err := a.Object(sch, props, uuid)
		require.Nil(t, err)
		require.Len(t, res, 4)

		expected := []Property{
			{
				Name: "description",
				Items: []Countable{
					{Data: []byte("pretty"), TermFrequency: 1},
					{Data: []byte("ok"), TermFrequency: 1},
					{Data: []byte("if"), TermFrequency: 1},
					{Data: []byte("you"), TermFrequency: 1},
					{Data: []byte("ask"), TermFrequency: 1},
					{Data: []byte("me"), TermFrequency: 1},
				},
				HasFilterableIndex: true,
				HasSearchableIndex: true,
			},
			{
				Name:               "_id",
				Items:              []Countable{{Data: []byte("2609f1bc-7693-48f3-b531-6ddc52cd2501")}},
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "_creationTimeUnix",
				Items:              []Countable{{Data: []byte("1650551406404")}},
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
			{
				Name:               "_lastUpdateTimeUnix",
				Items:              []Countable{{Data: []byte("1650551406404")}},
				HasFilterableIndex: true,
				HasSearchableIndex: false,
			},
		}

		for i := range res {
			assert.Equal(t, expected[i].Name, res[i].Name)
			assert.Equal(t, expected[i].HasFilterableIndex, res[i].HasFilterableIndex)
			assert.Equal(t, expected[i].HasSearchableIndex, res[i].HasSearchableIndex)
			assert.ElementsMatch(t, expected[i].Items, res[i].Items)
		}
	})
}

func TestConvertSliceToUntyped(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expectedErr error
	}{
		{
			name:  "interface{} slice",
			input: []interface{}{map[string]interface{}{}},
		},
		{
			name:  "string slice",
			input: []string{"some", "slice"},
		},
		{
			name:  "int slice",
			input: []int{1, 2, 3, 4, 5},
		},
		{
			name:  "time slice",
			input: []time.Time{time.Now(), time.Now(), time.Now()},
		},
		{
			name:  "bool slice",
			input: []bool{false},
		},
		{
			name:  "float64 slice",
			input: []float64{1.2, 53555, 4.123, 2, 7.8877887, 0.0001},
		},
		{
			name:  "empty slice",
			input: []string{},
		},
		{
			name:        "unsupported uint8 slice",
			input:       []uint8{1, 2, 3, 4, 5},
			expectedErr: fmt.Errorf("unsupported type []uint8"),
		},
		{
			name:        "unsupported struct{}",
			input:       struct{}{},
			expectedErr: fmt.Errorf("unsupported type struct {}"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output, err := typedSliceToUntyped(test.input)
			if test.expectedErr != nil {
				assert.EqualError(t, err, test.expectedErr.Error())
			} else {
				require.Nil(t, err)
				assert.Len(t, output, reflect.ValueOf(test.input).Len())
				assert.IsType(t, []interface{}{}, output)
			}
		})
	}
}

func TestIndexInverted(t *testing.T) {
	vFalse := false
	vTrue := true

	t.Run("has filterable index", func(t *testing.T) {
		type testCase struct {
			name            string
			indexFilterable *bool
			dataType        schema.DataType

			expextedFilterable bool
		}

		testCases := []testCase{
			{
				name:            "int, filterable null",
				indexFilterable: nil,
				dataType:        schema.DataTypeInt,

				expextedFilterable: true,
			},
			{
				name:            "int, filterable false",
				indexFilterable: &vFalse,
				dataType:        schema.DataTypeInt,

				expextedFilterable: false,
			},
			{
				name:            "int, filterable true",
				indexFilterable: &vTrue,
				dataType:        schema.DataTypeInt,

				expextedFilterable: true,
			},
			{
				name:            "text, filterable null",
				indexFilterable: nil,
				dataType:        schema.DataTypeText,

				expextedFilterable: true,
			},
			{
				name:            "text, filterable false",
				indexFilterable: &vFalse,
				dataType:        schema.DataTypeText,

				expextedFilterable: false,
			},
			{
				name:            "text, filterable true",
				indexFilterable: &vTrue,
				dataType:        schema.DataTypeText,

				expextedFilterable: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				hasFilterableIndex := HasFilterableIndex(&models.Property{
					Name:            "prop",
					DataType:        tc.dataType.PropString(),
					IndexFilterable: tc.indexFilterable,
				})

				assert.Equal(t, tc.expextedFilterable, hasFilterableIndex)
			})
		}
	})

	t.Run("has searchable index", func(t *testing.T) {
		type testCase struct {
			name            string
			indexSearchable *bool
			dataType        schema.DataType

			expextedSearchable bool
		}

		testCases := []testCase{
			{
				name:            "int, searchable null",
				indexSearchable: nil,
				dataType:        schema.DataTypeInt,

				expextedSearchable: false,
			},
			{
				name:            "int, searchable false",
				indexSearchable: &vFalse,
				dataType:        schema.DataTypeInt,

				expextedSearchable: false,
			},
			{
				name:            "int, searchable true",
				indexSearchable: &vTrue,
				dataType:        schema.DataTypeInt,

				expextedSearchable: false,
			},
			{
				name:            "text, searchable null",
				indexSearchable: nil,
				dataType:        schema.DataTypeText,

				expextedSearchable: true,
			},
			{
				name:            "text, searchable false",
				indexSearchable: &vFalse,
				dataType:        schema.DataTypeText,

				expextedSearchable: false,
			},
			{
				name:            "text, searchable true",
				indexSearchable: &vTrue,
				dataType:        schema.DataTypeText,

				expextedSearchable: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				hasSearchableIndex := HasSearchableIndex(&models.Property{
					Name:            "prop",
					DataType:        tc.dataType.PropString(),
					IndexSearchable: tc.indexSearchable,
				})

				assert.Equal(t, tc.expextedSearchable, hasSearchableIndex)
			})
		}
	})
}

func mustGetByteIntNumber(in int) []byte {
	out, err := LexicographicallySortableInt64(int64(in))
	if err != nil {
		panic(err)
	}
	return out
}

func mustGetByteFloatNumber(in float64) []byte {
	out, err := LexicographicallySortableFloat64(in)
	if err != nil {
		panic(err)
	}
	return out
}
