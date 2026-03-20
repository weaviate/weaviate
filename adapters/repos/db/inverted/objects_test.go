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

package inverted

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	ent "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestAnalyzeObject(t *testing.T) {
	a := NewAnalyzer(nil, "")

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
		res, _, err := a.Object(sch, props, strfmt.UUID(uuid))
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
		res, _, err := a.Object(sch, props, strfmt.UUID(uuid))
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
			res, _, err := a.Object(schema, props, strfmt.UUID(uuid))
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
			res, _, err := a.Object(schema, props, strfmt.UUID(uuid))
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
			res, _, err := a.Object(schema, props, strfmt.UUID(uuid))
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
			res, _, err := a.Object(sch, props, strfmt.UUID(uuid))
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

		res, _, err := a.Object(sch, props, uuid)
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

	t.Run("has rangeable index", func(t *testing.T) {
		b2s := func(b *bool) string {
			if b == nil {
				return "nil"
			}
			return strconv.FormatBool(*b)
		}
		rangeableDataTypes := map[schema.DataType]struct{}{
			schema.DataTypeInt:    {},
			schema.DataTypeNumber: {},
			schema.DataTypeDate:   {},
		}

		t.Run("supported types", func(t *testing.T) {
			for dataType := range rangeableDataTypes {
				for indexRangeFilters, expectedRangeFilters := range map[*bool]bool{
					nil:     false, // turned off by default
					&vFalse: false,
					&vTrue:  true,
				} {
					t.Run(fmt.Sprintf("rangeable_%s_%v", dataType, b2s(indexRangeFilters)), func(t *testing.T) {
						hasRangeableIndex := HasRangeableIndex(&models.Property{
							Name:              "prop",
							DataType:          dataType.PropString(),
							IndexRangeFilters: indexRangeFilters,
						})

						assert.Equal(t, expectedRangeFilters, hasRangeableIndex)
					})
				}
			}
		})

		t.Run("not supported types", func(t *testing.T) {
			for _, dataType := range schema.PrimitiveDataTypes {
				if _, ok := rangeableDataTypes[dataType]; ok {
					continue
				}

				for _, indexRangeFilters := range []*bool{nil, &vFalse, &vTrue} {
					t.Run(fmt.Sprintf("rangeable_%s_%v", dataType, b2s(indexRangeFilters)), func(t *testing.T) {
						hasRangeableIndex := HasRangeableIndex(&models.Property{
							Name:              "prop",
							DataType:          dataType.PropString(),
							IndexRangeFilters: indexRangeFilters,
						})

						assert.False(t, hasRangeableIndex)
					})
				}
			}
		})
	})
}

func mustGetByteIntNumber(in int) []byte {
	out, err := ent.LexicographicallySortableInt64(int64(in))
	if err != nil {
		panic(err)
	}
	return out
}

func mustGetByteFloatNumber(in float64) []byte {
	out, err := ent.LexicographicallySortableFloat64(in)
	if err != nil {
		panic(err)
	}
	return out
}

func boolPtr(v bool) *bool { return &v }

func TestHasNestedFilterableIndex(t *testing.T) {
	t.Run("child filterable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city"},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("child explicitly filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", IndexFilterable: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("all children not filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", IndexFilterable: boolPtr(false)},
			},
		}
		assert.False(t, HasNestedFilterableIndex(prop))
	})

	t.Run("deeply nested child is filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "tires",
					IndexFilterable: boolPtr(false),
					DataType:        schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", IndexFilterable: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasNestedFilterableIndex(prop))
	})

	t.Run("no nested properties", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
		}
		assert.False(t, HasNestedFilterableIndex(prop))
	})
}

func TestHasNestedSearchableIndex(t *testing.T) {
	t.Run("text child searchable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString()},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})

	t.Run("text child explicitly searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexSearchable: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})

	t.Run("text child not searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexSearchable: boolPtr(false)},
			},
		}
		assert.False(t, HasNestedSearchableIndex(prop))
	})

	t.Run("int child is never searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
			},
		}
		assert.False(t, HasNestedSearchableIndex(prop))
	})

	t.Run("deeply nested text child is searchable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "bio", DataType: schema.DataTypeText.PropString()},
					},
				},
			},
		}
		assert.True(t, HasNestedSearchableIndex(prop))
	})
}

func TestHasNestedRangeableIndex(t *testing.T) {
	t.Run("int child not rangeable by default", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
			},
		}
		assert.False(t, HasNestedRangeableIndex(prop))
	})

	t.Run("int child explicitly rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.True(t, HasNestedRangeableIndex(prop))
	})

	t.Run("text child is never rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.False(t, HasNestedRangeableIndex(prop))
	})

	t.Run("deeply nested number child is rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:     "stats",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "score", DataType: schema.DataTypeNumber.PropString(), IndexRangeFilters: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasNestedRangeableIndex(prop))
	})
}

func TestHasAnyNestedInvertedIndex(t *testing.T) {
	t.Run("child filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("child searchable only", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("child rangeable only", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("all children not indexed", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)},
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(false)},
			},
		}
		assert.False(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("parent not indexed but deeply nested child is filterable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "tires",
					DataType:        schema.DataTypeObjectArray.PropString(),
					IndexFilterable: boolPtr(false),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString(), IndexFilterable: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})

	t.Run("parent not indexed but deeply nested child is rangeable", func(t *testing.T) {
		prop := &models.Property{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{
					Name:            "stats",
					DataType:        schema.DataTypeObject.PropString(),
					IndexFilterable: boolPtr(false),
					NestedProperties: []*models.NestedProperty{
						{Name: "price", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
					},
				},
			},
		}
		assert.True(t, HasAnyNestedInvertedIndex(prop))
	})
}

func TestCollectNestedIndexConfig(t *testing.T) {
	props := []*models.NestedProperty{
		{Name: "city", DataType: schema.DataTypeText.PropString()},                                                                    // text: filterable+searchable
		{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},                                  // int: filterable+rangeable
		{Name: "active", DataType: schema.DataTypeBoolean.PropString()},                                                               // bool: filterable only
		{Name: "score", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false)},                                // number: not filterable, not rangeable
		{Name: "label", DataType: schema.DataTypeText.PropString(), IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)}, // not indexed at all
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "name", DataType: schema.DataTypeText.PropString()},
				{Name: "age", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		},
	}

	configs := collectNestedIndexConfig("", props)

	t.Run("text is filterable and searchable", func(t *testing.T) {
		cfg := configs["city"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
	})

	t.Run("int with range is filterable and rangeable", func(t *testing.T) {
		cfg := configs["count"]
		assert.True(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.True(t, cfg.rangeable)
	})

	t.Run("bool is filterable only", func(t *testing.T) {
		cfg := configs["active"]
		assert.True(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
	})

	t.Run("number with filterable disabled", func(t *testing.T) {
		cfg := configs["score"]
		assert.False(t, cfg.filterable)
		assert.False(t, cfg.searchable)
		assert.False(t, cfg.rangeable)
		assert.False(t, cfg.hasAny())
	})

	t.Run("fully disabled property", func(t *testing.T) {
		cfg := configs["label"]
		assert.False(t, cfg.hasAny())
	})

	t.Run("nested object paths not in config", func(t *testing.T) {
		_, exists := configs["owner"]
		assert.False(t, exists)
	})

	t.Run("nested leaf paths use dot notation", func(t *testing.T) {
		cfg := configs["owner.name"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.searchable)

		cfg = configs["owner.age"]
		assert.True(t, cfg.filterable)
		assert.True(t, cfg.rangeable)
	})

	t.Run("total leaf paths", func(t *testing.T) {
		// city, count, active, score, label, owner.name, owner.age
		assert.Len(t, configs, 7)
	})
}

func TestAnalyzeNestedProp(t *testing.T) {
	analyzer := NewAnalyzer(nil, "TestClass")

	t.Run("doc123 produces correct values and flags", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nestedObject",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{
					Name:     "owner",
					DataType: schema.DataTypeObject.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
						{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
						{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
					},
				},
				{
					Name:     "addresses",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					},
				},
				{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		}

		value := map[string]any{
			"name": "subdoc_123",
			"owner": map[string]any{
				"firstname": "Marsha",
				"lastname":  "Mallow",
				"nicknames": []any{"Marshmallow", "M&M"},
			},
			"addresses": []any{
				map[string]any{"city": "Berlin"},
			},
			"tags": []any{"german", "premium"},
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, "nestedObject", result.Name)
		assert.True(t, result.HasFilterableIndex)
		assert.True(t, result.HasSearchableIndex)
		assert.False(t, result.HasRangeableIndex)

		// Verify values are analyzed (bytes, not raw strings)
		assert.NotEmpty(t, result.Values)
		for _, v := range result.Values {
			assert.NotEmpty(t, v.Data, "value at %s should have analyzed data", v.Path)
			assert.NotEmpty(t, v.Positions, "value at %s should have positions", v.Path)
			assert.True(t, v.HasFilterableIndex, "all text values should be filterable")
		}

		// Verify metadata is propagated
		assert.NotEmpty(t, result.Idx)
		assert.NotEmpty(t, result.Exists)
	})

	t.Run("non-filterable paths are excluded from values", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "indexed", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(true)},
				{Name: "skipped", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(false), IndexSearchable: boolPtr(false)},
			},
		}

		value := map[string]any{
			"indexed": "hello",
			"skipped": "world",
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Only "indexed" should produce values
		for _, v := range result.Values {
			assert.Equal(t, "indexed", v.Path, "skipped path should not appear in values")
		}

		// But metadata still includes both (structural)
		existsPaths := map[string]bool{}
		for _, e := range result.Exists {
			existsPaths[e.Path] = true
		}
		assert.True(t, existsPaths["indexed"])
		assert.True(t, existsPaths["skipped"])
	})

	t.Run("aggregate flags reflect index types", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: "word", IndexFilterable: boolPtr(false)},
				{Name: "price", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: boolPtr(false), IndexRangeFilters: boolPtr(true)},
			},
		}

		value := map[string]any{
			"title": "hello",
			"price": float64(9.99),
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.False(t, result.HasFilterableIndex, "no filterable paths")
		assert.True(t, result.HasSearchableIndex, "title is searchable by default")
		assert.True(t, result.HasRangeableIndex, "price is rangeable")
	})

	t.Run("per-value flags match path config", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: boolPtr(true)},
			},
		}

		value := map[string]any{
			"city":  "Berlin",
			"count": float64(42),
		}

		result, err := analyzer.analyzeNestedProp(prop, value)
		require.NoError(t, err)

		for _, v := range result.Values {
			switch v.Path {
			case "city":
				assert.True(t, v.HasFilterableIndex)
				assert.True(t, v.HasSearchableIndex)
				assert.False(t, v.HasRangeableIndex)
			case "count":
				assert.True(t, v.HasFilterableIndex)
				assert.False(t, v.HasSearchableIndex)
				assert.True(t, v.HasRangeableIndex)
			default:
				t.Errorf("unexpected path %q", v.Path)
			}
		}
	})

	t.Run("nil value returns nil", func(t *testing.T) {
		prop := &models.Property{
			Name:     "nested",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString()},
			},
		}

		result, err := analyzer.analyzeNestedProp(prop, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	// Full design document tests using the shared schema from the design summary.
	// Each test runs with both full shared schema and minimal per-document schema
	// to verify that appending new sub-properties to the schema doesn't change
	// analysis results.

	fullSchema := []*models.NestedProperty{
		{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "firstname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "lastname", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "nicknames", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		},
		{
			Name:     "addresses",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "city", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "postcode", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{Name: "numbers", DataType: schema.DataTypeNumberArray.PropString()},
			},
		},
		{Name: "tags", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				{Name: "make", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
				{
					Name:     "tires",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "width", DataType: schema.DataTypeInt.PropString()},
						{Name: "radiuses", DataType: schema.DataTypeIntArray.PropString()},
					},
				},
				{
					Name:     "accessories",
					DataType: schema.DataTypeObjectArray.PropString(),
					NestedProperties: []*models.NestedProperty{
						{Name: "type", DataType: schema.DataTypeText.PropString(), Tokenization: "word"},
					},
				},
				{Name: "colors", DataType: schema.DataTypeTextArray.PropString(), Tokenization: "word"},
			},
		},
	}

	// doc123/124 schema: no accessories
	doc123Schema := []*models.NestedProperty{
		fullSchema[0], fullSchema[1], fullSchema[2], fullSchema[3],
		{
			Name:     "cars",
			DataType: schema.DataTypeObjectArray.PropString(),
			NestedProperties: []*models.NestedProperty{
				fullSchema[4].NestedProperties[0], // make
				fullSchema[4].NestedProperties[1], // tires
				fullSchema[4].NestedProperties[3], // colors
			},
		},
	}

	// doc125 schema: no nicknames in owner, has accessories
	doc125Schema := []*models.NestedProperty{
		fullSchema[0],
		{
			Name:     "owner",
			DataType: schema.DataTypeObject.PropString(),
			NestedProperties: []*models.NestedProperty{
				fullSchema[1].NestedProperties[0], // firstname
				fullSchema[1].NestedProperties[1], // lastname
			},
		},
		fullSchema[2], fullSchema[3], fullSchema[4],
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc123Schema},
	} {
		t.Run("doc123/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc123(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc123Schema},
	} {
		t.Run("doc124/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc124(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", doc125Schema},
	} {
		t.Run("doc125/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedObject", DataType: schema.DataTypeObject.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc125(t, analyzer, prop)
		})
	}

	for _, tc := range []struct {
		name   string
		schema []*models.NestedProperty
	}{
		{"full_schema", fullSchema},
		{"minimal_schema", fullSchema},
	} {
		t.Run("doc999/"+tc.name, func(t *testing.T) {
			prop := &models.Property{Name: "nestedArray", DataType: schema.DataTypeObjectArray.PropString(), NestedProperties: tc.schema}
			assertAnalyzeDoc999(t, analyzer, prop)
		})
	}
}

func assertAnalyzeDoc123(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_123",
		"owner": map[string]any{
			"firstname": "Marsha", "lastname": "Mallow",
			"nicknames": []any{"Marshmallow", "M&M"},
		},
		"addresses": []any{
			map[string]any{"city": "Berlin", "postcode": "10115", "numbers": []any{float64(123), float64(1123)}},
		},
		"tags": []any{"german", "premium"},
		"cars": []any{
			map[string]any{
				"make":   "BMW",
				"tires":  []any{map[string]any{"width": float64(225), "radiuses": []any{float64(18), float64(19)}}},
				"colors": []any{"black", "orange"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 2, valuePaths["owner.nicknames"])
	assert.Equal(t, 2, valuePaths["addresses.numbers"])
	assert.Equal(t, 2, valuePaths["tags"])
	assert.Equal(t, 2, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 2, valuePaths["cars.colors"])
	assert.Equal(t, 1, valuePaths["cars.tires.width"])
	assert.Equal(t, 1, valuePaths["cars.make"])
	assert.Contains(t, valuePaths, "name")
	assert.Contains(t, valuePaths, "owner.firstname")
	assert.Contains(t, valuePaths, "owner.lastname")
	assert.Contains(t, valuePaths, "addresses.city")
	assert.Contains(t, valuePaths, "addresses.postcode")

	assert.Len(t, result.Idx, 14)
	assert.Len(t, result.Exists, 17)
}

func assertAnalyzeDoc124(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_124",
		"owner": map[string]any{
			"firstname": "Justin", "lastname": "Time",
			"nicknames": []any{"watch"},
		},
		"addresses": []any{
			map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
			map[string]any{"city": "London", "postcode": "SW1"},
		},
		"tags": []any{"german", "japanese", "sedan"},
		"cars": []any{
			map[string]any{
				"make": "Audi",
				"tires": []any{
					map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
					map[string]any{"width": float64(225)},
				},
			},
			map[string]any{
				"make":   "Kia",
				"tires":  []any{map[string]any{"width": float64(195), "radiuses": []any{}}},
				"colors": []any{"white"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 1, valuePaths["owner.nicknames"])
	assert.Equal(t, 1, valuePaths["addresses.numbers"])
	assert.Equal(t, 2, valuePaths["addresses.city"])
	assert.Equal(t, 2, valuePaths["addresses.postcode"])
	assert.Equal(t, 3, valuePaths["tags"])
	assert.Equal(t, 2, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 3, valuePaths["cars.tires.width"])
	assert.Equal(t, 2, valuePaths["cars.make"])
	assert.Equal(t, 1, valuePaths["cars.colors"])

	assert.Len(t, result.Idx, 16)
	assert.Len(t, result.Exists, 23)
}

func assertAnalyzeDoc125(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := map[string]any{
		"name": "subdoc_125",
		"owner": map[string]any{
			"firstname": "Anna", "lastname": "Wanna",
		},
		"addresses": []any{
			map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
		},
		"tags": []any{"electric"},
		"cars": []any{
			map[string]any{
				"make":        "Tesla",
				"tires":       []any{map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}}},
				"accessories": []any{map[string]any{"type": "charger"}, map[string]any{"type": "mats"}},
				"colors":      []any{"yellow"},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	assert.Equal(t, 0, valuePaths["owner.nicknames"])
	assert.Equal(t, 1, valuePaths["addresses.numbers"])
	assert.Equal(t, 1, valuePaths["tags"])
	assert.Equal(t, 3, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 1, valuePaths["cars.tires.width"])
	assert.Equal(t, 2, valuePaths["cars.accessories.type"])
	assert.Equal(t, 1, valuePaths["cars.colors"])
	assert.Equal(t, 1, valuePaths["cars.make"])

	assert.Len(t, result.Idx, 12)
	assert.Len(t, result.Exists, 19)
}

func assertAnalyzeDoc999(t *testing.T, analyzer *Analyzer, prop *models.Property) {
	t.Helper()
	value := []any{
		map[string]any{
			"name": "subdoc_124",
			"owner": map[string]any{
				"firstname": "Justin", "lastname": "Time",
				"nicknames": []any{"watch"},
			},
			"addresses": []any{
				map[string]any{"city": "Madrid", "postcode": "28001", "numbers": []any{float64(124)}},
				map[string]any{"city": "London", "postcode": "SW1"},
			},
			"tags": []any{"german", "japanese", "sedan"},
			"cars": []any{
				map[string]any{
					"make": "Audi",
					"tires": []any{
						map[string]any{"width": float64(205), "radiuses": []any{float64(17), float64(18)}},
						map[string]any{"width": float64(225)},
					},
				},
				map[string]any{
					"make":   "Kia",
					"tires":  []any{map[string]any{"width": float64(195), "radiuses": []any{}}},
					"colors": []any{"white"},
				},
			},
		},
		map[string]any{
			"name": "subdoc_125",
			"owner": map[string]any{
				"firstname": "Anna", "lastname": "Wanna",
			},
			"addresses": []any{
				map[string]any{"city": "Paris", "postcode": "75001", "numbers": []any{float64(125)}},
			},
			"tags": []any{"electric"},
			"cars": []any{
				map[string]any{
					"make":        "Tesla",
					"tires":       []any{map[string]any{"width": float64(245), "radiuses": []any{float64(18), float64(19), float64(20)}}},
					"accessories": []any{map[string]any{"type": "charger"}, map[string]any{"type": "mats"}},
					"colors":      []any{"yellow"},
				},
			},
		},
	}

	result, err := analyzer.analyzeNestedProp(prop, value)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, result.HasFilterableIndex)
	assert.True(t, result.HasSearchableIndex)
	assert.False(t, result.HasRangeableIndex)

	valuePaths := collectValuePaths(result)
	// "subdoc_124"/"subdoc_125" tokenize to 2 tokens each
	assert.Equal(t, 4, valuePaths["name"])
	assert.Equal(t, 2, valuePaths["owner.firstname"])
	assert.Equal(t, 2, valuePaths["owner.lastname"])
	assert.Equal(t, 1, valuePaths["owner.nicknames"])
	assert.Equal(t, 3, valuePaths["addresses.city"])
	assert.Equal(t, 3, valuePaths["addresses.postcode"])
	assert.Equal(t, 2, valuePaths["addresses.numbers"])
	assert.Equal(t, 4, valuePaths["tags"])
	assert.Equal(t, 3, valuePaths["cars.make"])
	assert.Equal(t, 4, valuePaths["cars.tires.width"])
	assert.Equal(t, 5, valuePaths["cars.tires.radiuses"])
	assert.Equal(t, 2, valuePaths["cars.colors"])
	assert.Equal(t, 2, valuePaths["cars.accessories.type"])

	assert.Len(t, result.Idx, 28)
	assert.Len(t, result.Exists, 41)
}

func collectValuePaths(result *NestedProperty) map[string]int {
	paths := map[string]int{}
	for _, v := range result.Values {
		paths[v.Path]++
	}
	return paths
}
