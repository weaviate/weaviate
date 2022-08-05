//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeObject(t *testing.T) {
	a := NewAnalyzer(fakeStopwordDetector{})

	t.Run("with multiple properties", func(t *testing.T) {
		schema := map[string]interface{}{
			"description": "I am great!",
			"email":       "john@doe.com",
			"about_me":    "I like reading sci-fi books",
			"profession":  "Mechanical Engineer",
		}

		uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
		props := []*models.Property{
			{
				Name:         "description",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
			{
				Name:         "email",
				DataType:     []string{"string"},
				Tokenization: "word",
			},
			{
				Name:         "about_me",
				DataType:     []string{"string"},
				Tokenization: "word",
			},
			{
				Name:         "profession",
				DataType:     []string{"string"},
				Tokenization: "field",
			},
		}
		res, err := a.Object(schema, props, strfmt.UUID(uuid))
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
				Data:          []byte("I"),
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

		require.Len(t, res, 5)
		var actualDescription []Countable
		var actualEmail []Countable
		var actualAboutMe []Countable
		var actualProfession []Countable
		var actualUUID []Countable

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
		}

		assert.ElementsMatch(t, expectedEmail, actualEmail, res)
		assert.ElementsMatch(t, expectedDescription, actualDescription, res)
		assert.ElementsMatch(t, expectedAboutMe, actualAboutMe, res)
		assert.ElementsMatch(t, expectedProfession, actualProfession, res)
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

		t.Run("with array properties", func(t *testing.T) {
			schema := map[string]interface{}{
				"descriptions": []interface{}{"I am great!", "I am also great!"},
				"emails":       []interface{}{"john@doe.com", "john2@doe.com"},
				"about_me":     []interface{}{"I like reading sci-fi books", "I like playing piano"},
				"professions":  []interface{}{"Mechanical Engineer", "	Marketing Analyst"},
				"integers":     []interface{}{int64(1), int64(2), int64(3), int64(4)},
				"numbers":      []interface{}{float64(1.1), float64(2.2), float64(3.0), float64(4)},
			}

			uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
			props := []*models.Property{
				{
					Name:         "descriptions",
					DataType:     []string{"text[]"},
					Tokenization: "word",
				},
				{
					Name:         "emails",
					DataType:     []string{"string[]"},
					Tokenization: "word",
				},
				{
					Name:         "about_me",
					DataType:     []string{"string[]"},
					Tokenization: "word",
				},
				{
					Name:         "professions",
					DataType:     []string{"string[]"},
					Tokenization: "field",
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
			res, err := a.Object(schema, props, strfmt.UUID(uuid))
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
					Data:          []byte("I"),
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
	})

	t.Run("when objects are indexed by timestamps", func(t *testing.T) {
		schema := map[string]interface{}{
			"description":         "pretty ok if you ask me",
			"_creationTimeUnix":   1650551406404,
			"_lastUpdateTimeUnix": 1650551406404,
		}

		uuid := strfmt.UUID("2609f1bc-7693-48f3-b531-6ddc52cd2501")
		props := []*models.Property{
			{
				Name:         "description",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
		}

		res, err := a.Object(schema, props, uuid)
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
				HasFrequency: true,
			},
			{
				Name:  "_id",
				Items: []Countable{{Data: []byte("2609f1bc-7693-48f3-b531-6ddc52cd2501")}},
			},
			{
				Name:  "_creationTimeUnix",
				Items: []Countable{{Data: []byte("1650551406404")}},
			},
			{
				Name:  "_lastUpdateTimeUnix",
				Items: []Countable{{Data: []byte("1650551406404")}},
			},
		}

		for i := range res {
			assert.Equal(t, expected[i].Name, res[i].Name)
			assert.Equal(t, expected[i].HasFrequency, res[i].HasFrequency)
			assert.ElementsMatch(t, expected[i].Items, res[i].Items)
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
	out, err := LexicographicallySortableFloat64(float64(in))
	if err != nil {
		panic(err)
	}
	return out
}
