//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
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
	a := NewAnalyzer()

	t.Run("with multiple properties", func(t *testing.T) {
		schema := map[string]interface{}{
			"description": "I am great!",
			"email":       "john@doe.com",
		}

		uuid := "2609f1bc-7693-48f3-b531-6ddc52cd2501"
		props := []*models.Property{
			&models.Property{
				Name:     "description",
				DataType: []string{"text"},
			},
			&models.Property{
				Name:     "email",
				DataType: []string{"string"},
			},
		}
		res, err := a.Object(schema, props, strfmt.UUID(uuid))
		require.Nil(t, err)

		expectedDescription := []Countable{
			Countable{
				Data:          []byte("i"),
				TermFrequency: float32(1) / 3,
			},
			Countable{
				Data:          []byte("am"),
				TermFrequency: float32(1) / 3,
			},
			Countable{
				Data:          []byte("great"),
				TermFrequency: float32(1) / 3,
			},
		}

		expectedEmail := []Countable{
			Countable{
				Data:          []byte("john@doe.com"),
				TermFrequency: float32(1) / 1,
			},
		}

		expectedUUID := []Countable{
			Countable{
				Data:          []byte(uuid),
				TermFrequency: 0,
			},
		}

		require.Len(t, res, 3)
		var actualDescription []Countable
		var actualEmail []Countable
		var actualUUID []Countable

		for _, elem := range res {
			if elem.Name == "email" {
				actualEmail = elem.Items
			}

			if elem.Name == "description" {
				actualDescription = elem.Items
			}

			if elem.Name == "uuid" {
				actualUUID = elem.Items
			}
		}

		assert.ElementsMatch(t, expectedEmail, actualEmail, res)
		assert.ElementsMatch(t, expectedDescription, actualDescription, res)
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
				{Data: []uint8{0x00, 0x00, 0x00, 0x01}}, // 1 as uint32
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
				case "uuid":
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
				{Data: []uint8{0x00, 0x00, 0x00, 0x02}}, // 2 as uint32
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
				case "uuid":
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
				{Data: []uint8{0x00, 0x00, 0x00, 0x00}}, // 0 as uint32
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
				if elem.Name == "uuid" {
					actualUUID = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
			assert.ElementsMatch(t, expectedUUID, actualUUID, res)
		})
	})
}
