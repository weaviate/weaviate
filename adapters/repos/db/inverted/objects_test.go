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
		res, err := a.Object(schema, props)
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

		require.Len(t, res, 2)
		var actualDescription []Countable
		var actualEmail []Countable

		for _, elem := range res {
			if elem.Name == "email" {
				actualEmail = elem.Items
			}

			if elem.Name == "description" {
				actualDescription = elem.Items
			}
		}

		assert.ElementsMatch(t, expectedEmail, actualEmail, res)
		assert.ElementsMatch(t, expectedDescription, actualDescription, res)
	})

	t.Run("with refProps", func(t *testing.T) {
		t.Run("with the ref set in the object schema", func(t *testing.T) {
			schema := map[string]interface{}{
				"myRef": models.MultipleRef{
					&models.SingleRef{
						Beacon: "weaviate://localhost/c563d7fa-4a36-4eff-9f39-af1e1db276c4",
					},
				},
			}

			props := []*models.Property{
				&models.Property{
					Name:     "myRef",
					DataType: []string{"RefClass"},
				},
			}
			res, err := a.Object(schema, props)
			require.Nil(t, err)

			expectedRefCount := []Countable{
				Countable{
					Data: []uint8{0x00, 0x00, 0x00, 0x01}, // 1 as uint32
				},
			}

			require.Len(t, res, 1)
			var actualRefCount []Countable

			for _, elem := range res {
				if elem.Name == helpers.MetaCountProp("myRef") {
					actualRefCount = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
		})

		t.Run("with the ref omitted in the object schema", func(t *testing.T) {
			schema := map[string]interface{}{}

			props := []*models.Property{
				&models.Property{
					Name:     "myRef",
					DataType: []string{"RefClass"},
				},
			}
			res, err := a.Object(schema, props)
			require.Nil(t, err)

			expectedRefCount := []Countable{
				Countable{
					Data: []uint8{0x00, 0x00, 0x00, 0x00}, // 0 as uint32
				},
			}

			require.Len(t, res, 1)
			var actualRefCount []Countable

			for _, elem := range res {
				if elem.Name == helpers.MetaCountProp("myRef") {
					actualRefCount = elem.Items
				}
			}

			assert.ElementsMatch(t, expectedRefCount, actualRefCount, res)
		})
	})
}
