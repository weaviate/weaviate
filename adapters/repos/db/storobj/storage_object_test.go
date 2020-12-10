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

package storobj

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageObjectMarshalling(t *testing.T) {
	before := FromThing(
		&models.Thing{
			Class:              "MyFavoriteClass",
			CreationTimeUnix:   123456,
			LastUpdateTimeUnix: 56789,
			ID:                 strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
			Meta: &models.UnderscoreProperties{
				Classification: &models.UnderscorePropertiesClassification{
					BasedOn: []string{"some", "fields"},
				},
			},
			Schema: map[string]interface{}{
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
}

func TestNewStorageObject(t *testing.T) {
	t.Run("things", func(t *testing.T) {
		so := New(kind.Thing, 12)

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
			so.SetSchema(schema)
			assert.Equal(t, schema, so.Schema())
		})

		t.Run("must now be valid", func(t *testing.T) {
			assert.True(t, so.Valid())
		})

		t.Run("make sure it's identical with an object created from an existing thing",
			func(t *testing.T) {
				alt := FromThing(&models.Thing{
					Class: "MyClass",
					ID:    "bf706904-8618-463f-899c-4a2aafd48d56",
					Schema: map[string]interface{}{
						"foo": "bar",
					},
				}, nil)
				alt.SetDocID(13)

				assert.Equal(t, so, alt)
			})
	})

	t.Run("actions", func(t *testing.T) {
		so := New(kind.Action, 12)

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
			so.SetSchema(schema)
			assert.Equal(t, schema, so.Schema())
		})

		t.Run("must now be valid", func(t *testing.T) {
			assert.True(t, so.Valid())
		})

		t.Run("make sure it's identical with an object created from an existing action",
			func(t *testing.T) {
				alt := FromAction(&models.Action{
					Class: "MyClass",
					ID:    "bf706904-8618-463f-899c-4a2aafd48d56",
					Schema: map[string]interface{}{
						"foo": "bar",
					},
				}, nil)
				alt.SetDocID(13)

				assert.Equal(t, so, alt)
			})
	})
}
