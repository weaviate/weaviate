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

package schema

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/assert"
)

func TestRefFinder(t *testing.T) {
	t.Run("on an empty schema", func(t *testing.T) {
		s := schema.Schema{
			Actions: &models.Schema{
				Classes: nil,
			},
			Things: &models.Schema{
				Classes: nil,
			},
		}

		getter := &fakeSchemaGetterForRefFinder{s}
		res := NewRefFinder(getter, 3).Find("Car")

		assert.Len(t, res, 0)
	})

	t.Run("on an schema containing only the target class and unrelated classes", func(t *testing.T) {
		s := schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{
								Name:     "model",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
					{
						Class: "Tree",
						Properties: []*models.Property{
							{
								Name:     "kind",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
				},
			},
			Actions: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
				},
			},
		}

		getter := &fakeSchemaGetterForRefFinder{s}
		res := NewRefFinder(getter, 3).Find("Car")

		assert.Len(t, res, 0)
	})

	t.Run("on a schema containing a single level ref to the target", func(t *testing.T) {
		s := schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{
								Name:     "model",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
					{
						Class: "Tree",
						Properties: []*models.Property{
							{
								Name:     "kind",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
				},
			},
			Actions: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: []string{string(schema.DataTypeString)},
							},
							{
								Name:     "vehicle",
								DataType: []string{"Car"},
							},
						},
					},
				},
			},
		}

		getter := &fakeSchemaGetterForRefFinder{s}
		res := NewRefFinder(getter, 3).Find("Car")

		assert.Equal(t, []filters.Path{
			{
				Class:    "Drive",
				Property: "vehicle",
				Child: &filters.Path{
					Class:    "Car",
					Property: "uuid",
				},
			},
		}, res)
	})

	t.Run("on a schema containing a single level and a multi level ref to the target", func(t *testing.T) {
		s := schema.Schema{
			Things: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Dog",
						Properties: []*models.Property{
							{
								Name:     "name",
								DataType: []string{string(schema.DataTypeString)},
							},
							{
								Name:     "hasOwner",
								DataType: []string{"Person"},
							},
						},
					},
					{
						Class: "Car",
						Properties: []*models.Property{
							{
								Name:     "model",
								DataType: []string{string(schema.DataTypeString)},
							},
						},
					},
					{
						Class: "Person",
						Properties: []*models.Property{
							{
								Name:     "name",
								DataType: []string{string(schema.DataTypeString)},
							},
							{
								Name:     "travels",
								DataType: []string{"Drive"},
							},
							{
								Name:     "hasPets",
								DataType: []string{"Dog"},
							},
						},
					},
				},
			},
			Actions: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: []string{string(schema.DataTypeString)},
							},
							{
								Name:     "vehicle",
								DataType: []string{"Car"},
							},
						},
					},
				},
			},
		}

		getter := &fakeSchemaGetterForRefFinder{s}
		res := NewRefFinder(getter, 3).Find("Car")

		assert.Equal(t, []filters.Path{
			{
				Class:    "Drive",
				Property: "vehicle",
				Child: &filters.Path{
					Class:    "Car",
					Property: "uuid",
				},
			},
			{
				Class:    "Person",
				Property: "travels",
				Child: &filters.Path{
					Class:    "Drive",
					Property: "vehicle",
					Child: &filters.Path{
						Class:    "Car",
						Property: "uuid",
					},
				},
			},
			{
				Class:    "Dog",
				Property: "hasOwner",
				Child: &filters.Path{
					Class:    "Person",
					Property: "travels",
					Child: &filters.Path{
						Class:    "Drive",
						Property: "vehicle",
						Child: &filters.Path{
							Class:    "Car",
							Property: "uuid",
						},
					},
				},
			},
		}, res)
	})
}

type fakeSchemaGetterForRefFinder struct {
	schema schema.Schema
}

func (f *fakeSchemaGetterForRefFinder) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}
