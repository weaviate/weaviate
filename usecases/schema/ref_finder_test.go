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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestRefFinder(t *testing.T) {
	t.Run("on an empty schema", func(t *testing.T) {
		s := schema.Schema{
			Objects: &models.Schema{
				Classes: nil,
			},
		}

		getter := &fakeSchemaGetterForRefFinder{s}
		res := NewRefFinder(getter, 3).Find("Car")

		assert.Len(t, res, 0)
	})

	t.Run("on an schema containing only the target class and unrelated classes", func(t *testing.T) {
		s := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{
								Name:     "model",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					},
					{
						Class: "Tree",
						Properties: []*models.Property{
							{
								Name:     "kind",
								DataType: schema.DataTypeText.PropString(),
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
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{
								Name:     "model",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					},
					{
						Class: "Tree",
						Properties: []*models.Property{
							{
								Name:     "kind",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					},
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					},
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: schema.DataTypeText.PropString(),
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
					Property: "id",
				},
			},
		}, res)
	})

	t.Run("on a schema containing a single level and a multi level ref to the target", func(t *testing.T) {
		s := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Dog",
						Properties: []*models.Property{
							{
								Name:     "name",
								DataType: schema.DataTypeText.PropString(),
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
								DataType: schema.DataTypeText.PropString(),
							},
						},
					},
					{
						Class: "Person",
						Properties: []*models.Property{
							{
								Name:     "name",
								DataType: schema.DataTypeText.PropString(),
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
					{
						Class: "Drive",
						Properties: []*models.Property{
							{
								Name:     "destination",
								DataType: schema.DataTypeText.PropString(),
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
					Property: "id",
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
						Property: "id",
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
							Property: "id",
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
