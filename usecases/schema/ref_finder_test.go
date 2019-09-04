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

	t.Run("on an schema containing a single level ref to the target", func(t *testing.T) {
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
				Property: "Vehicle",
				Child: &filters.Path{
					Class:    "Car",
					Property: "uuid",
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
