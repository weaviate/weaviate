// +build integrationTest

package esvector

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEsVectorMigrator(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	waitForEsToBeReady(t, client)
	repo := NewRepo(client)
	migrator := NewMigrator(repo)

	t.Run("adding a class", func(t *testing.T) {
		type testCase struct {
			name  string
			kind  kind.Kind
			class *models.SemanticSchemaClass
		}

		tests := []testCase{
			{
				name: "thing class without props",
				kind: kind.Thing,
				class: &models.SemanticSchemaClass{
					Class: "MyThingClass",
				},
			},
			{
				name: "thing class with a string prop",
				kind: kind.Thing,
				class: &models.SemanticSchemaClass{
					Class: "MyThingClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
			},
			{
				name: "action class with an int prop",
				kind: kind.Action,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "age",
							DataType: []string{string(schema.DataTypeInt)},
						},
					},
				},
			},
			{
				name: "action class with an float prop",
				kind: kind.Action,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "weight",
							DataType: []string{string(schema.DataTypeNumber)},
						},
					},
				},
			},
			{
				name: "action class with bool prop",
				kind: kind.Action,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "awesome",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
					},
				},
			},
			{
				name: "action class with text, date and geo prop",
				kind: kind.Action,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "content",
							DataType: []string{string(schema.DataTypeText)},
						},
						&models.SemanticSchemaClassProperty{
							Name:     "date",
							DataType: []string{string(schema.DataTypeDate)},
						},
						&models.SemanticSchemaClassProperty{
							Name:     "location",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
					},
				},
			},
			{
				name: "action class with a ref prop",
				kind: kind.Action,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "awesome",
							DataType: []string{"SomeClass"},
						},
					},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				err := migrator.AddClass(context.Background(), test.kind, test.class)
				require.Nil(t, err)
			})

		}

	})

	t.Run("deleting a previously created class", func(t *testing.T) {
		err := migrator.DropClass(context.Background(), kind.Thing, "MyThingClass")
		assert.Nil(t, err)
		err = migrator.DropClass(context.Background(), kind.Action, "MyClass")
		assert.Nil(t, err)
	})

}
