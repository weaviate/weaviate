// +build integrationTest

package esvector

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
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
					Class: "MyClass",
				},
			},
			{
				name: "thing class with a string prop",
				kind: kind.Thing,
				class: &models.SemanticSchemaClass{
					Class: "MyClass",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
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

}
