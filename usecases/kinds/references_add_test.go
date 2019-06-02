/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ReferencesAdd_CardinalityMany(t *testing.T) {

	logger, _ := test.NewNullLogger()

	var (
		repo          *fakeRepo
		schemaManager *fakeSchemaManager
		locks         *fakeLocks
		network       *fakeNetwork
		cfg           *config.WeaviateConfig
		manager       *Manager
		authorizer    *fakeAuthorizer
		vectorizer    *fakeVectorizer
	)

	reset := func() {
		repo = &fakeRepo{}
		schemaManager = &fakeSchemaManager{}
		locks = &fakeLocks{}
		network = &fakeNetwork{}
		cfg = &config.WeaviateConfig{}
		authorizer = &fakeAuthorizer{}
		vectorizer = &fakeVectorizer{}
		manager = NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer, vectorizer)
	}

	t.Run("without prior refs", func(t *testing.T) {
		reset()
		repo.GetThingResponse = &models.Thing{
			Class: "Zoo",
			Schema: map[string]interface{}{
				"name": "MyZoo",
			},
		}
		schemaManager.GetSchemaResponse = zooAnimalSchemaForTest()
		newRef := &models.SingleRef{
			NrDollarCref: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedSchema := map[string]interface{}{
			"name": "MyZoo",
			"hasAnimals": []interface{}{
				&models.SingleRef{
					NrDollarCref: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
				},
			},
		}

		err := manager.AddThingReference(context.Background(), nil, strfmt.UUID("my-id"), "hasAnimals", newRef)

		require.Nil(t, err)
		assert.Equal(t, expectedSchema, repo.UpdateThingParameter.Schema)
	})

	t.Run("adding a second ref when one already exists", func(t *testing.T) {
		reset()
		repo.GetThingResponse = &models.Thing{
			Class: "Zoo",
			Schema: map[string]interface{}{
				"name": "MyZoo",
				"hasAnimals": []interface{}{
					&models.SingleRef{
						NrDollarCref: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
					},
				},
			},
		}
		schemaManager.GetSchemaResponse = zooAnimalSchemaForTest()
		newRef := &models.SingleRef{
			NrDollarCref: strfmt.URI("weaviate://localhost/things/1dd50566-93ce-4f68-81a2-2dfff2ab7835"),
		}
		expectedSchema := map[string]interface{}{
			"name": "MyZoo",
			"hasAnimals": []interface{}{
				&models.SingleRef{
					NrDollarCref: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
				},
				&models.SingleRef{
					NrDollarCref: strfmt.URI("weaviate://localhost/things/1dd50566-93ce-4f68-81a2-2dfff2ab7835"),
				},
			},
		}

		err := manager.AddThingReference(context.Background(), nil, strfmt.UUID("my-id"), "hasAnimals", newRef)

		require.Nil(t, err)
		assert.Equal(t, expectedSchema, repo.UpdateThingParameter.Schema)
	})
}

func zooAnimalSchemaForTest() schema.Schema {
	many := "many"
	return schema.Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "Zoo",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "name",
							DataType: []string{"string"},
						},
						&models.SemanticSchemaClassProperty{
							Name:        "hasAnimals",
							DataType:    []string{"Animal"},
							Cardinality: &many,
						},
					},
				},
				&models.SemanticSchemaClass{
					Class: "Animal",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
			},
		},
	}

}
