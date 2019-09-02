//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		repo.On("ClassExists", mock.Anything).Return(true, nil)
		schemaManager = &fakeSchemaManager{}
		locks = &fakeLocks{}
		network = &fakeNetwork{}
		cfg = &config.WeaviateConfig{}
		authorizer = &fakeAuthorizer{}
		vectorizer = &fakeVectorizer{}
		vectorRepo := &fakeVectorRepo{}
		manager = NewManager(repo, locks, schemaManager, network,
			cfg, logger, authorizer, vectorizer, vectorRepo)
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
			Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedSchema := map[string]interface{}{
			"name": "MyZoo",
			"hasAnimals": []interface{}{
				&models.SingleRef{
					Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
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
						Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
					},
				},
			},
		}
		schemaManager.GetSchemaResponse = zooAnimalSchemaForTest()
		newRef := &models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/things/1dd50566-93ce-4f68-81a2-2dfff2ab7835"),
		}
		expectedSchema := map[string]interface{}{
			"name": "MyZoo",
			"hasAnimals": []interface{}{
				&models.SingleRef{
					Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
				},
				&models.SingleRef{
					Beacon: strfmt.URI("weaviate://localhost/things/1dd50566-93ce-4f68-81a2-2dfff2ab7835"),
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
		Actions: &models.Schema{
			Classes: []*models.Class{},
		},
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "Zoo",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
						&models.Property{
							Name:        "hasAnimals",
							DataType:    []string{"Animal"},
							Cardinality: &many,
						},
					},
				},
				&models.Class{
					Class: "Animal",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
			},
		},
	}

}
