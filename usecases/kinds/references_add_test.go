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

package kinds

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_ReferencesAdd(t *testing.T) {

	logger, _ := test.NewNullLogger()

	var (
		vectorRepo    *fakeVectorRepo
		schemaManager *fakeSchemaManager
		locks         *fakeLocks
		network       *fakeNetwork
		cfg           *config.WeaviateConfig
		manager       *Manager
		authorizer    *fakeAuthorizer
		vectorizer    *fakeVectorizer
	)

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		schemaManager = &fakeSchemaManager{}
		locks = &fakeLocks{}
		network = &fakeNetwork{}
		cfg = &config.WeaviateConfig{}
		authorizer = &fakeAuthorizer{}
		vectorizer = &fakeVectorizer{}
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		manager = NewManager(locks, schemaManager, network,
			cfg, logger, authorizer, vectorizer, vectorRepo, extender, projector)
	}

	t.Run("without prior refs", func(t *testing.T) {
		reset()
		vectorRepo.On("Exists", mock.Anything).Return(true, nil)
		vectorRepo.On("ThingByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "Zoo",
			Schema: map[string]interface{}{
				"name": "MyZoo",
			},
		}, nil)
		schemaManager.GetSchemaResponse = zooAnimalSchemaForTest()
		newRef := &models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedRef := &models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/things/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedRefProperty := "hasAnimals"
		vectorRepo.On("AddReference", kind.Thing, mock.Anything, expectedRefProperty, expectedRef).Return(nil)

		err := manager.AddThingReference(context.Background(), nil, strfmt.UUID("my-id"), "hasAnimals", newRef)
		require.Nil(t, err)
		vectorRepo.AssertExpectations(t)
	})
}

func zooAnimalSchemaForTest() schema.Schema {
	return schema.Schema{
		Actions: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "ZooAction",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
						&models.Property{
							Name:     "area",
							DataType: []string{"number"},
						},
						&models.Property{
							Name:     "employees",
							DataType: []string{"int"},
						},
						&models.Property{
							Name:     "located",
							DataType: []string{"geoCoordinates"},
						},
						&models.Property{
							Name:     "foundedIn",
							DataType: []string{"date"},
						},
						&models.Property{
							Name:     "hasAnimals",
							DataType: []string{"AnimalAction"},
						},
					},
				},
				&models.Class{
					Class: "AnimalAction",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
			},
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
							Name:     "area",
							DataType: []string{"number"},
						},
						&models.Property{
							Name:     "employees",
							DataType: []string{"int"},
						},
						&models.Property{
							Name:     "located",
							DataType: []string{"geoCoordinates"},
						},
						&models.Property{
							Name:     "foundedIn",
							DataType: []string{"date"},
						},
						&models.Property{
							Name:     "hasAnimals",
							DataType: []string{"Animal"},
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
