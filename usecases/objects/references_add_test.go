//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
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
		cfg           *config.WeaviateConfig
		manager       *Manager
		authorizer    *fakeAuthorizer
		vectorizer    *fakeVectorizer
	)

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		schemaManager = &fakeSchemaManager{}
		locks = &fakeLocks{}
		cfg = &config.WeaviateConfig{}
		authorizer = &fakeAuthorizer{}
		vectorizer = &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager,
			cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
	}

	t.Run("without prior refs", func(t *testing.T) {
		reset()
		vectorRepo.On("Exists", mock.Anything).Return(true, nil)
		vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "Zoo",
			Schema: map[string]interface{}{
				"name": "MyZoo",
			},
		}, nil)
		schemaManager.GetSchemaResponse = zooAnimalSchemaForTest()
		newRef := &models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedRef := &models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
		}
		expectedRefProperty := "hasAnimals"
		vectorRepo.On("AddReference", mock.Anything, expectedRefProperty, expectedRef).Return(nil)

		err := manager.AddObjectReference(context.Background(), nil, strfmt.UUID("my-id"), "hasAnimals", newRef)
		require.Nil(t, err)
		vectorRepo.AssertExpectations(t)
	})
}

func zooAnimalSchemaForTest() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class:             "ZooAction",
					VectorIndexConfig: hnsw.UserConfig{},
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
					Class:             "AnimalAction",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
				&models.Class{
					Class:             "Zoo",
					VectorIndexConfig: hnsw.UserConfig{},
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
					Class:             "Animal",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{"string"},
						},
					},
				},
				&models.Class{
					Class:             "NotVectorized",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						&models.Property{
							Name:     "description",
							DataType: []string{"text"},
						},
					},
					Vectorizer: "none",
				},
			},
		},
	}
}
