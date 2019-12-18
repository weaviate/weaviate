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

// +build integrationTest

package esvector

import (
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// this is not a test suite in itself, but it makes sure that other test suites
// which depend no a specific caching state are run sequentially in a specific
// order, so that they don't interefere with each other

func Test_OrchestrateCaching(t *testing.T) {

	// requires caching to be stopped initially, starts caching at some point in
	// the test suite, stops caching in the end to clean up
	testEsVectorCache(t)

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)
	schemaGetter := &fakeSchemaGetter{schema: parkingGaragesSchema()}
	logger := logrus.New()
	repo := NewRepo(client, logger, schemaGetter, 2, 100, 1, "0-1")
	waitForEsToBeReady(t, repo)
	requestCounter := &testCounter{}
	repo.requestCounter = requestCounter
	migrator := NewMigrator(repo)

	// cache indexing not started yet, as the below test suite asserts on both state
	t.Run("test multiple cross ref types", testMultipleCrossRefTypes(repo, migrator))

	// cache indexing has since been started in the previous test suite
	t.Run("filtering on refprops", testFilteringOnRefProps(repo))

	t.Run("updating cached ref props", testUpdatingCachedRefProps(repo, parkingGaragesSchema()))

	t.Run("classifications", testClassifications(repo, migrator))

	// explicitly stop cache indexing to clean up
	repo.StopCacheIndexing()
}

func parkingGaragesSchema() schema.Schema {
	return schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class: "MultiRefParkingGarage",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "location",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
					},
				},
				&models.Class{
					Class: "MultiRefParkingLot",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
				&models.Class{
					Class: "MultiRefCar",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "parkedAt",
							DataType: []string{"MultiRefParkingGarage", "MultiRefParkingLot"},
						},
					},
				},
				&models.Class{
					Class: "MultiRefDriver",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "drives",
							DataType: []string{"MultiRefCar"},
						},
					},
				},
				&models.Class{
					Class: "MultiRefPerson",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "friendsWith",
							DataType: []string{"MultiRefDriver"},
						},
					},
				},
				&models.Class{
					Class: "MultiRefSociety",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						&models.Property{
							Name:     "hasMembers",
							DataType: []string{"MultiRefPerson"},
						},
					},
				},

				// for classifications test
				&models.Class{
					Class: "ExactCategory",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
				&models.Class{
					Class: "MainCategory",
					Properties: []*models.Property{
						&models.Property{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
			},
		},
	}
}
