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
	"context"
	"fmt"
	"testing"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoCache(t *testing.T) {
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

	t.Run("adding all classes to the schema", func(t *testing.T) {
		for _, class := range parkingGaragesSchema().Things.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Thing, class)
				require.Nil(t, err)
			})
		}
	})

	t.Run("importing with various combinations of props", func(t *testing.T) {
		objects := []models.Thing{
			models.Thing{
				Class: "MultiRefParkingGarage",
				Schema: map[string]interface{}{
					"name": "Luxury Parking Garage",
					"location": &models.GeoCoordinates{
						Latitude:  48.864716,
						Longitude: 2.349014,
					},
				},
				ID:               "a7e10b55-1ac4-464f-80df-82508eea1951",
				CreationTimeUnix: 1566469890,
			},
			models.Thing{
				Class: "MultiRefParkingGarage",
				Schema: map[string]interface{}{
					"name": "Crappy Parking Garage",
					"location": &models.GeoCoordinates{
						Latitude:  42.331429,
						Longitude: -83.045753,
					},
				},
				ID:               "ba2232cf-bb0e-413d-b986-6aa996d34d2e",
				CreationTimeUnix: 1566469892,
			},
			models.Thing{
				Class: "MultiRefParkingLot",
				Schema: map[string]interface{}{
					"name": "Fancy Parking Lot",
				},
				ID:               "1023967b-9512-475b-8ef9-673a110b695d",
				CreationTimeUnix: 1566469894,
			},
			models.Thing{
				Class: "MultiRefParkingLot",
				Schema: map[string]interface{}{
					"name": "The worst parking lot youve ever seen",
				},
				ID:               "901859d8-69bf-444c-bf43-498963d798d2",
				CreationTimeUnix: 1566469897,
			},
			models.Thing{
				Class: "MultiRefCar",
				Schema: map[string]interface{}{
					"name": "Car which is parked no where",
				},
				ID:               "329c306b-c912-4ec7-9b1d-55e5e0ca8dea",
				CreationTimeUnix: 1566469899,
			},
			models.Thing{
				Class: "MultiRefCar",
				Schema: map[string]interface{}{
					"name": "Car which is parked in a garage",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
					},
				},
				ID:               "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43",
				CreationTimeUnix: 1566469902,
			},
			models.Thing{
				Class: "MultiRefCar",
				Schema: map[string]interface{}{
					"name": "Car which is parked in a lot",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				},
				ID:               "21ab5130-627a-4268-baef-1a516bd6cad4",
				CreationTimeUnix: 1566469906,
			},
			models.Thing{
				Class: "MultiRefCar",
				Schema: map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				},
				ID:               "533673a7-2a5c-4e1c-b35d-a3809deabace",
				CreationTimeUnix: 1566469909,
			},
			models.Thing{
				Class: "MultiRefDriver",
				Schema: map[string]interface{}{
					"name": "Johny Drivemuch",
					"drives": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/533673a7-2a5c-4e1c-b35d-a3809deabace",
						},
					},
				},
				ID:               "9653ab38-c16b-4561-80df-7a7e19300dd0",
				CreationTimeUnix: 1566469912,
			},
			models.Thing{
				Class: "MultiRefPerson",
				Schema: map[string]interface{}{
					"name": "Jane Doughnut",
					"friendsWith": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/9653ab38-c16b-4561-80df-7a7e19300dd0",
						},
					},
				},
				ID:               "91ad23a3-07ba-4d4c-9836-76c57094f734",
				CreationTimeUnix: 1566469915,
			},
			models.Thing{
				Class: "MultiRefSociety",
				Schema: map[string]interface{}{
					"name": "Cool People",
					"hasMembers": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/91ad23a3-07ba-4d4c-9836-76c57094f734",
						},
					},
				},
				ID:               "5cd9afa6-f3df-4f57-a204-840d6b256dba",
				CreationTimeUnix: 1566469918,
			},
		}

		for _, thing := range objects {
			t.Run(fmt.Sprintf("add %s", thing.ID), func(t *testing.T) {
				err := repo.PutThing(context.Background(), &thing, []float32{1, 2, 3, 4, 5, 6, 7})
				require.Nil(t, err)
			})
		}
	})

	refreshAll(t, repo.client)

	t.Run("filtering", func(t *testing.T) {
		t.Run("one level deep", func(t *testing.T) {
			t.Run("ref name matches", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "Luxury Parking Garage")
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 2)
			})

			t.Run("ref id matches", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeString,
					"uuid", filters.OperatorEqual, "a7e10b55-1ac4-464f-80df-82508eea1951")
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 2)
			})

			t.Run("ref name doesn't match", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "There is no parking garage with this name")
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("within geo range", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 2)

				names := extractNames(res)
				expectedNames := []string{
					"Car which is parked in a garage",
					"Car which is parked in two places at the same time (magic!)",
				}

				assert.ElementsMatch(t, names, expectedNames)
			})

			t.Run("outside of geo range", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  42.279594,
							Longitude: -83.732124,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("combining ref filter with primitive root filter", func(t *testing.T) {
				parkedAtFilter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})

				filter := &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorAnd,
						Operands: []filters.Clause{
							*(parkedAtFilter.Root),
							filters.Clause{
								On: &filters.Path{
									Class:    schema.ClassName("MultiRefCar"),
									Property: schema.PropertyName("name"),
								},
								Value: &filters.Value{
									Value: "Car which is parked in a garage",
									Type:  schema.DataTypeString,
								},
								Operator: filters.OperatorEqual,
							},
						},
					},
				}
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				names := extractNames(res)
				expectedNames := []string{
					"Car which is parked in a garage",
				}

				assert.ElementsMatch(t, names, expectedNames)
			})

		})

		t.Run("multiple levels deep", func(t *testing.T) {
			t.Run("ref name matches", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "Luxury Parking Garage")
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				assert.Equal(t, "Johny Drivemuch", res[0].Schema.(map[string]interface{})["name"])
			})

			t.Run("ref name doesn't match", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "There is no parking garage with this name")
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("within geo range", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				assert.Equal(t, "Johny Drivemuch", res[0].Schema.(map[string]interface{})["name"])
			})

			t.Run("outside of geo range", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  42.279594,
							Longitude: -83.732124,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

		})

		t.Run("by reference count", func(t *testing.T) {
			t.Run("equal to zero", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorEqual, 0)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1) // there is just one car parked nowhere
				assert.Equal(t, "Car which is parked no where", res[0].Schema.(map[string]interface{})["name"])
			})

			t.Run("equal to one", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorEqual, 1)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				expectedNames := []string{
					"Car which is parked in a garage",
					"Car which is parked in a lot",
				}
				assert.ElementsMatch(t, expectedNames, extractNames(res))
			})

			t.Run("equal to more than one", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorGreaterThan, 1)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				expectedNames := []string{
					"Car which is parked in two places at the same time (magic!)",
				}
				assert.ElementsMatch(t, expectedNames, extractNames(res))
			})

			t.Run("greater or equal one", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorGreaterThanEqual, 1)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				expectedNames := []string{
					"Car which is parked in a garage",
					"Car which is parked in a lot",
					"Car which is parked in two places at the same time (magic!)",
				}
				assert.ElementsMatch(t, expectedNames, extractNames(res))
			})

			t.Run("less than one", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorLessThan, 1)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				expectedNames := []string{
					"Car which is parked no where",
				}
				assert.ElementsMatch(t, expectedNames, extractNames(res))
			})

			t.Run("less than or equal one", func(t *testing.T) {
				filter := filterCarParkedCount(filters.OperatorLessThanEqual, 1)
				params := getParamsWithFilter("MultiRefCar", filter)
				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				expectedNames := []string{
					"Car which is parked in a garage",
					"Car which is parked in a lot",
					"Car which is parked no where",
				}
				assert.ElementsMatch(t, expectedNames, extractNames(res))
			})
		})
	})
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

func filterCarParkedAtGarage(dataType schema.DataType,
	prop string, operator filters.Operator, value interface{}) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("MultiRefCar"),
				Property: schema.PropertyName("parkedAt"),
				Child: &filters.Path{
					Class:    schema.ClassName("MultiRefParkingGarage"),
					Property: schema.PropertyName(prop),
				},
			},
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
		},
	}
}

func filterCarParkedCount(operator filters.Operator, value int) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("MultiRefCar"),
				Property: schema.PropertyName("parkedAt"),
			},
			Value: &filters.Value{
				Value: value,
				Type:  schema.DataTypeInt,
			},
		},
	}
}

func filterDrivesCarParkedAtGarage(dataType schema.DataType,
	prop string, operator filters.Operator, value interface{}) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("MultiRefDriver"),
				Property: schema.PropertyName("drives"),
				Child:    filterCarParkedAtGarage(dataType, prop, operator, value).Root.On,
			},
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
		},
	}
}

func getParamsWithFilter(className string, filter *filters.LocalFilter) traverser.GetParams {
	return traverser.GetParams{
		Filters: filter,
		// we don't care about actually resolving the ref as long as filtering
		// on it worked
		Properties: nil,
		Pagination: &filters.Pagination{
			Limit: 10,
		},
		ClassName: className,
		Kind:      kind.Thing,
	}
}

func extractNames(in []search.Result) []string {
	out := make([]string, len(in), len(in))
	for i, res := range in {
		out[i] = res.Schema.(map[string]interface{})["name"].(string)
	}

	return out
}
