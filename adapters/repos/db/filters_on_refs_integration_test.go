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

// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefFilters(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		schemaGetter.schema.Things = &models.Schema{}
		for _, class := range parkingGaragesSchema().Things.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), kind.Thing, class)
				require.Nil(t, err)
				schemaGetter.schema.Things.Classes = append(schemaGetter.schema.Things.Classes, class)
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
						Latitude:  ptFloat32(48.864716),
						Longitude: ptFloat32(2.349014),
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
						Latitude:  ptFloat32(42.331429),
						Longitude: ptFloat32(-83.045753),
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
							Latitude:  ptFloat32(48.801407),
							Longitude: ptFloat32(2.130122),
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
							Latitude:  ptFloat32(42.279594),
							Longitude: ptFloat32(-83.732124),
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
							Latitude:  ptFloat32(48.801407),
							Longitude: ptFloat32(2.130122),
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
							Latitude:  ptFloat32(48.801407),
							Longitude: ptFloat32(2.130122),
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
							Latitude:  ptFloat32(42.279594),
							Longitude: ptFloat32(-83.732124),
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
