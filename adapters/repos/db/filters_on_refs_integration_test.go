//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
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
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryLimit:                20,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(testCtx())
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		schemaGetter.schema.Objects = &models.Schema{}
		for _, class := range parkingGaragesSchema().Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
				schemaGetter.schema.Objects.Classes = append(schemaGetter.schema.Objects.Classes, class)
			})
		}
	})

	t.Run("importing with various combinations of props", func(t *testing.T) {
		objects := []models.Object{
			{
				Class: "MultiRefParkingGarage",
				Properties: map[string]interface{}{
					"name": "Luxury Parking Garage",
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(48.864716),
						Longitude: ptFloat32(2.349014),
					},
				},
				ID:               "a7e10b55-1ac4-464f-80df-82508eea1951",
				CreationTimeUnix: 1566469890,
			},
			{
				Class: "MultiRefParkingGarage",
				Properties: map[string]interface{}{
					"name": "Crappy Parking Garage",
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(42.331429),
						Longitude: ptFloat32(-83.045753),
					},
				},
				ID:               "ba2232cf-bb0e-413d-b986-6aa996d34d2e",
				CreationTimeUnix: 1566469892,
			},
			{
				Class: "MultiRefParkingLot",
				Properties: map[string]interface{}{
					"name": "Fancy Parking Lot",
				},
				ID:               "1023967b-9512-475b-8ef9-673a110b695d",
				CreationTimeUnix: 1566469894,
			},
			{
				Class: "MultiRefParkingLot",
				Properties: map[string]interface{}{
					"name": "The worst parking lot youve ever seen",
				},
				ID:               "901859d8-69bf-444c-bf43-498963d798d2",
				CreationTimeUnix: 1566469897,
			},
			{
				Class: "MultiRefCar",
				Properties: map[string]interface{}{
					"name": "Car which is parked no where",
				},
				ID:               "329c306b-c912-4ec7-9b1d-55e5e0ca8dea",
				CreationTimeUnix: 1566469899,
			},
			{
				Class: "MultiRefCar",
				Properties: map[string]interface{}{
					"name": "Car which is parked in a garage",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							// TODO: @Redouan - This test currently exclusively uses the old
							// beacon format. That's a good indication that the old format
							// will keep working, but I think it would be good if we could
							// use this test to also prove that the new format works.
							// Especially in combination with some old and some new beacons,
							// which is a state that we would definitely expect in real life
							// when users slowly start migrating to the new format. I think
							// we could make sure that about half the beacons in this test
							// use the old format and the other half use the new format. That
							// would be a nice indication that both work well together.
							//
							// Note that I have not yet run this test, because it does not
							// compile yet (some crossref.New() calls are missing the
							// classname further below in the file). Judging from the
							// integrationt est that is now passing, I think this should also
							// pass.
							Beacon: "weaviate://localhost/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
					},
				},
				ID:               "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43",
				CreationTimeUnix: 1566469902,
			},
			{
				Class: "MultiRefCar",
				Properties: map[string]interface{}{
					"name": "Car which is parked in a lot",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				},
				ID:               "21ab5130-627a-4268-baef-1a516bd6cad4",
				CreationTimeUnix: 1566469906,
			},
			{
				Class: "MultiRefCar",
				Properties: map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
						&models.SingleRef{
							Beacon: "weaviate://localhost/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				},
				ID:               "533673a7-2a5c-4e1c-b35d-a3809deabace",
				CreationTimeUnix: 1566469909,
			},
			{
				Class: "MultiRefDriver",
				Properties: map[string]interface{}{
					"name": "Johny Drivemuch",
					"drives": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/533673a7-2a5c-4e1c-b35d-a3809deabace",
						},
					},
				},
				ID:               "9653ab38-c16b-4561-80df-7a7e19300dd0",
				CreationTimeUnix: 1566469912,
			},
			{
				Class: "MultiRefPerson",
				Properties: map[string]interface{}{
					"name": "Jane Doughnut",
					"friendsWith": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/9653ab38-c16b-4561-80df-7a7e19300dd0",
						},
					},
				},
				ID:               "91ad23a3-07ba-4d4c-9836-76c57094f734",
				CreationTimeUnix: 1566469915,
			},
			{
				Class: "MultiRefSociety",
				Properties: map[string]interface{}{
					"name": "Cool People",
					"hasMembers": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/91ad23a3-07ba-4d4c-9836-76c57094f734",
						},
					},
				},
				ID:               "5cd9afa6-f3df-4f57-a204-840d6b256dba",
				CreationTimeUnix: 1566469918,
			},
		}

		for _, thing := range objects {
			t.Run(fmt.Sprintf("add %s", thing.ID), func(t *testing.T) {
				err := repo.PutObject(context.Background(), &thing, []float32{1, 2, 3, 4, 5, 6, 7})
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
					"id", filters.OperatorEqual, "a7e10b55-1ac4-464f-80df-82508eea1951")
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
							{
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

func TestRefFilters_MergingWithAndOperator(t *testing.T) {
	// This test is to prevent a regression where checksums get lost on an AND
	// operator, which was discovered through a journey test as part of gh-1286.
	// The schema is modelled after the journey test, as the regular tests suites
	// above do not seem to run into this issue on their own
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	defer repo.Shutdown(testCtx())
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		schemaGetter.schema.Objects = &models.Schema{}
		for _, class := range cityCountryAirportSchema().Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
				schemaGetter.schema.Objects.Classes = append(schemaGetter.schema.Objects.Classes, class)
			})
		}
	})

	const (
		netherlands strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
		germany     strfmt.UUID = "561eea29-b733-4079-b50b-cfabd78190b7"
		amsterdam   strfmt.UUID = "8f5f8e44-d348-459c-88b1-c1a44bb8f8be"
		rotterdam   strfmt.UUID = "660db307-a163-41d2-8182-560782cd018f"
		berlin      strfmt.UUID = "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
		dusseldorf  strfmt.UUID = "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"
		nullisland  strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08003"
		airport1    strfmt.UUID = "4770bb19-20fd-406e-ac64-9dac54c27a0f"
		airport2    strfmt.UUID = "cad6ab9b-5bb9-4388-a933-a5bdfd23db37"
		airport3    strfmt.UUID = "55a4dbbb-e2af-4b2a-901d-98146d1eeca7"
		airport4    strfmt.UUID = "62d15920-b546-4844-bc87-3ae33268fab5"
	)

	t.Run("import all data objects", func(t *testing.T) {
		objects := []*models.Object{
			{
				Class: "Country",
				ID:    netherlands,
				Properties: map[string]interface{}{
					"name": "Netherlands",
				},
			},
			{
				Class: "Country",
				ID:    germany,
				Properties: map[string]interface{}{
					"name": "Germany",
				},
			},

			// cities
			{
				Class: "City",
				ID:    amsterdam,
				Properties: map[string]interface{}{
					"name":       "Amsterdam",
					"population": int64(1800000),
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(52.366667),
						Longitude: ptFloat32(4.9),
					},
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(
								strfmt.URI(crossref.New("localhost", netherlands).String()),
							),
						},
					},
				},
			},
			{
				Class: "City",
				ID:    rotterdam,
				Properties: map[string]interface{}{
					"name":       "Rotterdam",
					"population": int64(600000),
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", netherlands).String()),
						},
					},
				},
			},
			{
				Class: "City",
				ID:    berlin,
				Properties: map[string]interface{}{
					"name":       "Berlin",
					"population": int64(3470000),
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", germany).String()),
						},
					},
				},
			},
			{
				Class: "City",
				ID:    dusseldorf,
				Properties: map[string]interface{}{
					"name":       "Dusseldorf",
					"population": int64(600000),
					"inCountry": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", germany).String()),
						},
					},
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(51.225556),
						Longitude: ptFloat32(6.782778),
					},
				},
			},

			{
				Class: "City",
				ID:    nullisland,
				Properties: map[string]interface{}{
					"name":       "Null Island",
					"population": 0,
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(0),
						Longitude: ptFloat32(0),
					},
				},
			},

			// airports
			{
				Class: "Airport",
				ID:    airport1,
				Properties: map[string]interface{}{
					"code": "10000",
					"phone": map[string]interface{}{
						"input": "+311234567",
					},
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", amsterdam).String()),
						},
					},
				},
			},
			{
				Class: "Airport",
				ID:    airport2,
				Properties: map[string]interface{}{
					"code": "20000",
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", rotterdam).String()),
						},
					},
				},
			},
			{
				Class: "Airport",
				ID:    airport3,
				Properties: map[string]interface{}{
					"code": "30000",
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", dusseldorf).String()),
						},
					},
				},
			},
			{
				Class: "Airport",
				ID:    airport4,
				Properties: map[string]interface{}{
					"code": "40000",
					"inCity": models.MultipleRef{
						&models.SingleRef{
							Beacon: strfmt.URI(crossref.New("localhost", berlin).String()),
						},
					},
				},
			},
		}

		for _, obj := range objects {
			require.Nil(t, repo.PutObject(context.Background(), obj, []float32{0.1}))
		}
	})

	t.Run("combining multi-level ref filters with AND", func(t *testing.T) {
		// In gh-1286 we discovered that on this query the checksum was missing and
		// we somehow didn't perform a merge, but rather always took the first set
		// of ids

		filter := filterAirportsInGermanCitiesOver600k()
		res, err := repo.ClassSearch(context.Background(),
			getParamsWithFilter("Airport", filter))
		require.Nil(t, err)

		expectedCodes := []string{"40000"}
		actualCodes := extractCodes(res)

		assert.Equal(t, expectedCodes, actualCodes)
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

func filterAirportsInGermanCitiesOver600k() *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: and,
			Operands: []filters.Clause{
				{
					Operator: gt,
					On: &filters.Path{
						Class:    schema.ClassName("Airport"),
						Property: schema.PropertyName("inCity"),
						Child: &filters.Path{
							Class:    schema.ClassName("City"),
							Property: schema.PropertyName("population"),
						},
					},
					Value: &filters.Value{
						Value: 600000,
						Type:  dtInt,
					},
				},
				{
					Operator: eq,
					On: &filters.Path{
						Class:    schema.ClassName("Airport"),
						Property: schema.PropertyName("inCity"),
						Child: &filters.Path{
							Class:    schema.ClassName("City"),
							Property: schema.PropertyName("inCountry"),
							Child: &filters.Path{
								Class:    schema.ClassName("Country"),
								Property: schema.PropertyName("name"),
							},
						},
					},
					Value: &filters.Value{
						Value: "Germany",
						Type:  dtString,
					},
				},
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
			Offset: 0,
			Limit:  10,
		},
		ClassName: className,
	}
}

func extractNames(in []search.Result) []string {
	out := make([]string, len(in), len(in))
	for i, res := range in {
		out[i] = res.Schema.(map[string]interface{})["name"].(string)
	}

	return out
}

func extractCodes(in []search.Result) []string {
	out := make([]string, len(in), len(in))
	for i, res := range in {
		out[i] = res.Schema.(map[string]interface{})["code"].(string)
	}

	return out
}
