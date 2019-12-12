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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test suite has various scenarios for a class with multiple ref types
// in different caching scenarios. For all tests we want to make sure that
// all types can be retrieved and that only those are retrieved that the user
// asked for (select properties)
//
// Additionally it also sets the base for various other tests than can be
// performed on a schema with a lot of cross-refs, such as updating or
// filtering on refs
func testMultipleCrossRefTypes(repo *Repo, migrator *Migrator) func(t *testing.T) {
	return func(t *testing.T) {

		// we're caching two levels deep, so we can easily catch all cases:
		//
		// - one level -> fully in cache
		// - two levels -> fully in cache, but recursively resolved
		// - three levels -> outside cache, resolved at runtime
		// - four levels -> level three retrieved at runtime, but the retrieved
		//   item should itself have a cache of level four

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

		t.Run("before cache indexing", func(t *testing.T) {
			t.Run("car with no refs", func(t *testing.T) {
				var id strfmt.UUID = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"
				expectedSchema := map[string]interface{}{
					"name": "Car which is parked no where",
					"uuid": id.String(),
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})
			})

			t.Run("car with single ref to garage", func(t *testing.T) {
				var id strfmt.UUID = "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
					},
				}

				expectedSchemaNoRefs := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					// ref is not present at all
				}

				expectedSchemaWithRefs := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingGarage",
							Fields: map[string]interface{}{
								"name": "Luxury Parking Garage",
								"location": &models.GeoCoordinates{
									Latitude:  48.864716,
									Longitude: 2.349014,
								},
								"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaNoRefs, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})
			})

			t.Run("car with single ref to lot", func(t *testing.T) {
				var id strfmt.UUID = "21ab5130-627a-4268-baef-1a516bd6cad4"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				}

				expectedSchemaNoRefs := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					// ref is not present at all
				}

				expectedSchemaWithRefs := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingLot",
							Fields: map[string]interface{}{
								"name": "Fancy Parking Lot",
								"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaNoRefs, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})
			})

			t.Run("car with refs to both", func(t *testing.T) {
				var id strfmt.UUID = "533673a7-2a5c-4e1c-b35d-a3809deabace"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				}

				expectedSchemaWithLotRef := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingLot",
							Fields: map[string]interface{}{
								"name": "Fancy Parking Lot",
								"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
							},
						},
					},
				}
				expectedSchemaWithGarageRef := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingGarage",
							Fields: map[string]interface{}{
								"name": "Luxury Parking Garage",
								"location": &models.GeoCoordinates{
									Latitude:  48.864716,
									Longitude: 2.349014,
								},
								"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
							},
						},
					},
				}
				expectedSchemaWithAllRefs := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingLot",
							Fields: map[string]interface{}{
								"name": "Fancy Parking Lot",
								"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
							},
						},
						search.LocalRef{
							Class: "MultiRefParkingGarage",
							Fields: map[string]interface{}{
								"name": "Luxury Parking Garage",
								"location": &models.GeoCoordinates{
									Latitude:  48.864716,
									Longitude: 2.349014,
								},
								"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 5, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithAllRefs, res.Schema)
				})
			})

		})

		repo.InitCacheIndexing(50, 200*time.Millisecond, 200*time.Millisecond)
		time.Sleep(1500 * time.Millisecond)

		t.Run("verify that cache is hot", func(t *testing.T) {
			// by checking if the cache of the last imported thing is hot

			res, err := repo.ThingByID(context.Background(), "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43", nil, false)
			require.Nil(t, err)
			require.Equal(t, true, res.CacheHot)
		})

		t.Run("after cache indexing", func(t *testing.T) {
			t.Run("car with no refs", func(t *testing.T) {
				var id strfmt.UUID = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"
				expectedSchema := map[string]interface{}{
					"name": "Car which is parked no where",
					"uuid": id.String(),
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchema, res.Schema)
				})
			})

			t.Run("car with single ref to garage", func(t *testing.T) {
				var id strfmt.UUID = "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
					},
				}

				expectedSchemaNoRefs := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					// ref is not present at all
				}

				expectedSchemaWithRefs := map[string]interface{}{
					"name": "Car which is parked in a garage",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingGarage",
							Fields: map[string]interface{}{
								"name": "Luxury Parking Garage",
								"location": &models.GeoCoordinates{
									Latitude:  48.864716,
									Longitude: 2.349014,
								},
								"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaNoRefs, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})
			})

			t.Run("car with single ref to lot", func(t *testing.T) {
				var id strfmt.UUID = "21ab5130-627a-4268-baef-1a516bd6cad4"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				}

				expectedSchemaNoRefs := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					// ref is not present at all
				}

				expectedSchemaWithRefs := map[string]interface{}{
					"name": "Car which is parked in a lot",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingLot",
							Fields: map[string]interface{}{
								"name": "Fancy Parking Lot",
								"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaNoRefs, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithRefs, res.Schema)
				})
			})

			t.Run("car with refs to both (one level deep)", func(t *testing.T) {
				var id strfmt.UUID = "533673a7-2a5c-4e1c-b35d-a3809deabace"
				expectedSchemaUnresolved := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					// ref is present, but unresolved, therefore the lowercase letter
					"parkedAt": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/a7e10b55-1ac4-464f-80df-82508eea1951",
						},
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/1023967b-9512-475b-8ef9-673a110b695d",
						},
					},
				}

				expectedSchemaWithLotRef := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingLot",
							Fields: map[string]interface{}{
								"name": "Fancy Parking Lot",
								"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
							},
						},
					},
				}
				expectedSchemaWithGarageRef := map[string]interface{}{
					"name": "Car which is parked in two places at the same time (magic!)",
					"uuid": id.String(),
					"ParkedAt": []interface{}{
						search.LocalRef{
							Class: "MultiRefParkingGarage",
							Fields: map[string]interface{}{
								"name": "Luxury Parking Garage",
								"location": &models.GeoCoordinates{
									Latitude:  48.864716,
									Longitude: 2.349014,
								},
								"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
							},
						},
					},
				}

				t.Run("asking for no refs", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, nil, false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaUnresolved, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
				})

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, parkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					parkedAt, ok := res.Schema.(map[string]interface{})["ParkedAt"]
					require.True(t, ok)

					parkedAtSlice, ok := parkedAt.([]interface{})
					require.True(t, ok)

					assert.ElementsMatch(t, refToBothGarages(), parkedAtSlice)
				})
			})

			t.Run("car with refs to both (two levels deep)", func(t *testing.T) {
				// driver -> car -> garage
				// should be fully in cache
				var id strfmt.UUID = "9653ab38-c16b-4561-80df-7a7e19300dd0"

				expectedSchemaWithLotRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Johny Drivemuch",
					"Drives": []interface{}{
						search.LocalRef{
							Class: "MultiRefCar",
							Fields: map[string]interface{}{
								"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
								"name": "Car which is parked in two places at the same time (magic!)",
								"ParkedAt": []interface{}{
									search.LocalRef{
										Class: "MultiRefParkingLot",
										Fields: map[string]interface{}{
											"name": "Fancy Parking Lot",
											"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
										},
									},
								},
							},
						},
					},
				}

				expectedSchemaWithGarageRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Johny Drivemuch",
					"Drives": []interface{}{
						search.LocalRef{
							Class: "MultiRefCar",
							Fields: map[string]interface{}{
								"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
								"name": "Car which is parked in two places at the same time (magic!)",
								"ParkedAt": []interface{}{
									search.LocalRef{
										Class: "MultiRefParkingGarage",
										Fields: map[string]interface{}{
											"name": "Luxury Parking Garage",
											"location": &models.GeoCoordinates{
												Latitude:  48.864716,
												Longitude: 2.349014,
											},
											"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
										},
									},
								},
							},
						},
					},
				}

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, drivesCarParkedAtLot(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, drivesCarParkedAtGarage(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, drivesCarParkedAtEither(), false)
					require.Nil(t, err)

					assert.Equal(t, 1, repo.requestCounter.(*testCounter).count)
					drives, ok := res.Schema.(map[string]interface{})["Drives"]
					require.True(t, ok)

					drivesSlice, ok := drives.([]interface{})
					require.True(t, ok)
					require.Len(t, drivesSlice, 1)

					drivesRef, ok := drivesSlice[0].(search.LocalRef)
					require.True(t, ok)

					parkedAt, ok := drivesRef.Fields["ParkedAt"]
					require.True(t, ok)

					parkedAtSlice, ok := parkedAt.([]interface{})
					require.True(t, ok)

					assert.ElementsMatch(t, refToBothGarages(), parkedAtSlice)
				})
			})

			t.Run("car with refs to both (three levels deep)", func(t *testing.T) {
				// person -> driver -> car -> garage
				// should no longer be in cache
				var id strfmt.UUID = "91ad23a3-07ba-4d4c-9836-76c57094f734"

				expectedSchemaWithLotRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Jane Doughnut",
					"FriendsWith": []interface{}{
						search.LocalRef{
							Class: "MultiRefDriver",
							Fields: map[string]interface{}{
								"uuid": "9653ab38-c16b-4561-80df-7a7e19300dd0",
								"name": "Johny Drivemuch",
								"Drives": []interface{}{
									search.LocalRef{
										Class: "MultiRefCar",
										Fields: map[string]interface{}{
											"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
											"name": "Car which is parked in two places at the same time (magic!)",
											"ParkedAt": []interface{}{
												search.LocalRef{
													Class: "MultiRefParkingLot",
													Fields: map[string]interface{}{
														"name": "Fancy Parking Lot",
														"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}

				expectedSchemaWithGarageRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Jane Doughnut",
					"FriendsWith": []interface{}{
						search.LocalRef{
							Class: "MultiRefDriver",
							Fields: map[string]interface{}{
								"uuid": "9653ab38-c16b-4561-80df-7a7e19300dd0",
								"name": "Johny Drivemuch",
								"Drives": []interface{}{
									search.LocalRef{
										Class: "MultiRefCar",
										Fields: map[string]interface{}{
											"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
											"name": "Car which is parked in two places at the same time (magic!)",
											"ParkedAt": []interface{}{
												search.LocalRef{
													Class: "MultiRefParkingGarage",
													Fields: map[string]interface{}{
														"name": "Luxury Parking Garage",
														"location": &models.GeoCoordinates{
															Latitude:  48.864716,
															Longitude: 2.349014,
														},
														"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, friendsWithDrivesCarParkedAtLot(), false)
					require.Nil(t, err)

					// 3 calls, the initial one, outside of the cache there are two
					// possible types, so 2 additional requests
					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, friendsWithDrivesCarParkedAtGarage(), false)
					require.Nil(t, err)

					// 3 calls, the initial one, outside of the cache there are two
					// possible types, so 2 additional requests
					assert.Equal(t, 3, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, friendsWithDrivesCarParkedAtEither(), false)
					require.Nil(t, err)

					// 5 calls: 1 initial call, at boundary 2 requested props * 2 beacons = 4, 1+4=5
					assert.Equal(t, 5, repo.requestCounter.(*testCounter).count)

					friendsWith, ok := res.Schema.(map[string]interface{})["FriendsWith"]
					require.True(t, ok)

					friendsSlice, ok := friendsWith.([]interface{})
					require.True(t, ok)
					require.Len(t, friendsSlice, 1)

					friendsRef, ok := friendsSlice[0].(search.LocalRef)
					require.True(t, ok)

					drives, ok := friendsRef.Fields["Drives"]
					require.True(t, ok)

					drivesSlice, ok := drives.([]interface{})
					require.True(t, ok)
					require.Len(t, drivesSlice, 1)

					drivesRef, ok := drivesSlice[0].(search.LocalRef)
					require.True(t, ok)

					parkedAt, ok := drivesRef.Fields["ParkedAt"]
					require.True(t, ok)

					parkedAtSlice, ok := parkedAt.([]interface{})
					require.True(t, ok)

					assert.ElementsMatch(t, refToBothGarages(), parkedAtSlice)
				})
			})

			t.Run("car with refs to both (fours levels deep)", func(t *testing.T) {
				// society -> person -> driver -> car -> garage
				// cache bounday is crossed at driver->car which means car->garage
				// should be in cache again
				var id strfmt.UUID = "5cd9afa6-f3df-4f57-a204-840d6b256dba"

				expectedSchemaWithLotRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Cool People",
					"HasMembers": []interface{}{
						search.LocalRef{
							Class: "MultiRefPerson",
							Fields: map[string]interface{}{
								"uuid": "91ad23a3-07ba-4d4c-9836-76c57094f734",
								"name": "Jane Doughnut",
								"FriendsWith": []interface{}{
									search.LocalRef{
										Class: "MultiRefDriver",
										Fields: map[string]interface{}{
											"uuid": "9653ab38-c16b-4561-80df-7a7e19300dd0",
											"name": "Johny Drivemuch",
											"Drives": []interface{}{
												search.LocalRef{
													Class: "MultiRefCar",
													Fields: map[string]interface{}{
														"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
														"name": "Car which is parked in two places at the same time (magic!)",
														"ParkedAt": []interface{}{
															search.LocalRef{
																Class: "MultiRefParkingLot",
																Fields: map[string]interface{}{
																	"name": "Fancy Parking Lot",
																	"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}

				expectedSchemaWithGarageRef := map[string]interface{}{
					"uuid": id.String(),
					"name": "Cool People",
					"HasMembers": []interface{}{
						search.LocalRef{
							Class: "MultiRefPerson",
							Fields: map[string]interface{}{
								"uuid": "91ad23a3-07ba-4d4c-9836-76c57094f734",
								"name": "Jane Doughnut",
								"FriendsWith": []interface{}{
									search.LocalRef{
										Class: "MultiRefDriver",
										Fields: map[string]interface{}{
											"uuid": "9653ab38-c16b-4561-80df-7a7e19300dd0",
											"name": "Johny Drivemuch",
											"Drives": []interface{}{
												search.LocalRef{
													Class: "MultiRefCar",
													Fields: map[string]interface{}{
														"uuid": "533673a7-2a5c-4e1c-b35d-a3809deabace",
														"name": "Car which is parked in two places at the same time (magic!)",
														"ParkedAt": []interface{}{
															search.LocalRef{
																Class: "MultiRefParkingGarage",
																Fields: map[string]interface{}{
																	"name": "Luxury Parking Garage",
																	"location": &models.GeoCoordinates{
																		Latitude:  48.864716,
																		Longitude: 2.349014,
																	},
																	"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}

				t.Run("asking for refs of type lot", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, hasMembersFriendsWithDrivesCarParkedAtLot(), false)
					require.Nil(t, err)

					// initial call + cache boundary crossed at driver->car
					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
				})

				t.Run("asking for refs of type garage", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, hasMembersFriendsWithDrivesCarParkedAtGarage(), false)
					require.Nil(t, err)

					// initial call + cache boundary crossed at driver->car
					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)
					assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
				})

				t.Run("asking for refs of both types", func(t *testing.T) {
					repo.requestCounter.(*testCounter).reset()
					res, err := repo.ThingByID(context.Background(), id, hasMembersFriendsWithDrivesCarParkedAtEither(), false)
					require.Nil(t, err)

					// initial call + cache boundary crossed at driver->car
					assert.Equal(t, 2, repo.requestCounter.(*testCounter).count)

					hasMembers, ok := res.Schema.(map[string]interface{})["HasMembers"]
					require.True(t, ok)

					membersSlice, ok := hasMembers.([]interface{})
					require.True(t, ok)
					require.Len(t, membersSlice, 1)

					membersRef, ok := membersSlice[0].(search.LocalRef)
					require.True(t, ok)

					friendsWith, ok := membersRef.Fields["FriendsWith"]
					require.True(t, ok)

					friendsSlice, ok := friendsWith.([]interface{})
					require.True(t, ok)
					require.Len(t, friendsSlice, 1)

					friendsRef, ok := friendsSlice[0].(search.LocalRef)
					require.True(t, ok)

					drives, ok := friendsRef.Fields["Drives"]
					require.True(t, ok)

					drivesSlice, ok := drives.([]interface{})
					require.True(t, ok)
					require.Len(t, drivesSlice, 1)

					drivesRef, ok := drivesSlice[0].(search.LocalRef)
					require.True(t, ok)

					parkedAt, ok := drivesRef.Fields["ParkedAt"]
					require.True(t, ok)

					parkedAtSlice, ok := parkedAt.([]interface{})
					require.True(t, ok)

					assert.ElementsMatch(t, refToBothGarages(), parkedAtSlice)
				})
			})
		})
	}
}

func parkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "ParkedAt",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName: "MultiRefParkingGarage",
					RefProperties: traverser.SelectProperties{
						traverser.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
			},
		},
	}
}

func parkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "ParkedAt",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName: "MultiRefParkingLot",
					RefProperties: traverser.SelectProperties{
						traverser.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
			},
		},
	}
}

func parkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "ParkedAt",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName: "MultiRefParkingLot",
					RefProperties: traverser.SelectProperties{
						traverser.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
				traverser.SelectClass{
					ClassName: "MultiRefParkingGarage",
					RefProperties: traverser.SelectProperties{
						traverser.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
			},
		},
	}
}

func drivesCarParkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "Drives",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefCar",
					RefProperties: parkedAtLot(),
				},
			},
		},
	}
}

func drivesCarParkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "Drives",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefCar",
					RefProperties: parkedAtGarage(),
				},
			},
		},
	}
}

func drivesCarParkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "Drives",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefCar",
					RefProperties: parkedAtEither(),
				},
			},
		},
	}
}

func friendsWithDrivesCarParkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "FriendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarParkedAtLot(),
				},
			},
		},
	}
}

func friendsWithDrivesCarParkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "FriendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarParkedAtGarage(),
				},
			},
		},
	}
}

func friendsWithDrivesCarParkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "FriendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarParkedAtEither(),
				},
			},
		},
	}
}

func hasMembersFriendsWithDrivesCarParkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "HasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithDrivesCarParkedAtLot(),
				},
			},
		},
	}
}

func hasMembersFriendsWithDrivesCarParkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "HasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithDrivesCarParkedAtGarage(),
				},
			},
		},
	}
}

func hasMembersFriendsWithDrivesCarParkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "HasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithDrivesCarParkedAtEither(),
				},
			},
		},
	}
}

func refToBothGarages() []interface{} {
	return []interface{}{
		search.LocalRef{
			Class: "MultiRefParkingLot",
			Fields: map[string]interface{}{
				"name": "Fancy Parking Lot",
				"uuid": "1023967b-9512-475b-8ef9-673a110b695d",
			},
		},
		search.LocalRef{
			Class: "MultiRefParkingGarage",
			Fields: map[string]interface{}{
				"name": "Luxury Parking Garage",
				"uuid": "a7e10b55-1ac4-464f-80df-82508eea1951",
				"location": &models.GeoCoordinates{
					Latitude:  48.864716,
					Longitude: 2.349014,
				},
			},
		},
	}
}
