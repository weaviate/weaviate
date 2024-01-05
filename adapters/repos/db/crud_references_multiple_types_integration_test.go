//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func TestMultipleCrossRefTypes(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		for _, class := range parkingGaragesSchema().Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class, schemaGetter.shardState)
				require.Nil(t, err)
			})
		}
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = parkingGaragesSchema()

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
				err := repo.PutObject(context.Background(), &thing, []float32{1, 2, 3, 4, 5, 6, 7}, nil)
				require.Nil(t, err)
			})
		}
	})

	t.Run("car with no refs", func(t *testing.T) {
		var id strfmt.UUID = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"
		expectedSchema := map[string]interface{}{
			"name": "Car which is parked no where",
			"id":   id,
		}

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})
	})

	t.Run("car with single ref to garage", func(t *testing.T) {
		var id strfmt.UUID = "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43"
		expectedSchemaUnresolved := map[string]interface{}{
			"name": "Car which is parked in a garage",
			"id":   id,
			// ref is present, but unresolved, therefore the lowercase letter
			"parkedAt": models.MultipleRef{
				&models.SingleRef{
					Beacon: "weaviate://localhost/a7e10b55-1ac4-464f-80df-82508eea1951",
				},
			},
		}

		getExpectedSchema := func(withVector bool) map[string]interface{} {
			fields := map[string]interface{}{
				"name": "Luxury Parking Garage",
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(48.864716),
					Longitude: ptFloat32(2.349014),
				},
				"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
			}
			if withVector {
				fields["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			return map[string]interface{}{
				"name": "Car which is parked in a garage",
				"id":   id,
				"parkedAt": []interface{}{
					search.LocalRef{
						Class:  "MultiRefParkingGarage",
						Fields: fields,
					},
				},
			}
		}

		expectedSchemaWithRefs := getExpectedSchema(false)
		expectedSchemaWithRefsWithVector := getExpectedSchema(true)

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})

		t.Run("asking for refs of type garage with vector", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarageWithVector(true), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefsWithVector, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})
	})

	t.Run("car with single ref to lot", func(t *testing.T) {
		var id strfmt.UUID = "21ab5130-627a-4268-baef-1a516bd6cad4"
		expectedSchemaUnresolved := map[string]interface{}{
			"name": "Car which is parked in a lot",
			"id":   id,
			// ref is present, but unresolved, therefore the lowercase letter
			"parkedAt": models.MultipleRef{
				&models.SingleRef{
					Beacon: "weaviate://localhost/1023967b-9512-475b-8ef9-673a110b695d",
				},
			},
		}

		getSchemaWithRefs := func(withVector bool) map[string]interface{} {
			fields := map[string]interface{}{
				"name": "Fancy Parking Lot",
				"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
			}
			if withVector {
				fields["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			return map[string]interface{}{
				"name": "Car which is parked in a lot",
				"id":   id,
				"parkedAt": []interface{}{
					search.LocalRef{
						Class:  "MultiRefParkingLot",
						Fields: fields,
					},
				},
			}
		}

		expectedSchemaWithRefs := getSchemaWithRefs(false)
		expectedSchemaWithRefsWithVector := getSchemaWithRefs(true)

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})

		t.Run("asking for refs with vector of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLotWithVector(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefsWithVector, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})
	})

	t.Run("car with refs to both", func(t *testing.T) {
		var id strfmt.UUID = "533673a7-2a5c-4e1c-b35d-a3809deabace"
		expectedSchemaUnresolved := map[string]interface{}{
			"name": "Car which is parked in two places at the same time (magic!)",
			"id":   id,
			// ref is present, but unresolved, therefore the lowercase letter
			"parkedAt": models.MultipleRef{
				&models.SingleRef{
					Beacon: "weaviate://localhost/a7e10b55-1ac4-464f-80df-82508eea1951",
				},
				&models.SingleRef{
					Beacon: "weaviate://localhost/1023967b-9512-475b-8ef9-673a110b695d",
				},
			},
		}
		getExpectedSchemaWithLotRef := func(withVector bool) map[string]interface{} {
			fields := map[string]interface{}{
				"name": "Fancy Parking Lot",
				"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
			}
			if withVector {
				fields["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			return map[string]interface{}{
				"name": "Car which is parked in two places at the same time (magic!)",
				"id":   id,
				"parkedAt": []interface{}{
					search.LocalRef{
						Class:  "MultiRefParkingLot",
						Fields: fields,
					},
				},
			}
		}
		expectedSchemaWithLotRef := getExpectedSchemaWithLotRef(false)
		expectedSchemaWithLotRefWithVector := getExpectedSchemaWithLotRef(true)
		getExpectedSchemaWithGarageRef := func(withVector bool) map[string]interface{} {
			fields := map[string]interface{}{
				"name": "Luxury Parking Garage",
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(48.864716),
					Longitude: ptFloat32(2.349014),
				},
				"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
			}
			if withVector {
				fields["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			return map[string]interface{}{
				"name": "Car which is parked in two places at the same time (magic!)",
				"id":   id,
				"parkedAt": []interface{}{
					search.LocalRef{
						Class:  "MultiRefParkingGarage",
						Fields: fields,
					},
				},
			}
		}
		expectedSchemaWithGarageRef := getExpectedSchemaWithGarageRef(false)
		expectedSchemaWithGarageRefWithVector := getExpectedSchemaWithGarageRef(true)
		getExpectedSchemaWithAllRefs := func(withVector bool) map[string]interface{} {
			fieldsParkingLot := map[string]interface{}{
				"name": "Fancy Parking Lot",
				"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
			}
			if withVector {
				fieldsParkingLot["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			fieldsParkingGarage := map[string]interface{}{
				"name": "Luxury Parking Garage",
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(48.864716),
					Longitude: ptFloat32(2.349014),
				},
				"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
			}
			if withVector {
				fieldsParkingGarage["vector"] = []float32{1, 2, 3, 4, 5, 6, 7}
			}
			return map[string]interface{}{
				"name": "Car which is parked in two places at the same time (magic!)",
				"id":   id,
				"parkedAt": []interface{}{
					search.LocalRef{
						Class:  "MultiRefParkingLot",
						Fields: fieldsParkingLot,
					},
					search.LocalRef{
						Class:  "MultiRefParkingGarage",
						Fields: fieldsParkingGarage,
					},
				},
			}
		}
		expectedSchemaWithAllRefs := getExpectedSchemaWithAllRefs(false)
		expectedSchemaWithAllRefsWithVector := getExpectedSchemaWithAllRefs(true)

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
		})

		t.Run("asking for refs with vector of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarageWithVector(true), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithGarageRefWithVector, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
		})

		t.Run("asking for refs with vector of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLotWithVector(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithLotRefWithVector, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithAllRefs, res.Schema)
		})

		t.Run("asking for refs with vectors of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEitherWithVector(), additional.Properties{}, "")
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithAllRefsWithVector, res.Schema)
		})
	})
}

func parkedAtGarage() search.SelectProperties {
	return parkedAtGarageWithVector(false)
}

func parkedAtGarageWithVector(withVector bool) search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "parkedAt",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "MultiRefParkingGarage",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
					AdditionalProperties: additional.Properties{
						Vector: withVector,
					},
				},
			},
		},
	}
}

func parkedAtLot() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "parkedAt",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "MultiRefParkingLot",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
			},
		},
	}
}

func parkedAtLotWithVector() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "parkedAt",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "MultiRefParkingLot",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
					AdditionalProperties: additional.Properties{
						Vector: true,
					},
				},
			},
		},
	}
}

func parkedAtEither() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "parkedAt",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "MultiRefParkingLot",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
				{
					ClassName: "MultiRefParkingGarage",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
				},
			},
		},
	}
}

func parkedAtEitherWithVector() search.SelectProperties {
	return search.SelectProperties{
		search.SelectProperty{
			Name:        "parkedAt",
			IsPrimitive: false,
			Refs: []search.SelectClass{
				{
					ClassName: "MultiRefParkingLot",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
					AdditionalProperties: additional.Properties{
						Vector: true,
					},
				},
				{
					ClassName: "MultiRefParkingGarage",
					RefProperties: search.SelectProperties{
						search.SelectProperty{
							Name:        "name",
							IsPrimitive: true,
						},
					},
					AdditionalProperties: additional.Properties{
						Vector: true,
					},
				},
			},
		},
	}
}
