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
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipleCrossRefTypes(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("adding all classes to the schema", func(t *testing.T) {
		for _, class := range parkingGaragesSchema().Objects.Classes {
			t.Run(fmt.Sprintf("add %s", class.Class), func(t *testing.T) {
				err := migrator.AddClass(context.Background(), class)
				require.Nil(t, err)
			})
		}
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = parkingGaragesSchema()

	t.Run("importing with various combinations of props", func(t *testing.T) {
		objects := []models.Object{
			models.Object{
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
			models.Object{
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
			models.Object{
				Class: "MultiRefParkingLot",
				Properties: map[string]interface{}{
					"name": "Fancy Parking Lot",
				},
				ID:               "1023967b-9512-475b-8ef9-673a110b695d",
				CreationTimeUnix: 1566469894,
			},
			models.Object{
				Class: "MultiRefParkingLot",
				Properties: map[string]interface{}{
					"name": "The worst parking lot youve ever seen",
				},
				ID:               "901859d8-69bf-444c-bf43-498963d798d2",
				CreationTimeUnix: 1566469897,
			},
			models.Object{
				Class: "MultiRefCar",
				Properties: map[string]interface{}{
					"name": "Car which is parked no where",
				},
				ID:               "329c306b-c912-4ec7-9b1d-55e5e0ca8dea",
				CreationTimeUnix: 1566469899,
			},
			models.Object{
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
			models.Object{
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
			models.Object{
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
			models.Object{
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
			models.Object{
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
			models.Object{
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

	t.Run("car with no refs", func(t *testing.T) {
		var id strfmt.UUID = "329c306b-c912-4ec7-9b1d-55e5e0ca8dea"
		expectedSchema := map[string]interface{}{
			"name": "Car which is parked no where",
			"id":   id,
		}

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchema, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), traverser.AdditionalProperties{})
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

		expectedSchemaWithRefs := map[string]interface{}{
			"name": "Car which is parked in a garage",
			"id":   id,
			"parkedAt": []interface{}{
				search.LocalRef{
					Class: "MultiRefParkingGarage",
					Fields: map[string]interface{}{
						"name": "Luxury Parking Garage",
						"location": &models.GeoCoordinates{
							Latitude:  ptFloat32(48.864716),
							Longitude: ptFloat32(2.349014),
						},
						"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
					},
				},
			},
		}

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), traverser.AdditionalProperties{})
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

		expectedSchemaWithRefs := map[string]interface{}{
			"name": "Car which is parked in a lot",
			"id":   id,
			"parkedAt": []interface{}{
				search.LocalRef{
					Class: "MultiRefParkingLot",
					Fields: map[string]interface{}{
						"name": "Fancy Parking Lot",
						"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
					},
				},
			},
		}

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithRefs, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), traverser.AdditionalProperties{})
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

		expectedSchemaWithLotRef := map[string]interface{}{
			"name": "Car which is parked in two places at the same time (magic!)",
			"id":   id,
			"parkedAt": []interface{}{
				search.LocalRef{
					Class: "MultiRefParkingLot",
					Fields: map[string]interface{}{
						"name": "Fancy Parking Lot",
						"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
					},
				},
			},
		}
		expectedSchemaWithGarageRef := map[string]interface{}{
			"name": "Car which is parked in two places at the same time (magic!)",
			"id":   id,
			"parkedAt": []interface{}{
				search.LocalRef{
					Class: "MultiRefParkingGarage",
					Fields: map[string]interface{}{
						"name": "Luxury Parking Garage",
						"location": &models.GeoCoordinates{
							Latitude:  ptFloat32(48.864716),
							Longitude: ptFloat32(2.349014),
						},
						"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
					},
				},
			},
		}
		expectedSchemaWithAllRefs := map[string]interface{}{
			"name": "Car which is parked in two places at the same time (magic!)",
			"id":   id,
			"parkedAt": []interface{}{
				search.LocalRef{
					Class: "MultiRefParkingLot",
					Fields: map[string]interface{}{
						"name": "Fancy Parking Lot",
						"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
					},
				},
				search.LocalRef{
					Class: "MultiRefParkingGarage",
					Fields: map[string]interface{}{
						"name": "Luxury Parking Garage",
						"location": &models.GeoCoordinates{
							Latitude:  ptFloat32(48.864716),
							Longitude: ptFloat32(2.349014),
						},
						"id": strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
					},
				},
			},
		}

		t.Run("asking for no refs", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, nil, traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaUnresolved, res.Schema)
		})

		t.Run("asking for refs of type garage", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtGarage(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithGarageRef, res.Schema)
		})

		t.Run("asking for refs of type lot", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtLot(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithLotRef, res.Schema)
		})

		t.Run("asking for refs of both types", func(t *testing.T) {
			res, err := repo.ObjectByID(context.Background(), id, parkedAtEither(), traverser.AdditionalProperties{})
			require.Nil(t, err)

			assert.Equal(t, expectedSchemaWithAllRefs, res.Schema)
		})
	})
}

func parkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "parkedAt",
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
			Name:        "parkedAt",
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
			Name:        "parkedAt",
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

func drivesCarparkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "drives",
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

func drivesCarparkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "drives",
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

func drivesCarparkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "drives",
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

func friendsWithdrivesCarparkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "friendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarparkedAtLot(),
				},
			},
		},
	}
}

func friendsWithdrivesCarparkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "friendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarparkedAtGarage(),
				},
			},
		},
	}
}

func friendsWithdrivesCarparkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "friendsWith",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefDriver",
					RefProperties: drivesCarparkedAtEither(),
				},
			},
		},
	}
}

func hasMembersfriendsWithdrivesCarparkedAtLot() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "hasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithdrivesCarparkedAtLot(),
				},
			},
		},
	}
}

func hasMembersfriendsWithdrivesCarparkedAtGarage() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "hasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithdrivesCarparkedAtGarage(),
				},
			},
		},
	}
}

func hasMembersfriendsWithdrivesCarparkedAtEither() traverser.SelectProperties {
	return traverser.SelectProperties{
		traverser.SelectProperty{
			Name:        "hasMembers",
			IsPrimitive: false,
			Refs: []traverser.SelectClass{
				traverser.SelectClass{
					ClassName:     "MultiRefPerson",
					RefProperties: friendsWithdrivesCarparkedAtEither(),
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
				"id":   strfmt.UUID("1023967b-9512-475b-8ef9-673a110b695d"),
			},
		},
		search.LocalRef{
			Class: "MultiRefParkingGarage",
			Fields: map[string]interface{}{
				"name": "Luxury Parking Garage",
				"id":   strfmt.UUID("a7e10b55-1ac4-464f-80df-82508eea1951"),
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(48.864716),
					Longitude: ptFloat32(2.349014),
				},
			},
		},
	}
}
