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
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	ucschema "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/stretchr/testify/require"
)

// this test suite actually runs as part of the
// cache_multiple_reftypes_integration_test.go test suite

func testUpdatingCachedRefProps(repo *Repo, schema libschema.Schema) func(t *testing.T) {
	return func(t *testing.T) {
		refFinder := ucschema.NewRefFinder(&fakeSchemaGetter{schema}, 2)
		repo.SetSchemaRefFinder(refFinder)

		t.Run("changing the name of the innermost class", func(t *testing.T) {
			garage := models.Thing{
				Class: "MultiRefParkingGarage",
				Schema: map[string]interface{}{
					"name": "Very Luxury Parking Garage",
					"location": &models.GeoCoordinates{
						Latitude:  48.864716,
						Longitude: 2.349014,
					},
				},
				ID:                 "a7e10b55-1ac4-464f-80df-82508eea1951",
				CreationTimeUnix:   1566469890,
				LastUpdateTimeUnix: 1566469957,
			}

			err := repo.PutThing(context.Background(), &garage, []float32{1, 2, 3, 4, 5, 6, 7})
			require.Nil(t, err)
		})

		t.Run("verify direct ref (one level) has an updated cache", func(t *testing.T) {
			testhelper.AssertEventuallyEqual(t, "Very Luxury Parking Garage", func() interface{} {
				res, err := repo.ThingByID(context.Background(), "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43",
					parkedAtGarage(), false)
				require.Nil(t, err)

				parkedSlice, ok := res.Schema.(map[string]interface{})["ParkedAt"].([]interface{})
				require.True(t, ok)
				require.Len(t, parkedSlice, 1)

				garageRef, ok := parkedSlice[0].(search.LocalRef)
				require.True(t, ok)
				return garageRef.Fields["name"]
			})
		})

		t.Run("verify indirect ref (two levels) has an updated cache", func(t *testing.T) {
			testhelper.AssertEventuallyEqual(t, "Very Luxury Parking Garage", func() interface{} {
				res, err := repo.ThingByID(context.Background(), "9653ab38-c16b-4561-80df-7a7e19300dd0",
					drivesCarParkedAtGarage(), false)
				require.Nil(t, err)

				drivesSlice, ok := res.Schema.(map[string]interface{})["Drives"].([]interface{})
				require.True(t, ok)
				require.Len(t, drivesSlice, 1)

				carRef, ok := drivesSlice[0].(search.LocalRef)
				require.True(t, ok)

				parkedSlice, ok := carRef.Fields["ParkedAt"].([]interface{})
				require.True(t, ok)
				require.Len(t, parkedSlice, 1)

				garageRef, ok := parkedSlice[0].(search.LocalRef)
				require.True(t, ok)

				return garageRef.Fields["name"]
			})
		})
	}
}
