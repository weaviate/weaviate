// +build integrationTest

package esvector

import (
	"context"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	"github.com/semi-technologies/weaviate/entities/models"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
	ucschema "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/stretchr/testify/assert"
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

		// wait for caching cycles to have completed
		time.Sleep(1500 * time.Millisecond)

		t.Run("verify direct ref (one level) has an updated cache", func(t *testing.T) {
			res, err := repo.ThingByID(context.Background(), "fe3ca25d-8734-4ede-9a81-bc1ed8c3ea43",
				parkedAtGarage())
			require.Nil(t, err)

			parkedSlice, ok := res.Schema.(map[string]interface{})["ParkedAt"].([]interface{})
			require.True(t, ok)
			require.Len(t, parkedSlice, 1)

			garageRef, ok := parkedSlice[0].(get.LocalRef)
			require.True(t, ok)

			assert.Equal(t, "Very Luxury Parking Garage", garageRef.Fields["name"])
		})

	}
}
