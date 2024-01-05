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

package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestCRUD(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "TheBestThingClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "location",
				DataType: []string{string(schema.DataTypeGeoCoordinates)},
			},
			{
				Name:     "phone",
				DataType: []string{string(schema.DataTypePhoneNumber)},
			},
		},
	}
	actionclass := &models.Class{
		Class:               "TheBestActionClass",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "refProp",
				DataType: []string{"TheBestThingClass"},
			},
			{
				Name:     "phone",
				DataType: []string{string(schema.DataTypePhoneNumber)},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), thingclass, schemaGetter.shardState))
	})

	t.Run("creating the action class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), actionclass, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{actionclass, thingclass},
		},
	}

	thingID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")

	t.Run("validating that the thing doesn't exist prior", func(t *testing.T) {
		ok, err := repo.Exists(context.Background(), "TheBestThingClass", thingID, nil, "")
		require.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("adding a thing", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Properties: map[string]interface{}{
				"stringProp": "some value",
				"phone": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1234567",
					Valid:                  true,
					InternationalFormatted: "+49 171 1234567",
					National:               1234567,
					NationalFormatted:      "0171 1234567",
				},
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(1),
					Longitude: ptFloat32(2),
				},
			},
			Additional: models.AdditionalProperties{
				"interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept":    "some",
							"occurrence": float64(1),
							"weight":     float64(1),
						},
						map[string]interface{}{
							"concept":    "value",
							"occurrence": float64(1),
							"weight":     float64(1),
						},
					},
				},
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutObject(context.Background(), thing, vector, nil)

		assert.Nil(t, err)
	})

	t.Run("validating that the thing exists now", func(t *testing.T) {
		ok, err := repo.Exists(context.Background(), "TheBestThingClass", thingID, nil, "")
		require.Nil(t, err)
		assert.True(t, ok)
	})

	t.Run("trying to add a thing to a non-existing class", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "WrongClass",
			Properties: map[string]interface{}{
				"stringProp": "some value",
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutObject(context.Background(), thing, vector, nil)
		assert.Equal(t,
			fmt.Errorf("import into non-existing index for WrongClass"), err)
	})

	timeMust := func(t strfmt.DateTime, err error) strfmt.DateTime {
		if err != nil {
			panic(err)
		}

		return t
	}

	t.Run("updating the thing", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 10000020,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Properties: map[string]interface{}{
				"stringProp": "updated value",
				"phone": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1234567",
					Valid:                  true,
					InternationalFormatted: "+49 171 1234567",
					National:               1234567,
					NationalFormatted:      "0171 1234567",
				},
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(1),
					Longitude: ptFloat32(2),
				},
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutObject(context.Background(), thing, vector, nil)
		assert.Nil(t, err)
	})

	t.Run("validating the updates are reflected", func(t *testing.T) {
		expected := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 10000020,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			VectorWeights:      map[string]string(nil),
			Properties: map[string]interface{}{
				"stringProp": "updated value",
				"phone": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1234567",
					Valid:                  true,
					InternationalFormatted: "+49 171 1234567",
					National:               1234567,
					NationalFormatted:      "0171 1234567",
				},
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(1),
					Longitude: ptFloat32(2),
				},
			},
			Additional: models.AdditionalProperties{},
		}
		res, err := repo.ObjectByID(context.Background(), thingID, nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res.ObjectWithVector(false))

		res, err = repo.Object(context.Background(), expected.Class, thingID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res.ObjectWithVector(false))
	})

	t.Run("finding the updated object by querying for an updated value",
		func(t *testing.T) {
			// This is to verify the inverted index was updated correctly
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:  "TheBestThingClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestThingClass",
							Property: "stringProp",
						},
						Value: &filters.Value{
							// we would not have found this object before using "updated", as
							// this string was only introduced as part of the update
							Value: "updated",
							Type:  schema.DataTypeText,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, thingID, res[0].ID)
		})

	t.Run("NOT finding the previous version by querying for an outdated value",
		func(t *testing.T) {
			// This is to verify the inverted index was cleaned up correctly
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:  "TheBestThingClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestThingClass",
							Property: "stringProp",
						},
						Value: &filters.Value{
							Value: "some",
							Type:  schema.DataTypeText,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 0)
		})

	t.Run("still finding it for an unchanged term",
		func(t *testing.T) {
			// This is to verify that while we're adding new links and cleaning up
			// old ones, we don't actually touch those that were present and still
			// should be
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:  "TheBestThingClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestThingClass",
							Property: "stringProp",
						},
						Value: &filters.Value{
							// we would not have found this object before using "updated", as
							// this string was only introduced as part of the update
							Value: "value",
							Type:  schema.DataTypeText,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, thingID, res[0].ID)
		})

	t.Run("updating the thing back to its original value", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Properties: map[string]interface{}{
				"stringProp": "some value",
				"phone": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1234567",
					Valid:                  true,
					InternationalFormatted: "+49 171 1234567",
					National:               1234567,
					NationalFormatted:      "0171 1234567",
				},
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(1),
					Longitude: ptFloat32(2),
				},
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutObject(context.Background(), thing, vector, nil)
		assert.Nil(t, err)
	})

	actionID := strfmt.UUID("022ca5ba-7c0b-4a78-85bf-26346bbcfae7")
	t.Run("adding an action", func(t *testing.T) {
		action := &models.Object{
			CreationTimeUnix:   1000002,
			LastUpdateTimeUnix: 1000003,
			ID:                 actionID,
			Class:              "TheBestActionClass",
			Properties: map[string]interface{}{
				"stringProp": "some act-citing value",
				"refProp": models.MultipleRef{
					&models.SingleRef{
						Classification: &models.ReferenceMetaClassification{
							LosingDistance:         ptFloat64(0.7),
							MeanLosingDistance:     ptFloat64(0.7),
							ClosestLosingDistance:  ptFloat64(0.65),
							WinningDistance:        0.3,
							MeanWinningDistance:    0.3,
							ClosestWinningDistance: 0.25,
							ClosestOverallDistance: 0.25,
							OverallCount:           3,
							WinningCount:           2,
							LosingCount:            1,
						},
						Beacon: strfmt.URI(
							crossref.NewLocalhost("", thingID).String()),
					},
				},
			},
			Additional: models.AdditionalProperties{
				"classification": &additional.Classification{
					ID:               "foo",
					Scope:            []string{"scope1", "scope2"},
					ClassifiedFields: []string{"field1", "field2"},
					Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
				},
			},
		}
		vector := []float32{3, 1, 0.3, 12}

		err := repo.PutObject(context.Background(), action, vector, nil)

		assert.Nil(t, err)
	})

	t.Run("searching by vector", func(t *testing.T) {
		// the search vector is designed to be very close to the action, but
		// somewhat far from the thing. So it should match the action closer
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}

		res, err := repo.CrossClassVectorSearch(context.Background(), searchVector, 0, 10, nil)

		require.Nil(t, err)
		require.Equal(t, true, len(res) >= 2)
		assert.Equal(t, actionID, res[0].ID)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, int64(1000002), res[0].Created)
		assert.Equal(t, int64(1000003), res[0].Updated)
		assert.Equal(t, thingID, res[1].ID)

		assert.Equal(t, "TheBestThingClass", res[1].ClassName)
		assert.Equal(t, int64(1565612833955), res[1].Created)
		assert.Equal(t, int64(1000001), res[1].Updated)
	})

	t.Run("searching by vector for a single class", func(t *testing.T) {
		// the search vector is designed to be very close to the action, but
		// somewhat far from the thing. So it should match the action closer
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}

		params := dto.GetParams{
			SearchVector: searchVector,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.VectorSearch(context.Background(), params)

		require.Nil(t, err)
		require.Len(t, res, 1, "got exactly one result")
		assert.Equal(t, thingID, res[0].ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", res[0].ClassName, "matches the class name")
		schema := res[0].Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, &models.PhoneNumber{
			CountryCode:            49,
			DefaultCountry:         "DE",
			Input:                  "0171 1234567",
			Valid:                  true,
			InternationalFormatted: "+49 171 1234567",
			National:               1234567,
			NationalFormatted:      "0171 1234567",
		}, schema["phone"], "has correct phone prop")
		assert.Equal(t, models.AdditionalProperties{}, res[0].AdditionalProperties, "no meta information should be included unless explicitly asked for")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
	})

	t.Run("searching by class type", func(t *testing.T) {
		params := dto.GetParams{
			SearchVector: nil,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.Search(context.Background(), params)

		require.Nil(t, err)
		require.Len(t, res, 1, "got exactly one result")
		assert.Equal(t, thingID, res[0].ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", res[0].ClassName, "matches the class name")
		schema := res[0].Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
	})

	t.Run("adding a thing with interpretation additional property", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Properties: map[string]interface{}{
				"stringProp": "some value",
				"phone": &models.PhoneNumber{
					CountryCode:            49,
					DefaultCountry:         "DE",
					Input:                  "0171 1234567",
					Valid:                  true,
					InternationalFormatted: "+49 171 1234567",
					National:               1234567,
					NationalFormatted:      "0171 1234567",
				},
				"location": &models.GeoCoordinates{
					Latitude:  ptFloat32(1),
					Longitude: ptFloat32(2),
				},
			},
			Additional: models.AdditionalProperties{
				"interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept":    "some",
							"occurrence": float64(1),
							"weight":     float64(1),
						},
						map[string]interface{}{
							"concept":    "value",
							"occurrence": float64(1),
							"weight":     float64(1),
						},
					},
				},
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutObject(context.Background(), thing, vector, nil)

		assert.Nil(t, err)
	})

	t.Run("searching all things", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, nil, additional.Properties{}, "")
		require.Nil(t, err)

		item, ok := findID(res, thingID)
		require.Equal(t, true, ok, "results should contain our desired thing id")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
		assert.Equal(t, models.AdditionalProperties{}, item.AdditionalProperties, "has no additional properties unless explicitly asked for")
	})

	t.Run("searching all things with Vector additional props", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, nil, additional.Properties{Vector: true}, "")
		require.Nil(t, err)

		item, ok := findID(res, thingID)
		require.Equal(t, true, ok, "results should contain our desired thing id")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
		assert.Equal(t, []float32{1, 3, 5, 0.4}, item.Vector, "has Vector property")
	})

	t.Run("searching all things with Vector and Interpretation additional props", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		params := additional.Properties{
			Vector: true,
			ModuleParams: map[string]interface{}{
				"interpretation": true,
			},
		}
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, nil, params, "")
		require.Nil(t, err)

		item, ok := findID(res, thingID)
		require.Equal(t, true, ok, "results should contain our desired thing id")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
		assert.Equal(t, []float32{1, 3, 5, 0.4}, item.Vector, "has Vector property")
		assert.Equal(t, models.AdditionalProperties{
			"interpretation": map[string]interface{}{
				"source": []interface{}{
					map[string]interface{}{
						"concept":    "some",
						"occurrence": float64(1),
						"weight":     float64(1),
					},
					map[string]interface{}{
						"concept":    "value",
						"occurrence": float64(1),
						"weight":     float64(1),
					},
				},
			},
		}, item.AdditionalProperties, "has Vector and Interpretation additional property")
	})

	t.Run("searching a thing by ID", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), thingID, search.SelectProperties{}, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
	})

	// Check the same, but with Object()
	t.Run("searching a thing by ID", func(t *testing.T) {
		item, err := repo.Object(context.Background(), "TheBestThingClass",
			thingID, search.SelectProperties{}, additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
	})

	t.Run("listing multiple things by IDs (MultiGet)", func(t *testing.T) {
		query := []multi.Identifier{
			{
				ID:        "be685717-e61e-450d-8d5c-f44f32d0336c", // this id does not exist
				ClassName: "TheBestThingClass",
			},
			{
				ID:        thingID.String(),
				ClassName: "TheBestThingClass",
			},
		}
		res, err := repo.MultiGet(context.Background(), query, additional.Properties{}, "")
		require.Nil(t, err)
		require.Len(t, res, 2, "length must match even with nil-items")

		assert.Equal(t, strfmt.UUID(""), res[0].ID, "empty object for the not-found item")

		item := res[1]
		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["id"], "has id in schema as uuid field")
	})

	t.Run("searching an action by ID without meta", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{}, "")
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, models.AdditionalProperties{}, item.AdditionalProperties, "not meta information should be included unless explicitly asked for")
		expectedRefProp := models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(
					crossref.NewLocalhost("", thingID).String()),
			},
		}
		assert.Equal(t, expectedRefProp, schema["refProp"])
	})

	t.Run("searching an action by ID with Classification and Vector additional properties", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{Classification: true, Vector: true, RefMeta: true}, "")
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, models.AdditionalProperties{
			"classification": &additional.Classification{
				ID:               "foo",
				Scope:            []string{"scope1", "scope2"},
				ClassifiedFields: []string{"field1", "field2"},
				Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
			},
		}, item.AdditionalProperties, "it should include the object meta as it was explicitly specified")
		assert.Equal(t, []float32{3, 1, 0.3, 12}, item.Vector, "has Vector property")

		expectedRefProp := models.MultipleRef{
			&models.SingleRef{
				Classification: &models.ReferenceMetaClassification{
					LosingDistance:         ptFloat64(0.7),
					MeanLosingDistance:     ptFloat64(0.7),
					ClosestLosingDistance:  ptFloat64(0.65),
					WinningDistance:        0.3,
					MeanWinningDistance:    0.3,
					ClosestWinningDistance: 0.25,
					ClosestOverallDistance: 0.25,
					OverallCount:           3,
					WinningCount:           2,
					LosingCount:            1,
				},
				Beacon: strfmt.URI(
					crossref.NewLocalhost("", thingID).String()),
			},
		}
		assert.Equal(t, expectedRefProp, schema["refProp"])
	})

	t.Run("searching an action by ID with only Vector additional property", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{Vector: true}, "")
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, []float32{3, 1, 0.3, 12}, item.Vector, "it should include the object meta as it was explicitly specified")
	})

	t.Run("searching all actions", func(t *testing.T) {
		res, err := repo.ObjectSearch(context.Background(), 0, 10, nil, nil, additional.Properties{}, "")
		require.Nil(t, err)

		item, ok := findID(res, actionID)
		require.Equal(t, true, ok, "results should contain our desired action id")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
	})

	t.Run("sorting all objects", func(t *testing.T) {
		// prepare
		thingID1 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000001")
		thingID2 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000002")
		thingID3 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000003")
		thingID4 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000004")
		actionID1 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b100001")
		actionID2 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b100002")
		actionID3 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b100003")
		testData := []struct {
			id         strfmt.UUID
			className  string
			stringProp string
			phone      uint64
			longitude  float32
		}{
			{
				id:         thingID1,
				className:  "TheBestThingClass",
				stringProp: "a very short text",
				phone:      1234900,
				longitude:  10,
			},
			{
				id:         thingID2,
				className:  "TheBestThingClass",
				stringProp: "zebra lives in Zoo",
				phone:      1234800,
				longitude:  111,
			},
			{
				id:         thingID3,
				className:  "TheBestThingClass",
				stringProp: "the best thing class",
				phone:      1234910,
				longitude:  2,
			},
			{
				id:         thingID4,
				className:  "TheBestThingClass",
				stringProp: "car",
				phone:      1234901,
				longitude:  11,
			},
			{
				id:         actionID1,
				className:  "TheBestActionClass",
				stringProp: "a very short text",
				phone:      1234000,
				longitude:  10,
			},
			{
				id:         actionID2,
				className:  "TheBestActionClass",
				stringProp: "zebra lives in Zoo",
				phone:      1234002,
				longitude:  5,
			},
			{
				id:         actionID3,
				className:  "TheBestActionClass",
				stringProp: "fossil fuels",
				phone:      1234010,
				longitude:  6,
			},
		}
		for _, td := range testData {
			object := &models.Object{
				CreationTimeUnix:   1565612833990,
				LastUpdateTimeUnix: 1000001,
				ID:                 td.id,
				Class:              td.className,
				Properties: map[string]interface{}{
					"stringProp": td.stringProp,
					"phone": &models.PhoneNumber{
						CountryCode:            49,
						DefaultCountry:         "DE",
						Input:                  fmt.Sprintf("0171 %d", td.phone),
						Valid:                  true,
						InternationalFormatted: fmt.Sprintf("+49 171 %d", td.phone),
						National:               td.phone,
						NationalFormatted:      fmt.Sprintf("0171 %d", td.phone),
					},
					"location": &models.GeoCoordinates{
						Latitude:  ptFloat32(1),
						Longitude: ptFloat32(td.longitude),
					},
				},
			}
			vector := []float32{1.1, 1.3, 1.5, 1.4}
			err := repo.PutObject(context.Background(), object, vector, nil)
			assert.Nil(t, err)
		}
		// run sorting tests
		tests := []struct {
			name               string
			sort               []filters.Sort
			expectedThingIDs   []strfmt.UUID
			expectedActionIDs  []strfmt.UUID
			constainsErrorMsgs []string
		}{
			{
				name:              "by stringProp asc",
				sort:              []filters.Sort{{Path: []string{"stringProp"}, Order: "asc"}},
				expectedThingIDs:  []strfmt.UUID{thingID1, thingID4, thingID, thingID3, thingID2},
				expectedActionIDs: []strfmt.UUID{actionID1, actionID3, actionID, actionID2},
			},
			{
				name:              "by stringProp desc",
				sort:              []filters.Sort{{Path: []string{"stringProp"}, Order: "desc"}},
				expectedThingIDs:  []strfmt.UUID{thingID2, thingID3, thingID, thingID4, thingID1},
				expectedActionIDs: []strfmt.UUID{actionID2, actionID, actionID3, actionID1},
			},
			{
				name:              "by phone asc",
				sort:              []filters.Sort{{Path: []string{"phone"}, Order: "asc"}},
				expectedThingIDs:  []strfmt.UUID{thingID, thingID2, thingID1, thingID4, thingID3},
				expectedActionIDs: []strfmt.UUID{actionID, actionID1, actionID2, actionID3},
			},
			{
				name:              "by phone desc",
				sort:              []filters.Sort{{Path: []string{"phone"}, Order: "desc"}},
				expectedThingIDs:  []strfmt.UUID{thingID3, thingID4, thingID1, thingID2, thingID},
				expectedActionIDs: []strfmt.UUID{actionID3, actionID2, actionID1, actionID},
			},
			{
				name: "by phone and stringProp asc",
				sort: []filters.Sort{
					{Path: []string{"phone"}, Order: "asc"},
					{Path: []string{"stringProp"}, Order: "asc"},
				},
				expectedThingIDs:  []strfmt.UUID{thingID, thingID2, thingID1, thingID4, thingID3},
				expectedActionIDs: []strfmt.UUID{actionID, actionID1, actionID2, actionID3},
			},
			{
				name: "by location asc",
				sort: []filters.Sort{{Path: []string{"location"}, Order: "asc"}},
				constainsErrorMsgs: []string{"search: search index thebestactionclass: sort parameter at position 0: " +
					"no such prop with name 'location' found in class 'TheBestActionClass' in the schema. " +
					"Check your schema files for which properties in this class are available"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, tt.sort, additional.Properties{Vector: true}, "")
				if len(tt.constainsErrorMsgs) > 0 {
					require.NotNil(t, err)
					for _, errorMsg := range tt.constainsErrorMsgs {
						assert.Contains(t, err.Error(), errorMsg)
					}
				} else {
					require.Nil(t, err)
					require.Len(t, res, 9)

					var thingIds, actionIds []strfmt.UUID
					for i := range res {
						if res[i].ClassName == "TheBestThingClass" {
							thingIds = append(thingIds, res[i].ID)
						} else {
							actionIds = append(actionIds, res[i].ID)
						}
					}
					assert.EqualValues(t, thingIds, tt.expectedThingIDs, "thing ids don't match")
					assert.EqualValues(t, actionIds, tt.expectedActionIDs, "action ids don't match")
				}
			})
		}
		// clean up
		for _, td := range testData {
			err := repo.DeleteObject(context.Background(), td.className, td.id, nil, "")
			assert.Nil(t, err)
		}
	})

	t.Run("verifying the thing is indexed in the inverted index", func(t *testing.T) {
		// This is a control for the upcoming deletion, after the deletion it should not
		// be indexed anymore.
		res, err := repo.Search(context.Background(), dto.GetParams{
			ClassName:  "TheBestThingClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "TheBestThingClass",
						Property: "stringProp",
					},
					Value: &filters.Value{
						Value: "some",
						Type:  schema.DataTypeText,
					},
				},
			},
		})
		require.Nil(t, err)
		require.Len(t, res, 1)
	})

	t.Run("verifying the action is indexed in the inverted index", func(t *testing.T) {
		// This is a control for the upcoming deletion, after the deletion it should not
		// be indexed anymore.
		res, err := repo.Search(context.Background(), dto.GetParams{
			ClassName:  "TheBestActionClass",
			Pagination: &filters.Pagination{Limit: 10},
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    "TheBestActionClass",
						Property: "stringProp",
					},
					Value: &filters.Value{
						Value: "some",
						Type:  schema.DataTypeText,
					},
				},
			},
		})
		require.Nil(t, err)
		require.Len(t, res, 1)
	})

	t.Run("deleting a thing again", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(), "TheBestThingClass", thingID, nil, "")

		assert.Nil(t, err)
	})

	t.Run("deleting a action again", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(), "TheBestActionClass", actionID, nil, "")

		assert.Nil(t, err)
	})

	t.Run("trying to delete from a non-existing class", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(), "WrongClass", thingID, nil, "")

		assert.Equal(t, fmt.Errorf(
			"delete from non-existing index for WrongClass"), err)
	})

	t.Run("verifying the thing is NOT indexed in the inverted index",
		func(t *testing.T) {
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:  "TheBestThingClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestThingClass",
							Property: "stringProp",
						},
						Value: &filters.Value{
							Value: "some",
							Type:  schema.DataTypeText,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 0)
		})

	t.Run("verifying the action is NOT indexed in the inverted index",
		func(t *testing.T) {
			res, err := repo.Search(context.Background(), dto.GetParams{
				ClassName:  "TheBestActionClass",
				Pagination: &filters.Pagination{Limit: 10},
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestActionClass",
							Property: "stringProp",
						},
						Value: &filters.Value{
							Value: "some",
							Type:  schema.DataTypeText,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 0)
		})

	t.Run("trying to get the deleted thing by ID", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), thingID, search.SelectProperties{}, additional.Properties{}, "")
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	t.Run("trying to get the deleted action by ID", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{}, "")
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	t.Run("searching by vector for a single thing class again after deletion",
		func(t *testing.T) {
			searchVector := []float32{2.9, 1.1, 0.5, 8.01}
			params := dto.GetParams{
				SearchVector: searchVector,
				ClassName:    "TheBestThingClass",
				Pagination:   &filters.Pagination{Limit: 10},
				Filters:      nil,
			}

			res, err := repo.VectorSearch(context.Background(), params)

			require.Nil(t, err)
			assert.Len(t, res, 0)
		})

	t.Run("searching by vector for a single action class again after deletion", func(t *testing.T) {
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}
		params := dto.GetParams{
			SearchVector: searchVector,
			ClassName:    "TheBestActionClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}

		res, err := repo.VectorSearch(context.Background(), params)

		require.Nil(t, err)
		assert.Len(t, res, 0)
	})

	t.Run("ensure referenced class searches are not limited", func(t *testing.T) {
		numThings := int(repo.config.QueryMaximumResults * 10)
		createdActionIDs := make([]strfmt.UUID, numThings)
		createdThingIDs := make([]strfmt.UUID, numThings)

		t.Run("add new action objects", func(t *testing.T) {
			actionBatch := make([]objects.BatchObject, numThings)
			for i := 0; i < len(createdActionIDs); i++ {
				newID := strfmt.UUID(uuid.NewString())
				actionBatch[i] = objects.BatchObject{
					UUID: newID,
					Object: &models.Object{
						ID:    newID,
						Class: "TheBestActionClass",
						Properties: map[string]interface{}{
							"stringProp": fmt.Sprintf("action#%d", i),
						},
					},
				}
				createdActionIDs[i] = newID
			}
			batchObjResp, err := repo.BatchPutObjects(context.Background(), actionBatch, nil)
			require.Len(t, batchObjResp, numThings)
			require.Nil(t, err)
			for _, r := range batchObjResp {
				require.Nil(t, r.Err)
			}
		})

		t.Run("add more thing objects to reference", func(t *testing.T) {
			thingBatch := make([]objects.BatchObject, numThings)
			for i := 0; i < len(createdThingIDs); i++ {
				newID := strfmt.UUID(uuid.NewString())
				thingBatch[i] = objects.BatchObject{
					UUID: newID,
					Object: &models.Object{
						ID:    newID,
						Class: "TheBestThingClass",
						Properties: map[string]interface{}{
							"stringProp": fmt.Sprintf("thing#%d", i),
						},
					},
				}
				createdThingIDs[i] = newID
			}
			batchObjResp, err := repo.BatchPutObjects(context.Background(), thingBatch, nil)
			require.Len(t, batchObjResp, numThings)
			require.Nil(t, err)
			for _, r := range batchObjResp {
				require.Nil(t, r.Err)
			}
		})

		t.Run("reference each thing from an action", func(t *testing.T) {
			refBatch := make([]objects.BatchReference, numThings)
			for i := range refBatch {
				ref := objects.BatchReference{
					From: &crossref.RefSource{
						Local:    true,
						PeerName: "localhost",
						Class:    "TheBestActionClass",
						Property: schema.PropertyName("refProp"),
						TargetID: createdActionIDs[i],
					},
					To: &crossref.Ref{
						Local:    true,
						PeerName: "localhost",
						TargetID: createdThingIDs[i],
					},
				}
				refBatch[i] = ref
			}
			batchRefResp, err := repo.AddBatchReferences(context.Background(), refBatch, nil)
			require.Nil(t, err)
			require.Len(t, batchRefResp, numThings)
			for _, r := range batchRefResp {
				require.Nil(t, r.Err)
			}
		})

		t.Run("query every action for its referenced thing", func(t *testing.T) {
			for i := range createdActionIDs {
				resp, err := repo.Search(context.Background(), dto.GetParams{
					ClassName:            "TheBestActionClass",
					Pagination:           &filters.Pagination{Limit: 5},
					AdditionalProperties: additional.Properties{ID: true},
					Properties: search.SelectProperties{
						{
							Name: "refProp",
							Refs: []search.SelectClass{
								{
									ClassName: "TheBestThingClass",
									RefProperties: search.SelectProperties{
										{
											Name:        "stringProp",
											IsPrimitive: true,
										},
									},
								},
							},
						},
					},
					Filters: &filters.LocalFilter{
						Root: &filters.Clause{
							Operator: filters.OperatorAnd,
							Operands: []filters.Clause{
								{
									Operator: filters.OperatorEqual,
									On: &filters.Path{
										Class:    "TheBestActionClass",
										Property: "stringProp",
									},
									Value: &filters.Value{
										Value: fmt.Sprintf("action#%d", i),
										Type:  schema.DataTypeText,
									},
								},
								{
									Operator: filters.OperatorLike,
									On: &filters.Path{
										Class:    "TheBestActionClass",
										Property: "refProp",
										Child: &filters.Path{
											Class:    "TheBestThingClass",
											Property: "stringProp",
										},
									},
									Value: &filters.Value{
										Value: "thing#*",
										Type:  schema.DataTypeText,
									},
								},
							},
						},
					},
				})

				require.Nil(t, err)
				require.Len(t, resp, 1)
				assert.Len(t, resp[0].Schema.(map[string]interface{})["refProp"], 1)
			}
		})
	})

	t.Run("query obj by id which has no props", func(t *testing.T) {
		id := strfmt.UUID("2cd8a381-6568-4724-9d5c-1ef28d439e94")

		t.Run("insert test obj", func(t *testing.T) {
			vec := []float32{0.1, 0.2, 0.3, 0.4}
			obj := &models.Object{
				ID:     id,
				Class:  "TheBestActionClass",
				Vector: vec,
			}
			require.Nil(t, repo.PutObject(context.Background(), obj, vec, nil))
		})

		t.Run("perform search with id filter", func(t *testing.T) {
			res, err := repo.Search(context.Background(), dto.GetParams{
				Pagination: &filters.Pagination{Limit: 10},
				ClassName:  "TheBestActionClass",
				Filters: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorEqual,
						On: &filters.Path{
							Class:    "TheBestActionClass",
							Property: filters.InternalPropID,
						},
						Value: &filters.Value{
							Value: id.String(),
							Type:  schema.DataTypeText,
						},
					},
				},
			})

			require.Nil(t, err)

			expected := []search.Result{
				{
					ID:        id,
					ClassName: "TheBestActionClass",
					Schema: map[string]interface{}{
						"id": id,
					},
					Score:                0,
					AdditionalProperties: models.AdditionalProperties{},
					Dims:                 4,
				},
			}

			assert.Equal(t, expected, res)
		})
	})
}

func TestCRUD_Query(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "TheBestThingClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), thingclass, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{thingclass},
		},
	}

	t.Run("scroll through all objects", func(t *testing.T) {
		// prepare
		className := "TheBestThingClass"
		thingID1 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000001")
		thingID2 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000002")
		thingID3 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000003")
		thingID4 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000004")
		thingID5 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000005")
		thingID6 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000006")
		thingID7 := strfmt.UUID("7c8183ae-150d-433f-92b6-ed095b000007")
		testData := []struct {
			id         strfmt.UUID
			className  string
			stringProp string
			phone      uint64
			longitude  float32
		}{
			{
				id:         thingID1,
				className:  className,
				stringProp: "a very short text",
			},
			{
				id:         thingID2,
				className:  className,
				stringProp: "zebra lives in Zoo",
			},
			{
				id:         thingID3,
				className:  className,
				stringProp: "the best thing class",
			},
			{
				id:         thingID4,
				className:  className,
				stringProp: "car",
			},
			{
				id:         thingID5,
				className:  className,
				stringProp: "a very short text",
			},
			{
				id:         thingID6,
				className:  className,
				stringProp: "zebra lives in Zoo",
			},
			{
				id:         thingID7,
				className:  className,
				stringProp: "fossil fuels",
			},
		}
		for _, td := range testData {
			object := &models.Object{
				CreationTimeUnix:   1565612833990,
				LastUpdateTimeUnix: 1000001,
				ID:                 td.id,
				Class:              td.className,
				Properties: map[string]interface{}{
					"stringProp": td.stringProp,
				},
			}
			vector := []float32{1.1, 1.3, 1.5, 1.4}
			err := repo.PutObject(context.Background(), object, vector, nil)
			assert.Nil(t, err)
		}
		// toParams helper method
		toParams := func(className string, offset, limit int,
			cursor *filters.Cursor, filters *filters.LocalFilter, sort []filters.Sort,
		) *objects.QueryInput {
			return &objects.QueryInput{
				Class:      className,
				Offset:     offset,
				Limit:      limit,
				Cursor:     cursor,
				Filters:    filters,
				Sort:       sort,
				Additional: additional.Properties{},
			}
		}
		// run scrolling through all results
		tests := []struct {
			name               string
			className          string
			cursor             *filters.Cursor
			query              *objects.QueryInput
			expectedThingIDs   []strfmt.UUID
			constainsErrorMsgs []string
		}{
			{
				name:             "all results with step limit: 100",
				query:            toParams(className, 0, 100, &filters.Cursor{After: "", Limit: 100}, nil, nil),
				expectedThingIDs: []strfmt.UUID{thingID1, thingID2, thingID3, thingID4, thingID5, thingID6, thingID7},
			},
			{
				name:             "all results with step limit: 1",
				query:            toParams(className, 0, 1, &filters.Cursor{After: "", Limit: 1}, nil, nil),
				expectedThingIDs: []strfmt.UUID{thingID1, thingID2, thingID3, thingID4, thingID5, thingID6, thingID7},
			},
			{
				name:             "all results with step limit: 1 after: thingID4",
				query:            toParams(className, 0, 1, &filters.Cursor{After: thingID4.String(), Limit: 1}, nil, nil),
				expectedThingIDs: []strfmt.UUID{thingID5, thingID6, thingID7},
			},
			{
				name:             "all results with step limit: 1 after: thingID7",
				query:            toParams(className, 0, 1, &filters.Cursor{After: thingID7.String(), Limit: 1}, nil, nil),
				expectedThingIDs: []strfmt.UUID{},
			},
			{
				name:             "all results with step limit: 3",
				query:            toParams(className, 0, 3, &filters.Cursor{After: "", Limit: 3}, nil, nil),
				expectedThingIDs: []strfmt.UUID{thingID1, thingID2, thingID3, thingID4, thingID5, thingID6, thingID7},
			},
			{
				name:             "all results with step limit: 7",
				query:            toParams(className, 0, 7, &filters.Cursor{After: "", Limit: 7}, nil, nil),
				expectedThingIDs: []strfmt.UUID{thingID1, thingID2, thingID3, thingID4, thingID5, thingID6, thingID7},
			},
			{
				name:               "error on empty class",
				query:              toParams("", 0, 7, &filters.Cursor{After: "", Limit: 7}, nil, nil),
				constainsErrorMsgs: []string{"class not found"},
			},
			{
				name: "error on sort parameter",
				query: toParams(className, 0, 7,
					&filters.Cursor{After: "", Limit: 7}, nil,
					[]filters.Sort{{Path: []string{"stringProp"}, Order: "asc"}},
				),
				cursor:             &filters.Cursor{After: "", Limit: 7},
				constainsErrorMsgs: []string{"sort cannot be set with after and limit parameters"},
			},
			{
				name: "error on offset parameter",
				query: toParams(className, 10, 7,
					&filters.Cursor{After: "", Limit: 7}, nil,
					nil,
				),
				cursor:             &filters.Cursor{After: "", Limit: 7},
				constainsErrorMsgs: []string{"offset cannot be set with after and limit parameters"},
			},
			{
				name: "error on offset and sort parameter",
				query: toParams(className, 10, 7,
					&filters.Cursor{After: "", Limit: 7}, nil,
					[]filters.Sort{{Path: []string{"stringProp"}, Order: "asc"}},
				),
				cursor:             &filters.Cursor{After: "", Limit: 7},
				constainsErrorMsgs: []string{"offset,sort cannot be set with after and limit parameters"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if len(tt.constainsErrorMsgs) > 0 {
					res, err := repo.Query(context.Background(), tt.query)
					require.NotNil(t, err)
					assert.Nil(t, res)
					for _, errorMsg := range tt.constainsErrorMsgs {
						assert.Contains(t, err.Error(), errorMsg)
					}
				} else {
					cursorSearch := func(t *testing.T, className string, cursor *filters.Cursor) []strfmt.UUID {
						res, err := repo.Query(context.Background(), toParams(className, 0, cursor.Limit, cursor, nil, nil))
						require.Nil(t, err)
						var ids []strfmt.UUID
						for i := range res {
							ids = append(ids, res[i].ID)
						}
						return ids
					}

					var thingIds []strfmt.UUID
					cursor := tt.query.Cursor
					for {
						result := cursorSearch(t, tt.query.Class, cursor)
						thingIds = append(thingIds, result...)
						if len(result) == 0 {
							break
						}
						after := result[len(result)-1]
						cursor = &filters.Cursor{After: after.String(), Limit: cursor.Limit}
					}

					require.Equal(t, len(tt.expectedThingIDs), len(thingIds))
					for i := range tt.expectedThingIDs {
						assert.Equal(t, tt.expectedThingIDs[i], thingIds[i])
					}
				}
			})
		}
		// clean up
		for _, td := range testData {
			err := repo.DeleteObject(context.Background(), td.className, td.id, nil, "")
			assert.Nil(t, err)
		}
	})
}

func Test_ImportWithoutVector_UpdateWithVectorLater(t *testing.T) {
	r := getRandomSeed()
	total := 100
	individual := total / 4
	className := "DeferredVector"
	var data []*models.Object
	var class *models.Class

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("prepare data for test", func(t *testing.T) {
		data = make([]*models.Object, total)
		for i := range data {
			data[i] = &models.Object{
				ID:    strfmt.UUID(uuid.Must(uuid.NewRandom()).String()),
				Class: className,
				Properties: map[string]interface{}{
					"int_prop": int64(i),
				},
				Vector: nil,
			}
		}
	})

	t.Run("create required schema", func(t *testing.T) {
		class = &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					DataType: []string{string(schema.DataTypeInt)},
					Name:     "int_prop",
				},
			},
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	t.Run("import individual objects without vector", func(t *testing.T) {
		for i := 0; i < individual; i++ {
			err := repo.PutObject(context.Background(), data[i], nil, nil) // nil vector !
			require.Nil(t, err)
		}
	})

	t.Run("import batch objects without vector", func(t *testing.T) {
		batch := make(objects.BatchObjects, total-individual)

		for i := range batch {
			batch[i] = objects.BatchObject{
				OriginalIndex: i,
				Err:           nil,
				Vector:        nil,
				Object:        data[i+individual],
				UUID:          data[i+individual].ID,
			}
		}

		res, err := repo.BatchPutObjects(context.Background(), batch, nil)
		require.Nil(t, err)

		for _, obj := range res {
			require.Nil(t, obj.Err)
		}
	})

	t.Run("verify inverted index works correctly", func(t *testing.T) {
		res, err := repo.Search(context.Background(), dto.GetParams{
			Filters:   buildFilter("int_prop", total+1, lte, dtInt),
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
		})
		require.Nil(t, err)
		assert.Len(t, res, total)
	})

	t.Run("perform unfiltered vector search and verify there are no matches", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			Filters:   nil,
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(r, 7),
		})
		require.Nil(t, err)
		assert.Len(t, res, 0) // we skipped the vector on half the elements, so we should now match half
	})

	t.Run("update some of the objects to add vectors", func(t *testing.T) {
		for i := range data {
			if i%2 == 1 {
				continue
			}

			data[i].Vector = randomVector(r, 7)
			err := repo.PutObject(context.Background(), data[i], data[i].Vector, nil)
			require.Nil(t, err)
		}
	})

	t.Run("perform unfiltered vector search and verify correct matches", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			Filters:   nil,
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(r, 7),
		})
		require.Nil(t, err)
		assert.Len(t, res, total/2) // we skipped the vector on half the elements, so we should now match half
	})

	t.Run("perform filtered vector search and verify correct matches", func(t *testing.T) {
		res, err := repo.VectorSearch(context.Background(), dto.GetParams{
			Filters:   buildFilter("int_prop", 50, lt, dtInt),
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(r, 7),
		})
		require.Nil(t, err)
		// we skipped the vector on half the elements, and cut the list in half with
		// the filter, so we're only expected a quarter of the total size now
		assert.Len(t, res, total/4)
	})
}

func TestVectorSearch_ByDistance(t *testing.T) {
	className := "SomeClass"
	var class *models.Class

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter: 60,
		RootPath:                dirName,
		// this is set really low to ensure that search
		// by distance is conducted, which executes
		// without regard to this value
		QueryMaximumResults:       1,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("create required schema", func(t *testing.T) {
		class = &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					DataType: []string{string(schema.DataTypeInt)},
					Name:     "int_prop",
				},
			},
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	searchVector := []float32{-0.10190568, -0.06259751, 0.05616188, -0.19249836, 0.09714927, -0.1902525, -0.064424865, -0.0387358, 0.17581701, 0.4476738, 0.29261824, 0.12026761, -0.19975126, 0.023600178, 0.17348698, 0.12701738, -0.36018127, -0.12051587, -0.17620522, 0.060741074, -0.064512916, 0.18640806, -0.1529852, 0.08211839, -0.02558465, -0.11369845, 0.0924098, -0.10544433, -0.14728987, -0.041860342, -0.08533595, 0.25886244, 0.2963937, 0.26010615, 0.2111097, 0.029396622, 0.01429563, 0.06410264, -0.119665794, 0.33583277, -0.05802661, 0.023306102, 0.14435922, -0.003951336, -0.13870825, 0.07140894, 0.10469943, -0.059021875, -0.065911904, 0.024216041, -0.26282874, 0.04896568, -0.08291928, -0.12793182, -0.077824734, 0.08843151, 0.31247458, -0.066301286, 0.006904921, -0.08277095, 0.13936226, -0.64392364, -0.19566211, 0.047227614, 0.086121306, -0.20725192, -0.096485816, -0.16436341, -0.06559169, -0.019639932, -0.012729637, 0.08901619, 0.0015896161, -0.24789932, 0.35496348, -0.16272856, -0.01648429, 0.11247674, 0.08099968, 0.13339259, 0.055829972, -0.34662855, 0.068509, 0.13880715, 0.3201848, -0.055557363, 0.22142135, -0.12867308, 0.0037871755, 0.24888979, -0.007443307, 0.08906625, -0.02022331, 0.11510742, -0.2385861, 0.16177008, -0.16214795, -0.28715602, 0.016784908, 0.19386634, -0.07731616, -0.100485384, 0.4100712, 0.061834496, -0.2325293, -0.026056025, -0.11632323, -0.17040555, -0.081960455, -0.0061040106, -0.05949373, 0.044952348, -0.079565264, 0.024430245, -0.09375341, -0.30249637, 0.115205586, -0.13083287, -0.04264671, -0.089810364, 0.16227561, 0.07318055, -0.10496504, 0.00063501706, -0.04936106, -0.0022282854, 1.0893154, 0.1698662, -0.019563455, -0.011128426, 0.04477475, -0.15656771, -0.056911886, -0.5759019, -0.1881429, 0.17088258, 0.24124439, 0.111288875, -0.0015475494, -0.021278847, -0.08362156, 0.09997524, -0.094385885, -0.1674031, 0.061180864, 0.28517494, -0.016217072, 0.025866214, -0.22854298, -0.17924422, -0.037767246, 0.12252907, -0.31698978, -0.038031228, 0.055408552, 0.1743545, -0.040576655, 0.1293942, -0.56650764, -0.10306195, -0.19548112, -0.245544, -0.018241389, -0.039024632, -0.31659162, 0.1565075, 0.08412337, 0.13177724, -0.13766576, -0.15355161, -0.16960397, -0.012436442, 0.04828157, 0.12566057, -0.35308784, -0.37520224, -0.1265899, -0.13991497, 0.14402144, 0.117542416, -0.20750546, -0.5849919, -0.010469457, -0.19677396, 0.011365964, 0.00666846, -0.083470255, 0.24928358, 0.07026387, 0.19082819, 0.24557637, 0.014292963, 0.14846677, 0.031625308, -0.20398879, 0.19507346, -0.18119761, -0.045725327, -0.042455163, -0.099733196, -0.33636123, -0.28447086, 0.30274838, -0.01603988, -0.0529655, 0.15784146, 0.08746072, -0.1703993, 0.2414512, 0.060322937, -0.00812057, 0.031162385, -0.1764905, 0.22107981, -0.016657066, 0.31948856, 0.07282925, -0.036991462, 0.01266936, -0.009106514, -0.038732465, 0.20973183, 0.033236098, -0.10673938, -0.06880061, 0.115524575, -0.39688373, 0.08749971, -0.21816005, -0.22100002, -0.3716853, -0.14720486, 0.24316181, 0.29673144, 0.020808747, 0.07658521, 0.16310681, 0.38785335, 0.0992224, 0.14177811, 0.025954131, -0.08690783, 0.19653428, 0.09584941, 0.040072605, -0.00038361162, -0.094546966, 0.1910902, 0.13217318, 0.060072783, -0.0655816, 0.2777626, 0.1799169, 0.20187178, -0.0996889, -0.01932122, -0.13133621, 0.057482753, -0.36892185, -0.032093313, 0.14607865, 0.12033318, -0.041683596, -0.2048406, -0.041777443, -0.14975598, -0.2526341, 0.12659752, 0.010567178, -0.297333, -0.27522174, 0.06923473, 0.043150593, -0.017045585, -0.2400216, 0.11413547, -0.40081662, -0.0018820907, 0.13800722, 0.085972115, -0.01519989, -0.10491216, 0.09170084, 0.063085504, 0.046743374, -0.014466267, 0.09880224, 0.027706565, 0.09951337, 0.17317492, -0.025654864, 0.14658073, 0.042377427, -0.08402882, -0.12423425, 0.32714987, -0.1527207, 0.106094465, 0.017378228, -0.06302387}
	searchObject := strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3")

	tests := map[strfmt.UUID]struct {
		inputVec []float32
		expected bool
	}{
		strfmt.UUID("88460290-03b2-44a3-9adb-9fa3ae11d9e6"): {
			inputVec: []float32{-0.11015724, -0.05380307, 0.027512914, -0.16925375, 0.08306809, -0.19312492, -0.08910436, -0.011051652, 0.17981204, 0.40469593, 0.28226805, 0.09381516, -0.18380599, 0.03102771, 0.1645333, 0.1530153, -0.3187937, -0.10800173, -0.18466279, 0.0004336393, -0.0495677, 0.19905856, -0.11614494, 0.08834681, -0.011200292, -0.11969374, 0.12497086, -0.12427251, -0.13395442, -0.0060353535, -0.07504816, 0.23205791, 0.2982508, 0.2517544, 0.176147, -0.036871903, 0.017852835, 0.040007118, -0.118621, 0.3648693, -0.058933854, 0.04004229, 0.11871147, -0.019860389, -0.12701912, 0.106662825, 0.086498804, -0.04303973, -0.0742352, 0.018250324, -0.26544014, 0.029228423, -0.087171465, -0.1282789, -0.06403083, 0.09680911, 0.31433868, -0.081510685, -0.011283603, -0.041624587, 0.16530018, -0.6714878, -0.2436993, 0.03173918, 0.106117725, -0.20803581, -0.10429562, -0.16975354, -0.078582145, -0.0065962705, -0.06840946, 0.094937086, -0.020617036, -0.23795949, 0.34785536, -0.19834635, -0.015064479, 0.11930141, 0.090962164, 0.120560184, 0.054095767, -0.38602966, 0.057141174, 0.12039684, 0.32000408, -0.05146908, 0.20762976, -0.09342379, 0.037577383, 0.23894139, -0.0075003104, 0.104791366, -0.015841056, 0.102840215, -0.20813248, 0.1855997, -0.12594056, -0.27132365, -0.0055563124, 0.21954241, -0.10798524, -0.111896284, 0.44049335, 0.049884494, -0.22339955, -0.005374135, -0.120713554, -0.22275059, -0.09146004, 0.017188415, -0.106493734, 0.045247544, -0.07725446, 0.056848228, -0.10294392, -0.2896642, 0.112891, -0.13773362, -0.089911595, -0.13500965, 0.14051703, 0.040092673, -0.13896292, 0.04580957, -0.014300959, 0.03737215, 1.0661443, 0.19767477, -0.07703914, -0.012910904, -0.0037716173, -0.14437087, -0.06938004, -0.5348036, -0.16047458, 0.19416414, 0.21938956, 0.092242256, -0.012630808, -0.021863988, -0.051702406, 0.08780951, -0.0815602, -0.15332024, 0.077632725, 0.25709584, -0.025725808, 0.042116437, -0.22687604, -0.11791685, -0.028626656, 0.16734225, -0.3017483, -0.03236202, 0.02888077, 0.18193199, -0.009032297, 0.14454253, -0.511494, -0.12119192, -0.20757924, -0.2561716, -0.03904554, -0.07348411, -0.28547177, 0.15967208, 0.079396725, 0.14358875, -0.12829632, -0.18175666, -0.15540425, -0.020419862, 0.019070208, 0.12168836, -0.3428434, -0.357543, -0.11218741, -0.12834033, 0.13564876, 0.12768728, -0.1817461, -0.61235875, -0.029409664, -0.19765733, 0.03872163, 0.0074950717, -0.10025679, 0.2872255, 0.033995092, 0.12945211, 0.21831632, 0.04666009, 0.14233032, 0.016767867, -0.2039244, 0.2000191, -0.13099428, -0.020210614, -0.06286195, -0.0948797, -0.34830436, -0.21595824, 0.32722405, -0.024735296, -0.07859145, 0.16975155, 0.08186461, -0.19249061, 0.23405583, 0.04837592, 0.021467948, -0.022215014, -0.14892808, 0.23908867, -0.050126728, 0.2867907, 0.07740656, -0.01714987, -0.0046314416, -0.0048108613, -0.007407311, 0.1807499, 0.049772616, -0.14680666, -0.07335314, 0.09023705, -0.40600133, 0.05522128, -0.20085222, -0.20410904, -0.34319055, -0.10792269, 0.2297779, 0.30397663, 0.05230268, 0.06408224, 0.13797496, 0.3691112, 0.083033495, 0.13695791, -0.015612457, -0.06413475, 0.18117142, 0.12928344, 0.049171276, 0.016104931, -0.102417335, 0.19589683, 0.14380622, 0.0748065, -0.005402455, 0.27243868, 0.14925551, 0.19564849, -0.10738364, -0.054175537, -0.10068278, 0.06004795, -0.38755924, -0.01654251, 0.1394104, 0.0968949, 0.004271706, -0.17105898, -0.050423585, -0.15311627, -0.24458972, 0.12665795, -0.022814916, -0.23887472, -0.289588, 0.05521137, 0.041259795, -0.021133862, -0.23674431, 0.08424598, -0.37863016, 0.017239956, 0.13776784, 0.060790475, 0.057887543, -0.08784489, 0.08803934, 0.027996546, 0.085972995, -0.014455558, 0.11668073, 0.03812387, 0.088413864, 0.22228678, -0.015599858, 0.11000236, 0.035271563, -0.08088438, -0.13092226, 0.29378533, -0.12311522, 0.09377676, 0.02948718, -0.09136077},
			expected: true,
		},
		strfmt.UUID("c99bc97d-7035-4311-94f3-947dc6471f51"): {
			inputVec: []float32{-0.07545987, -0.046643265, 0.044445477, -0.18531442, 0.07922216, -0.19388637, -0.069393866, -0.036144026, 0.1713317, 0.41803706, 0.23576374, 0.073170714, -0.14085358, 0.012535708, 0.17439325, 0.10057567, -0.33506152, -0.06800867, -0.18882714, 0.002687021, -0.03276807, 0.17267752, -0.13951558, 0.071382746, 0.020254405, -0.10178502, 0.13977699, -0.107296936, -0.113307, -0.002506761, -0.092065684, 0.21008658, 0.31157792, 0.19640765, 0.15769793, -0.0050196033, 0.0022481605, 0.015436289, -0.11822955, 0.31494477, -0.07425527, 0.051401984, 0.11648046, -0.00016831602, -0.12758006, 0.06814693, 0.06108981, -0.025454175, -0.018695071, 0.041827776, -0.23480764, 0.06652185, -0.078328855, -0.121668324, -0.04341973, 0.114403985, 0.32614416, -0.07992741, -0.019665314, -0.017408244, 0.12615794, -0.6350545, -0.17056493, 0.07171332, 0.047071394, -0.18428493, -0.09011123, -0.15995751, -0.03345579, -0.014678, -0.038699757, 0.044125225, 0.0042562615, -0.29445595, 0.30460796, -0.13630153, 0.00014055961, 0.08996278, 0.08948901, 0.12164838, 0.079090506, -0.36153567, 0.02817218, 0.11914518, 0.29805067, -0.07431765, 0.16793592, -0.099549234, 0.045226075, 0.22235383, -0.045654725, 0.09233901, -0.004902314, 0.08621588, -0.19723448, 0.19557382, -0.13199815, -0.22924824, -0.015981175, 0.19762704, -0.08940076, -0.084909916, 0.43372774, 0.026998578, -0.20827708, 0.037450224, -0.078997016, -0.18065391, -0.071308024, 0.00870316, -0.114981964, 0.017085023, -0.07696264, 0.009330409, -0.097458, -0.25530958, 0.118254915, -0.12516825, -0.008301536, -0.20432962, 0.15235707, 0.012840041, -0.18034773, 0.039270073, -0.03131139, 0.013706253, 0.98688674, 0.18840753, -0.055119563, 0.00836046, 0.019445436, -0.10701598, -0.024610046, -0.50088006, -0.15488546, 0.14209819, 0.1798376, 0.08615982, -0.0119235935, -0.0060070297, -0.08406098, 0.10096481, -0.09077014, -0.15957798, 0.10556352, 0.2100476, -0.036947068, 0.05316554, -0.20480183, -0.14873864, -0.0069070593, 0.16027303, -0.288908, -0.04487129, 0.0705415, 0.11973847, -0.0017247469, 0.14092937, -0.5262047, -0.094283305, -0.19120996, -0.2816572, -0.010916339, -0.07984056, -0.28659204, 0.13706332, 0.07364347, 0.12300072, -0.17554194, -0.16378267, -0.15244205, 0.00075927645, 0.017289847, 0.12072629, -0.33452734, -0.33727616, -0.12780978, -0.09350711, 0.105674624, 0.10770573, -0.17278843, -0.5760599, -0.013741414, -0.15395893, 0.009837732, 0.015417911, -0.11384676, 0.24567491, 0.04905973, 0.10762609, 0.2131752, 0.019281652, 0.11665857, 0.022718405, -0.2234067, 0.23241606, -0.12194457, -0.049972955, -0.012225418, -0.14856412, -0.386102, -0.23018965, 0.28920102, -0.023396742, -0.114672944, 0.12130062, 0.05654803, -0.16194181, 0.24095012, 0.03644393, 0.028024165, -0.008832254, -0.16496961, 0.19496499, -0.035887964, 0.25981775, 0.0970074, 0.0013458093, -0.009548204, 0.040741496, -0.019192837, 0.20718361, -0.004034228, -0.1343262, -0.06990001, 0.09888768, -0.35942966, 0.043895893, -0.19182123, -0.17963983, -0.3222771, -0.10223457, 0.23866613, 0.25855777, 0.04051543, 0.08756274, 0.15683484, 0.37856522, 0.04853359, 0.10198129, -0.0061066896, -0.049892712, 0.17087941, 0.14563805, 0.06984385, 0.0071270005, -0.11838641, 0.18716812, 0.14013803, 0.05242403, 0.034357738, 0.3083466, 0.14742611, 0.17841975, -0.124118194, -0.014102871, -0.052544866, 0.037493005, -0.33485797, -0.013164912, 0.1066288, 0.11141791, -0.04029921, -0.16429856, -0.032241724, -0.15965424, -0.2430594, 0.13654563, 0.009401224, -0.2045843, -0.28467956, 0.07325551, 0.027996557, -0.033877768, -0.24350801, 0.08329816, -0.35555813, 0.006908567, 0.07227365, 0.03188268, 0.032559503, -0.09180395, 0.05601515, 0.0047281734, 0.06878795, -0.018943194, 0.08251342, 0.042039152, 0.12902294, 0.20526606, -0.014881293, 0.11723917, 0.0115632, -0.09016013, -0.12117223, 0.31020245, -0.111444525, 0.077845715, 0.00046715315, -0.104099475},
			expected: true,
		},
		strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3"): {
			inputVec: []float32{-0.20739016, -0.19551805, 0.06645163, 0.008650202, 0.03700748, -0.04132599, -0.029881354, 0.04684896, 0.096614264, 0.42888844, 0.10003969, 0.026234219, -0.051639702, -0.118660435, 0.14473079, 0.2911885, -0.1180539, -0.16804434, -0.48081538, 0.021702053, 0.12612472, 0.15442817, -0.05836532, 0.074295096, -0.28077397, -0.24297802, 0.047836643, -0.36753318, -0.30482984, 0.09265357, 0.25571078, 0.41130066, 0.46177864, 0.34033778, 0.20721313, -0.37726295, 0.07721501, 0.08009689, 0.00027321206, 0.5168123, -0.15305339, 0.0937765, 0.096195236, -0.21120761, 0.014014921, 0.3133104, 0.20773117, 0.08483507, -0.27784437, -0.17281856, -0.6050923, -0.22439326, -0.16914369, -0.3149047, -0.13828672, 0.16334395, -0.0018224253, -0.024342008, 0.3511251, 0.04979151, 0.34223744, -0.6965703, -0.36211932, -0.27092442, 0.34418032, -0.09667905, 0.13344757, -0.15622364, -0.24129291, 0.06958589, -0.2681816, -0.09497071, -0.08923615, -0.06642436, 0.48688608, -0.33535984, 0.014242731, 0.079838976, 0.32949054, 0.09051045, -0.2653392, -0.47393548, 0.07508276, 0.0062832804, 0.724184, -0.18929236, 0.11718613, 0.049603477, 0.08766128, 0.31040704, 0.04038693, -0.0017023507, -0.18986607, 0.056264438, -0.20978904, -0.107441366, -0.30505633, -0.45781082, -0.11571784, 0.32160303, -0.1347523, -0.08090298, 0.51651996, -0.023250414, -0.18725531, -0.14222279, 0.009277832, -0.49789724, -0.25156206, 0.0042495225, 0.0038805408, -0.031416763, 0.10277136, 0.14383446, -0.23241928, -0.42357358, 0.027033398, -0.2262604, -0.2685295, -0.14510548, 0.18256307, 0.063297585, 0.027636252, 0.081166506, 0.06726344, 0.1677495, 1.5217289, 0.33152232, -0.2209926, 0.051426213, 0.15640806, -0.30210486, -0.32857975, -0.4170022, -0.028293105, 0.28772062, 0.50510746, 0.09162247, -0.12383193, -0.25066972, -0.1441897, 0.107192926, -0.07404076, 0.0042472635, 0.11014519, 0.22332853, 0.09434378, -0.3278343, 0.041899726, 0.06838457, 0.10983681, 0.11864574, -0.25336757, -0.047530346, -0.027303243, 0.37403497, 0.13420461, 0.14946426, -0.41996637, -0.037703935, -0.47961184, -0.29839846, -0.103934005, -0.12058302, -0.12806267, 0.22814582, 0.3904893, -0.16044962, -0.17479864, -0.33139735, -0.29185295, 0.0653074, 0.042426735, 0.06092335, -0.18776153, -0.52555144, -0.15889317, -0.20644087, 0.2293067, 0.26668283, -0.15607063, -0.696593, -0.08224992, -0.4283747, 0.26883888, -0.031052848, -0.1311875, 0.26636878, 0.16457985, 0.15660451, 0.10629464, 0.17345549, 0.23963387, 0.22997221, -0.111713186, -0.08499592, -0.2274625, 0.19285984, -0.08285016, -0.02692149, -0.3426618, -0.13361897, 0.2870389, -0.12032792, -0.22944619, 0.25588584, 0.24607788, -0.2762531, 0.30983892, 0.011088746, -0.15739818, 0.053215, -0.21660997, 0.033805694, -0.17886437, 0.2979239, 0.2163545, -0.08381542, 0.19666128, -0.28977823, -0.20994817, -0.012160099, 0.057499636, -0.12549455, 0.19303595, -0.14420606, -0.51937664, 0.23400985, -0.27893808, -0.2660984, -0.27870297, -0.32149136, 0.19958079, 0.34468395, 0.18947665, -0.16529581, 0.101419374, 0.30195153, 0.09030288, 0.12496541, 0.02999903, -0.016697621, 0.15314853, 0.27848768, 0.24102053, 0.06933273, 0.08923653, 0.10477832, 0.4389032, 0.15679164, -0.11119637, 0.134823, 0.30230528, 0.20818473, -0.005579584, -0.3474488, -0.44394243, 0.22270252, -0.3668763, 0.07474772, 0.011691334, 0.088187896, 0.23832949, -0.07960201, 0.066471875, 0.034641538, -0.39984587, 0.0032980456, -0.28492525, -0.46358657, -0.2148288, -0.107226945, 0.02734428, -0.24686679, -0.123900555, 0.18174778, -0.31248868, 0.13808723, 0.31549984, 0.21521719, 0.13966985, -0.27272752, 0.12091104, 0.14257833, 0.23175247, 0.15639938, 0.40828535, 0.31916845, 0.023645567, 0.20658277, -0.20365283, 0.113746524, 0.13173752, -0.050343305, -0.31581175, 0.09704622, -0.014172505, 0.16924341, 0.30327854, -0.17770194},
			expected: false,
		},
		strfmt.UUID("e7bf6c45-de72-493a-b273-5ef198974d61"): {
			inputVec: []float32{0.089313604, -0.050221898, 0.18352903, 0.16257699, 0.14520381, 0.17993976, 0.14594483, 0.019256027, -0.15505213, 0.23606326, -0.14456263, 0.2679586, -0.112208664, 0.12997514, 0.0051072896, 0.28151348, -0.10495799, 0.026782967, -0.38603118, 0.16190273, -0.0428943, -0.16265322, -0.17910561, 0.0746288, -0.3117934, -0.15871756, -0.11377734, -0.06822346, -0.13829489, 0.13019162, 0.30741218, 0.16194165, 0.013218932, 0.054517113, 0.12490437, -0.07709048, 0.02556826, -0.21159878, -0.09082174, 0.24629511, 0.05013666, 0.25168124, -0.14423938, -0.0937688, -0.07811525, -0.049346007, 0.3592527, 0.30411252, -0.1168557, 0.18870471, 0.06614835, -0.20099068, -0.084436245, 0.073036775, -0.03448665, -0.11147946, -0.10862863, -0.012393957, 0.18990599, 0.060957544, 0.19518377, -0.027541652, -0.26750082, -0.12780671, 0.09570065, -0.03541132, 0.094820626, -0.13539355, -0.09468136, 0.18476579, -0.20970085, -0.20989786, -0.12084438, -0.04517079, -0.008074663, 0.02824076, 0.114496395, -0.20462593, 0.103516705, -0.101554185, -0.1374868, -0.24884155, -0.08101618, -0.016105993, 0.22608215, -0.007247754, -0.17246912, 0.058247145, -0.041018173, 0.19471274, -0.022576109, 0.032828204, -0.079321206, -0.09259324, 0.041115705, -0.25280195, -0.28517374, -0.19496292, 0.18070905, 0.06384923, -0.004056949, 0.1536253, 0.17861623, -0.033833142, 0.12039968, 0.04458716, 0.08793809, -0.15683243, -0.1087904, 0.1741014, 0.007256374, -0.20265253, 0.034111258, 0.03311363, -0.09449356, -0.13161612, -0.026084669, 0.07609202, 0.03452338, 0.08840356, -0.044566724, 0.1507175, 0.089273594, 0.18872644, 0.18333815, -0.023196407, 0.63831943, 0.20309874, 0.10217627, 0.11445079, 0.18965706, -0.16809432, -0.343172, -0.06439529, 0.08362327, 0.32746288, 0.38483366, 0.020372175, -0.25239283, 0.019468365, -0.016367752, 0.016749177, 0.024621855, 0.030529505, 0.20601188, -0.100692995, -0.16414656, -0.23193358, 0.26616478, 0.06166736, 0.14341855, 0.1294041, 0.045133967, 0.0014262896, -0.0194398, 0.040737696, 0.10099013, -0.10838136, -0.28768313, -0.073719576, -0.15836753, -0.10482511, -0.1349642, -0.107005455, 0.01957546, 0.13799994, 0.056444198, -0.38841644, -0.07585945, -0.018703599, -0.19934878, 0.15176265, 0.04133126, 0.063531734, 0.09720055, -0.29999572, 0.04765686, -0.23604262, 0.081500284, 0.056092553, -0.13664724, -0.37729686, 0.031137427, -0.052083906, 0.117984496, -0.14562207, -0.029609507, 0.13725121, 0.090367764, 0.12787215, 0.11026589, 0.25123242, 0.12911159, 0.055398554, 0.0032232201, 0.026706887, 0.14584258, 0.019900957, -0.12197998, -0.087177716, -0.24649806, -0.17869286, 0.07139921, -0.09633085, -0.16027117, 0.23617831, 0.05429949, -0.061085824, 0.040451035, 0.052443117, -0.14255014, 0.15598148, -0.2336374, 0.08394173, -0.34318882, 0.3419207, 0.18282516, -0.03709172, 0.10525048, -0.1871602, -0.22663523, 0.01635051, 0.16996534, -0.18056048, -0.169894, -0.18467705, -0.3641231, 0.060861763, -0.080082566, -0.08888943, 0.11629789, -0.00973362, 0.07452957, 0.25680214, 0.042024083, -0.024963235, 0.1743134, 0.10921186, 0.25191578, 0.028438354, 0.004781374, -0.08364819, 0.051807538, 0.1165724, 0.29184434, -0.21512283, 0.12515399, -0.08803969, 0.41930157, -0.10181762, 0.038189832, 0.085555896, -0.026453126, 0.04717047, 0.12667313, 0.023158737, -0.45877644, 0.18732828, 0.062374037, -0.21956007, -0.04449947, 0.19028638, 0.1359094, 0.26384917, 0.077602044, 0.35136092, 0.069637895, 0.048263475, -0.02498448, -0.09221205, -0.012142404, -0.124592446, 0.14599627, -0.050875153, -0.25454503, -0.069588415, -0.29793787, -0.13407284, 0.25388947, 0.35565627, -0.034204755, 0.0024766966, 0.086427726, -0.054318108, 0.063218184, -0.037823644, 0.108287826, 0.14440496, 0.025134278, 0.14978257, -0.03355889, 0.02980915, -0.13764386, 0.4167542, -0.03938922, 0.026970355, 0.24595529, 0.111741625, -0.074567944, -0.057232533},
			expected: false,
		},
		strfmt.UUID("0999d109-1d5f-465a-bd8b-e3fbd46f10aa"): {
			inputVec: []float32{-0.10486144, -0.07437922, 0.069469325, -0.1438278, 0.07740161, -0.18606456, -0.09991434, -0.020051572, 0.19863395, 0.4347328, 0.297606, 0.07853262, -0.16025662, 0.023596637, 0.16935731, 0.17052403, -0.29870638, -0.10309007, -0.20055692, 0.0027809117, -0.03928043, 0.21178603, -0.13793766, 0.08118157, 0.006693433, -0.13829204, 0.14778963, -0.13180175, -0.21128704, -0.0026104634, -0.076393716, 0.22200249, 0.32417125, 0.26045212, 0.1783609, -0.114116184, 0.0100981165, 0.07233143, -0.15913877, 0.4238603, -0.036907215, 0.0595873, 0.0807002, -0.07637312, -0.12889846, 0.111177936, 0.091114685, -0.018454906, -0.12132672, 0.056664582, -0.30461523, 0.020763714, -0.10992191, -0.14430659, -0.092879646, 0.13615008, 0.33039626, -0.115675874, 0.03607886, -0.027918883, 0.19531779, -0.7211654, -0.23073879, 0.011791817, 0.1315166, -0.22779183, -0.13773227, -0.1814997, -0.09008116, 0.021698939, -0.102921166, 0.090760864, 0.011856942, -0.25561005, 0.40769714, -0.21286584, -0.018059848, 0.13812906, 0.079457305, 0.12631191, 0.0024881593, -0.4282836, 0.0619608, 0.12207897, 0.39083096, -0.009502015, 0.19990632, -0.06503092, 0.0635979, 0.27579078, -0.020699967, 0.068474516, 0.0043831975, 0.10303624, -0.1885405, 0.22989234, -0.15952443, -0.29842895, 0.006752088, 0.22831629, -0.13150804, -0.13695218, 0.5357904, 0.050116863, -0.24064547, -0.01375713, -0.096647836, -0.24984525, -0.10429946, 0.002098812, -0.08113263, 0.05237009, -0.10246039, 0.05234802, -0.13899775, -0.3439524, 0.12522809, -0.18406768, -0.09022853, -0.19954625, 0.15810682, 0.039185096, -0.13576287, 0.045047805, 0.0035671506, 0.055920787, 1.1730403, 0.24019612, -0.13423051, -0.008052084, -0.00431602, -0.17079304, -0.09064658, -0.58728856, -0.1365065, 0.22919424, 0.22795208, 0.13396585, 0.018962797, -0.0075796233, -0.072394304, 0.10908417, -0.10881145, -0.16565171, 0.10378018, 0.27296618, -0.059810717, 0.03355443, -0.22429268, -0.12499127, -0.0441017, 0.20800696, -0.29992488, -0.003536096, 0.0026575085, 0.2427503, -0.007395092, 0.13233404, -0.5494433, -0.13144702, -0.2899963, -0.27367246, -0.05257514, -0.0939783, -0.267614, 0.16651331, 0.13891254, 0.08047202, -0.14046521, -0.19062972, -0.1433134, 0.0067776316, 0.00207368, 0.12986982, -0.35847133, -0.41852546, -0.15541135, -0.09865207, 0.14805861, 0.17072491, -0.22655731, -0.6473966, -0.007884447, -0.2060257, 0.035390265, 0.02781265, -0.09760371, 0.30535778, 0.047540557, 0.14565119, 0.21733035, 0.06558403, 0.13184759, 0.044231005, -0.22218557, 0.1897204, -0.1596938, 0.017510587, -0.030249557, -0.082377456, -0.39669412, -0.18365891, 0.34806964, -0.024830062, -0.06955674, 0.21521395, 0.1201222, -0.21855503, 0.23522708, 0.038058903, -0.019610198, -0.025448406, -0.18122384, 0.26068974, -0.055872105, 0.29595166, 0.11005987, -0.00841942, 0.006325112, -0.0013332894, -0.025598384, 0.17320716, 0.03480282, -0.1504056, -0.07133905, 0.08367911, -0.41866872, 0.062191408, -0.14972427, -0.18488628, -0.37027854, -0.14803104, 0.23587811, 0.33285886, 0.059688937, 0.030515533, 0.16795416, 0.3813925, 0.0755207, 0.15504116, -0.003507182, -0.08249321, 0.24292688, 0.13771294, 0.08057683, 0.016365156, -0.12878628, 0.1833687, 0.17496476, 0.050333332, 0.008188007, 0.32129762, 0.15476923, 0.2052587, -0.060781036, -0.1502798, -0.10187848, 0.11062117, -0.41137248, 0.016532877, 0.107270226, 0.08759128, 0.011842419, -0.17039144, -0.0139911, -0.13244899, -0.23845059, 0.075682834, -0.052250806, -0.30011725, -0.28581655, -0.00055503653, 0.022204043, -0.08598292, -0.24763824, 0.08245162, -0.39607832, 0.008443992, 0.16124122, 0.08812278, 0.0335653, -0.09692297, 0.07613783, 0.033542078, 0.11447116, -0.0069911424, 0.09004892, 0.09898015, 0.14595516, 0.24977732, -0.0018444546, 0.06290809, 0.013354713, -0.10336537, -0.1028908, 0.31109008, -0.110210516, 0.07165067, 0.050161615, -0.11413514},
			expected: true,
		},
	}

	t.Run("insert test objects", func(t *testing.T) {
		for id, props := range tests {
			err := repo.PutObject(context.Background(), &models.Object{Class: className, ID: id}, props.inputVec, nil)
			require.Nil(t, err)
		}
	})

	t.Run("perform nearVector search by distance", func(t *testing.T) {
		results, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Distance: 0.1,
			},
			SearchVector:         searchVector,
			AdditionalProperties: additional.Properties{Distance: true},
		})
		require.Nil(t, err)
		require.NotEmpty(t, results)
		// ensure that we receive more results than
		// the `QueryMaximumResults`, as this should
		// only apply to limited vector searches
		require.Greater(t, len(results), 1)

		for _, res := range results {
			if props, ok := tests[res.ID]; !ok {
				t.Fatalf("received unexpected result: %+v", res)
			} else {
				assert.True(t, props.expected, "result id was not intended to meet threshold %s", res.ID)
			}
		}
	})

	t.Run("perform nearObject search by distance", func(t *testing.T) {
		results, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearObject: &searchparams.NearObject{
				Distance: 0.1,
				ID:       searchObject.String(),
			},
			SearchVector:         searchVector,
			AdditionalProperties: additional.Properties{Distance: true},
		})
		require.Nil(t, err)
		require.NotEmpty(t, results)
		// ensure that we receive more results than
		// the `QueryMaximumResults`, as this should
		// only apply to limited vector searches
		require.Greater(t, len(results), 1)

		for _, res := range results {
			if props, ok := tests[res.ID]; !ok {
				t.Fatalf("received unexpected result: %+v", res)
			} else {
				assert.True(t, props.expected, "result id was not intended to meet threshold %s", res.ID)
			}
		}
	})
}

func TestVectorSearch_ByCertainty(t *testing.T) {
	className := "SomeClass"
	var class *models.Class

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter: 60,
		RootPath:                dirName,
		// this is set really low to ensure that search
		// by distance is conducted, which executes
		// without regard to this value
		QueryMaximumResults:       1,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)

	t.Run("create required schema", func(t *testing.T) {
		class = &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					DataType: []string{string(schema.DataTypeInt)},
					Name:     "int_prop",
				},
			},
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	searchVector := []float32{-0.10190568, -0.06259751, 0.05616188, -0.19249836, 0.09714927, -0.1902525, -0.064424865, -0.0387358, 0.17581701, 0.4476738, 0.29261824, 0.12026761, -0.19975126, 0.023600178, 0.17348698, 0.12701738, -0.36018127, -0.12051587, -0.17620522, 0.060741074, -0.064512916, 0.18640806, -0.1529852, 0.08211839, -0.02558465, -0.11369845, 0.0924098, -0.10544433, -0.14728987, -0.041860342, -0.08533595, 0.25886244, 0.2963937, 0.26010615, 0.2111097, 0.029396622, 0.01429563, 0.06410264, -0.119665794, 0.33583277, -0.05802661, 0.023306102, 0.14435922, -0.003951336, -0.13870825, 0.07140894, 0.10469943, -0.059021875, -0.065911904, 0.024216041, -0.26282874, 0.04896568, -0.08291928, -0.12793182, -0.077824734, 0.08843151, 0.31247458, -0.066301286, 0.006904921, -0.08277095, 0.13936226, -0.64392364, -0.19566211, 0.047227614, 0.086121306, -0.20725192, -0.096485816, -0.16436341, -0.06559169, -0.019639932, -0.012729637, 0.08901619, 0.0015896161, -0.24789932, 0.35496348, -0.16272856, -0.01648429, 0.11247674, 0.08099968, 0.13339259, 0.055829972, -0.34662855, 0.068509, 0.13880715, 0.3201848, -0.055557363, 0.22142135, -0.12867308, 0.0037871755, 0.24888979, -0.007443307, 0.08906625, -0.02022331, 0.11510742, -0.2385861, 0.16177008, -0.16214795, -0.28715602, 0.016784908, 0.19386634, -0.07731616, -0.100485384, 0.4100712, 0.061834496, -0.2325293, -0.026056025, -0.11632323, -0.17040555, -0.081960455, -0.0061040106, -0.05949373, 0.044952348, -0.079565264, 0.024430245, -0.09375341, -0.30249637, 0.115205586, -0.13083287, -0.04264671, -0.089810364, 0.16227561, 0.07318055, -0.10496504, 0.00063501706, -0.04936106, -0.0022282854, 1.0893154, 0.1698662, -0.019563455, -0.011128426, 0.04477475, -0.15656771, -0.056911886, -0.5759019, -0.1881429, 0.17088258, 0.24124439, 0.111288875, -0.0015475494, -0.021278847, -0.08362156, 0.09997524, -0.094385885, -0.1674031, 0.061180864, 0.28517494, -0.016217072, 0.025866214, -0.22854298, -0.17924422, -0.037767246, 0.12252907, -0.31698978, -0.038031228, 0.055408552, 0.1743545, -0.040576655, 0.1293942, -0.56650764, -0.10306195, -0.19548112, -0.245544, -0.018241389, -0.039024632, -0.31659162, 0.1565075, 0.08412337, 0.13177724, -0.13766576, -0.15355161, -0.16960397, -0.012436442, 0.04828157, 0.12566057, -0.35308784, -0.37520224, -0.1265899, -0.13991497, 0.14402144, 0.117542416, -0.20750546, -0.5849919, -0.010469457, -0.19677396, 0.011365964, 0.00666846, -0.083470255, 0.24928358, 0.07026387, 0.19082819, 0.24557637, 0.014292963, 0.14846677, 0.031625308, -0.20398879, 0.19507346, -0.18119761, -0.045725327, -0.042455163, -0.099733196, -0.33636123, -0.28447086, 0.30274838, -0.01603988, -0.0529655, 0.15784146, 0.08746072, -0.1703993, 0.2414512, 0.060322937, -0.00812057, 0.031162385, -0.1764905, 0.22107981, -0.016657066, 0.31948856, 0.07282925, -0.036991462, 0.01266936, -0.009106514, -0.038732465, 0.20973183, 0.033236098, -0.10673938, -0.06880061, 0.115524575, -0.39688373, 0.08749971, -0.21816005, -0.22100002, -0.3716853, -0.14720486, 0.24316181, 0.29673144, 0.020808747, 0.07658521, 0.16310681, 0.38785335, 0.0992224, 0.14177811, 0.025954131, -0.08690783, 0.19653428, 0.09584941, 0.040072605, -0.00038361162, -0.094546966, 0.1910902, 0.13217318, 0.060072783, -0.0655816, 0.2777626, 0.1799169, 0.20187178, -0.0996889, -0.01932122, -0.13133621, 0.057482753, -0.36892185, -0.032093313, 0.14607865, 0.12033318, -0.041683596, -0.2048406, -0.041777443, -0.14975598, -0.2526341, 0.12659752, 0.010567178, -0.297333, -0.27522174, 0.06923473, 0.043150593, -0.017045585, -0.2400216, 0.11413547, -0.40081662, -0.0018820907, 0.13800722, 0.085972115, -0.01519989, -0.10491216, 0.09170084, 0.063085504, 0.046743374, -0.014466267, 0.09880224, 0.027706565, 0.09951337, 0.17317492, -0.025654864, 0.14658073, 0.042377427, -0.08402882, -0.12423425, 0.32714987, -0.1527207, 0.106094465, 0.017378228, -0.06302387}
	searchObject := strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3")

	tests := map[strfmt.UUID]struct {
		inputVec []float32
		expected bool
	}{
		strfmt.UUID("88460290-03b2-44a3-9adb-9fa3ae11d9e6"): {
			inputVec: []float32{-0.11015724, -0.05380307, 0.027512914, -0.16925375, 0.08306809, -0.19312492, -0.08910436, -0.011051652, 0.17981204, 0.40469593, 0.28226805, 0.09381516, -0.18380599, 0.03102771, 0.1645333, 0.1530153, -0.3187937, -0.10800173, -0.18466279, 0.0004336393, -0.0495677, 0.19905856, -0.11614494, 0.08834681, -0.011200292, -0.11969374, 0.12497086, -0.12427251, -0.13395442, -0.0060353535, -0.07504816, 0.23205791, 0.2982508, 0.2517544, 0.176147, -0.036871903, 0.017852835, 0.040007118, -0.118621, 0.3648693, -0.058933854, 0.04004229, 0.11871147, -0.019860389, -0.12701912, 0.106662825, 0.086498804, -0.04303973, -0.0742352, 0.018250324, -0.26544014, 0.029228423, -0.087171465, -0.1282789, -0.06403083, 0.09680911, 0.31433868, -0.081510685, -0.011283603, -0.041624587, 0.16530018, -0.6714878, -0.2436993, 0.03173918, 0.106117725, -0.20803581, -0.10429562, -0.16975354, -0.078582145, -0.0065962705, -0.06840946, 0.094937086, -0.020617036, -0.23795949, 0.34785536, -0.19834635, -0.015064479, 0.11930141, 0.090962164, 0.120560184, 0.054095767, -0.38602966, 0.057141174, 0.12039684, 0.32000408, -0.05146908, 0.20762976, -0.09342379, 0.037577383, 0.23894139, -0.0075003104, 0.104791366, -0.015841056, 0.102840215, -0.20813248, 0.1855997, -0.12594056, -0.27132365, -0.0055563124, 0.21954241, -0.10798524, -0.111896284, 0.44049335, 0.049884494, -0.22339955, -0.005374135, -0.120713554, -0.22275059, -0.09146004, 0.017188415, -0.106493734, 0.045247544, -0.07725446, 0.056848228, -0.10294392, -0.2896642, 0.112891, -0.13773362, -0.089911595, -0.13500965, 0.14051703, 0.040092673, -0.13896292, 0.04580957, -0.014300959, 0.03737215, 1.0661443, 0.19767477, -0.07703914, -0.012910904, -0.0037716173, -0.14437087, -0.06938004, -0.5348036, -0.16047458, 0.19416414, 0.21938956, 0.092242256, -0.012630808, -0.021863988, -0.051702406, 0.08780951, -0.0815602, -0.15332024, 0.077632725, 0.25709584, -0.025725808, 0.042116437, -0.22687604, -0.11791685, -0.028626656, 0.16734225, -0.3017483, -0.03236202, 0.02888077, 0.18193199, -0.009032297, 0.14454253, -0.511494, -0.12119192, -0.20757924, -0.2561716, -0.03904554, -0.07348411, -0.28547177, 0.15967208, 0.079396725, 0.14358875, -0.12829632, -0.18175666, -0.15540425, -0.020419862, 0.019070208, 0.12168836, -0.3428434, -0.357543, -0.11218741, -0.12834033, 0.13564876, 0.12768728, -0.1817461, -0.61235875, -0.029409664, -0.19765733, 0.03872163, 0.0074950717, -0.10025679, 0.2872255, 0.033995092, 0.12945211, 0.21831632, 0.04666009, 0.14233032, 0.016767867, -0.2039244, 0.2000191, -0.13099428, -0.020210614, -0.06286195, -0.0948797, -0.34830436, -0.21595824, 0.32722405, -0.024735296, -0.07859145, 0.16975155, 0.08186461, -0.19249061, 0.23405583, 0.04837592, 0.021467948, -0.022215014, -0.14892808, 0.23908867, -0.050126728, 0.2867907, 0.07740656, -0.01714987, -0.0046314416, -0.0048108613, -0.007407311, 0.1807499, 0.049772616, -0.14680666, -0.07335314, 0.09023705, -0.40600133, 0.05522128, -0.20085222, -0.20410904, -0.34319055, -0.10792269, 0.2297779, 0.30397663, 0.05230268, 0.06408224, 0.13797496, 0.3691112, 0.083033495, 0.13695791, -0.015612457, -0.06413475, 0.18117142, 0.12928344, 0.049171276, 0.016104931, -0.102417335, 0.19589683, 0.14380622, 0.0748065, -0.005402455, 0.27243868, 0.14925551, 0.19564849, -0.10738364, -0.054175537, -0.10068278, 0.06004795, -0.38755924, -0.01654251, 0.1394104, 0.0968949, 0.004271706, -0.17105898, -0.050423585, -0.15311627, -0.24458972, 0.12665795, -0.022814916, -0.23887472, -0.289588, 0.05521137, 0.041259795, -0.021133862, -0.23674431, 0.08424598, -0.37863016, 0.017239956, 0.13776784, 0.060790475, 0.057887543, -0.08784489, 0.08803934, 0.027996546, 0.085972995, -0.014455558, 0.11668073, 0.03812387, 0.088413864, 0.22228678, -0.015599858, 0.11000236, 0.035271563, -0.08088438, -0.13092226, 0.29378533, -0.12311522, 0.09377676, 0.02948718, -0.09136077},
			expected: true,
		},
		strfmt.UUID("c99bc97d-7035-4311-94f3-947dc6471f51"): {
			inputVec: []float32{-0.07545987, -0.046643265, 0.044445477, -0.18531442, 0.07922216, -0.19388637, -0.069393866, -0.036144026, 0.1713317, 0.41803706, 0.23576374, 0.073170714, -0.14085358, 0.012535708, 0.17439325, 0.10057567, -0.33506152, -0.06800867, -0.18882714, 0.002687021, -0.03276807, 0.17267752, -0.13951558, 0.071382746, 0.020254405, -0.10178502, 0.13977699, -0.107296936, -0.113307, -0.002506761, -0.092065684, 0.21008658, 0.31157792, 0.19640765, 0.15769793, -0.0050196033, 0.0022481605, 0.015436289, -0.11822955, 0.31494477, -0.07425527, 0.051401984, 0.11648046, -0.00016831602, -0.12758006, 0.06814693, 0.06108981, -0.025454175, -0.018695071, 0.041827776, -0.23480764, 0.06652185, -0.078328855, -0.121668324, -0.04341973, 0.114403985, 0.32614416, -0.07992741, -0.019665314, -0.017408244, 0.12615794, -0.6350545, -0.17056493, 0.07171332, 0.047071394, -0.18428493, -0.09011123, -0.15995751, -0.03345579, -0.014678, -0.038699757, 0.044125225, 0.0042562615, -0.29445595, 0.30460796, -0.13630153, 0.00014055961, 0.08996278, 0.08948901, 0.12164838, 0.079090506, -0.36153567, 0.02817218, 0.11914518, 0.29805067, -0.07431765, 0.16793592, -0.099549234, 0.045226075, 0.22235383, -0.045654725, 0.09233901, -0.004902314, 0.08621588, -0.19723448, 0.19557382, -0.13199815, -0.22924824, -0.015981175, 0.19762704, -0.08940076, -0.084909916, 0.43372774, 0.026998578, -0.20827708, 0.037450224, -0.078997016, -0.18065391, -0.071308024, 0.00870316, -0.114981964, 0.017085023, -0.07696264, 0.009330409, -0.097458, -0.25530958, 0.118254915, -0.12516825, -0.008301536, -0.20432962, 0.15235707, 0.012840041, -0.18034773, 0.039270073, -0.03131139, 0.013706253, 0.98688674, 0.18840753, -0.055119563, 0.00836046, 0.019445436, -0.10701598, -0.024610046, -0.50088006, -0.15488546, 0.14209819, 0.1798376, 0.08615982, -0.0119235935, -0.0060070297, -0.08406098, 0.10096481, -0.09077014, -0.15957798, 0.10556352, 0.2100476, -0.036947068, 0.05316554, -0.20480183, -0.14873864, -0.0069070593, 0.16027303, -0.288908, -0.04487129, 0.0705415, 0.11973847, -0.0017247469, 0.14092937, -0.5262047, -0.094283305, -0.19120996, -0.2816572, -0.010916339, -0.07984056, -0.28659204, 0.13706332, 0.07364347, 0.12300072, -0.17554194, -0.16378267, -0.15244205, 0.00075927645, 0.017289847, 0.12072629, -0.33452734, -0.33727616, -0.12780978, -0.09350711, 0.105674624, 0.10770573, -0.17278843, -0.5760599, -0.013741414, -0.15395893, 0.009837732, 0.015417911, -0.11384676, 0.24567491, 0.04905973, 0.10762609, 0.2131752, 0.019281652, 0.11665857, 0.022718405, -0.2234067, 0.23241606, -0.12194457, -0.049972955, -0.012225418, -0.14856412, -0.386102, -0.23018965, 0.28920102, -0.023396742, -0.114672944, 0.12130062, 0.05654803, -0.16194181, 0.24095012, 0.03644393, 0.028024165, -0.008832254, -0.16496961, 0.19496499, -0.035887964, 0.25981775, 0.0970074, 0.0013458093, -0.009548204, 0.040741496, -0.019192837, 0.20718361, -0.004034228, -0.1343262, -0.06990001, 0.09888768, -0.35942966, 0.043895893, -0.19182123, -0.17963983, -0.3222771, -0.10223457, 0.23866613, 0.25855777, 0.04051543, 0.08756274, 0.15683484, 0.37856522, 0.04853359, 0.10198129, -0.0061066896, -0.049892712, 0.17087941, 0.14563805, 0.06984385, 0.0071270005, -0.11838641, 0.18716812, 0.14013803, 0.05242403, 0.034357738, 0.3083466, 0.14742611, 0.17841975, -0.124118194, -0.014102871, -0.052544866, 0.037493005, -0.33485797, -0.013164912, 0.1066288, 0.11141791, -0.04029921, -0.16429856, -0.032241724, -0.15965424, -0.2430594, 0.13654563, 0.009401224, -0.2045843, -0.28467956, 0.07325551, 0.027996557, -0.033877768, -0.24350801, 0.08329816, -0.35555813, 0.006908567, 0.07227365, 0.03188268, 0.032559503, -0.09180395, 0.05601515, 0.0047281734, 0.06878795, -0.018943194, 0.08251342, 0.042039152, 0.12902294, 0.20526606, -0.014881293, 0.11723917, 0.0115632, -0.09016013, -0.12117223, 0.31020245, -0.111444525, 0.077845715, 0.00046715315, -0.104099475},
			expected: true,
		},
		strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3"): {
			inputVec: []float32{-0.20739016, -0.19551805, 0.06645163, 0.008650202, 0.03700748, -0.04132599, -0.029881354, 0.04684896, 0.096614264, 0.42888844, 0.10003969, 0.026234219, -0.051639702, -0.118660435, 0.14473079, 0.2911885, -0.1180539, -0.16804434, -0.48081538, 0.021702053, 0.12612472, 0.15442817, -0.05836532, 0.074295096, -0.28077397, -0.24297802, 0.047836643, -0.36753318, -0.30482984, 0.09265357, 0.25571078, 0.41130066, 0.46177864, 0.34033778, 0.20721313, -0.37726295, 0.07721501, 0.08009689, 0.00027321206, 0.5168123, -0.15305339, 0.0937765, 0.096195236, -0.21120761, 0.014014921, 0.3133104, 0.20773117, 0.08483507, -0.27784437, -0.17281856, -0.6050923, -0.22439326, -0.16914369, -0.3149047, -0.13828672, 0.16334395, -0.0018224253, -0.024342008, 0.3511251, 0.04979151, 0.34223744, -0.6965703, -0.36211932, -0.27092442, 0.34418032, -0.09667905, 0.13344757, -0.15622364, -0.24129291, 0.06958589, -0.2681816, -0.09497071, -0.08923615, -0.06642436, 0.48688608, -0.33535984, 0.014242731, 0.079838976, 0.32949054, 0.09051045, -0.2653392, -0.47393548, 0.07508276, 0.0062832804, 0.724184, -0.18929236, 0.11718613, 0.049603477, 0.08766128, 0.31040704, 0.04038693, -0.0017023507, -0.18986607, 0.056264438, -0.20978904, -0.107441366, -0.30505633, -0.45781082, -0.11571784, 0.32160303, -0.1347523, -0.08090298, 0.51651996, -0.023250414, -0.18725531, -0.14222279, 0.009277832, -0.49789724, -0.25156206, 0.0042495225, 0.0038805408, -0.031416763, 0.10277136, 0.14383446, -0.23241928, -0.42357358, 0.027033398, -0.2262604, -0.2685295, -0.14510548, 0.18256307, 0.063297585, 0.027636252, 0.081166506, 0.06726344, 0.1677495, 1.5217289, 0.33152232, -0.2209926, 0.051426213, 0.15640806, -0.30210486, -0.32857975, -0.4170022, -0.028293105, 0.28772062, 0.50510746, 0.09162247, -0.12383193, -0.25066972, -0.1441897, 0.107192926, -0.07404076, 0.0042472635, 0.11014519, 0.22332853, 0.09434378, -0.3278343, 0.041899726, 0.06838457, 0.10983681, 0.11864574, -0.25336757, -0.047530346, -0.027303243, 0.37403497, 0.13420461, 0.14946426, -0.41996637, -0.037703935, -0.47961184, -0.29839846, -0.103934005, -0.12058302, -0.12806267, 0.22814582, 0.3904893, -0.16044962, -0.17479864, -0.33139735, -0.29185295, 0.0653074, 0.042426735, 0.06092335, -0.18776153, -0.52555144, -0.15889317, -0.20644087, 0.2293067, 0.26668283, -0.15607063, -0.696593, -0.08224992, -0.4283747, 0.26883888, -0.031052848, -0.1311875, 0.26636878, 0.16457985, 0.15660451, 0.10629464, 0.17345549, 0.23963387, 0.22997221, -0.111713186, -0.08499592, -0.2274625, 0.19285984, -0.08285016, -0.02692149, -0.3426618, -0.13361897, 0.2870389, -0.12032792, -0.22944619, 0.25588584, 0.24607788, -0.2762531, 0.30983892, 0.011088746, -0.15739818, 0.053215, -0.21660997, 0.033805694, -0.17886437, 0.2979239, 0.2163545, -0.08381542, 0.19666128, -0.28977823, -0.20994817, -0.012160099, 0.057499636, -0.12549455, 0.19303595, -0.14420606, -0.51937664, 0.23400985, -0.27893808, -0.2660984, -0.27870297, -0.32149136, 0.19958079, 0.34468395, 0.18947665, -0.16529581, 0.101419374, 0.30195153, 0.09030288, 0.12496541, 0.02999903, -0.016697621, 0.15314853, 0.27848768, 0.24102053, 0.06933273, 0.08923653, 0.10477832, 0.4389032, 0.15679164, -0.11119637, 0.134823, 0.30230528, 0.20818473, -0.005579584, -0.3474488, -0.44394243, 0.22270252, -0.3668763, 0.07474772, 0.011691334, 0.088187896, 0.23832949, -0.07960201, 0.066471875, 0.034641538, -0.39984587, 0.0032980456, -0.28492525, -0.46358657, -0.2148288, -0.107226945, 0.02734428, -0.24686679, -0.123900555, 0.18174778, -0.31248868, 0.13808723, 0.31549984, 0.21521719, 0.13966985, -0.27272752, 0.12091104, 0.14257833, 0.23175247, 0.15639938, 0.40828535, 0.31916845, 0.023645567, 0.20658277, -0.20365283, 0.113746524, 0.13173752, -0.050343305, -0.31581175, 0.09704622, -0.014172505, 0.16924341, 0.30327854, -0.17770194},
			expected: false,
		},
		strfmt.UUID("e7bf6c45-de72-493a-b273-5ef198974d61"): {
			inputVec: []float32{0.089313604, -0.050221898, 0.18352903, 0.16257699, 0.14520381, 0.17993976, 0.14594483, 0.019256027, -0.15505213, 0.23606326, -0.14456263, 0.2679586, -0.112208664, 0.12997514, 0.0051072896, 0.28151348, -0.10495799, 0.026782967, -0.38603118, 0.16190273, -0.0428943, -0.16265322, -0.17910561, 0.0746288, -0.3117934, -0.15871756, -0.11377734, -0.06822346, -0.13829489, 0.13019162, 0.30741218, 0.16194165, 0.013218932, 0.054517113, 0.12490437, -0.07709048, 0.02556826, -0.21159878, -0.09082174, 0.24629511, 0.05013666, 0.25168124, -0.14423938, -0.0937688, -0.07811525, -0.049346007, 0.3592527, 0.30411252, -0.1168557, 0.18870471, 0.06614835, -0.20099068, -0.084436245, 0.073036775, -0.03448665, -0.11147946, -0.10862863, -0.012393957, 0.18990599, 0.060957544, 0.19518377, -0.027541652, -0.26750082, -0.12780671, 0.09570065, -0.03541132, 0.094820626, -0.13539355, -0.09468136, 0.18476579, -0.20970085, -0.20989786, -0.12084438, -0.04517079, -0.008074663, 0.02824076, 0.114496395, -0.20462593, 0.103516705, -0.101554185, -0.1374868, -0.24884155, -0.08101618, -0.016105993, 0.22608215, -0.007247754, -0.17246912, 0.058247145, -0.041018173, 0.19471274, -0.022576109, 0.032828204, -0.079321206, -0.09259324, 0.041115705, -0.25280195, -0.28517374, -0.19496292, 0.18070905, 0.06384923, -0.004056949, 0.1536253, 0.17861623, -0.033833142, 0.12039968, 0.04458716, 0.08793809, -0.15683243, -0.1087904, 0.1741014, 0.007256374, -0.20265253, 0.034111258, 0.03311363, -0.09449356, -0.13161612, -0.026084669, 0.07609202, 0.03452338, 0.08840356, -0.044566724, 0.1507175, 0.089273594, 0.18872644, 0.18333815, -0.023196407, 0.63831943, 0.20309874, 0.10217627, 0.11445079, 0.18965706, -0.16809432, -0.343172, -0.06439529, 0.08362327, 0.32746288, 0.38483366, 0.020372175, -0.25239283, 0.019468365, -0.016367752, 0.016749177, 0.024621855, 0.030529505, 0.20601188, -0.100692995, -0.16414656, -0.23193358, 0.26616478, 0.06166736, 0.14341855, 0.1294041, 0.045133967, 0.0014262896, -0.0194398, 0.040737696, 0.10099013, -0.10838136, -0.28768313, -0.073719576, -0.15836753, -0.10482511, -0.1349642, -0.107005455, 0.01957546, 0.13799994, 0.056444198, -0.38841644, -0.07585945, -0.018703599, -0.19934878, 0.15176265, 0.04133126, 0.063531734, 0.09720055, -0.29999572, 0.04765686, -0.23604262, 0.081500284, 0.056092553, -0.13664724, -0.37729686, 0.031137427, -0.052083906, 0.117984496, -0.14562207, -0.029609507, 0.13725121, 0.090367764, 0.12787215, 0.11026589, 0.25123242, 0.12911159, 0.055398554, 0.0032232201, 0.026706887, 0.14584258, 0.019900957, -0.12197998, -0.087177716, -0.24649806, -0.17869286, 0.07139921, -0.09633085, -0.16027117, 0.23617831, 0.05429949, -0.061085824, 0.040451035, 0.052443117, -0.14255014, 0.15598148, -0.2336374, 0.08394173, -0.34318882, 0.3419207, 0.18282516, -0.03709172, 0.10525048, -0.1871602, -0.22663523, 0.01635051, 0.16996534, -0.18056048, -0.169894, -0.18467705, -0.3641231, 0.060861763, -0.080082566, -0.08888943, 0.11629789, -0.00973362, 0.07452957, 0.25680214, 0.042024083, -0.024963235, 0.1743134, 0.10921186, 0.25191578, 0.028438354, 0.004781374, -0.08364819, 0.051807538, 0.1165724, 0.29184434, -0.21512283, 0.12515399, -0.08803969, 0.41930157, -0.10181762, 0.038189832, 0.085555896, -0.026453126, 0.04717047, 0.12667313, 0.023158737, -0.45877644, 0.18732828, 0.062374037, -0.21956007, -0.04449947, 0.19028638, 0.1359094, 0.26384917, 0.077602044, 0.35136092, 0.069637895, 0.048263475, -0.02498448, -0.09221205, -0.012142404, -0.124592446, 0.14599627, -0.050875153, -0.25454503, -0.069588415, -0.29793787, -0.13407284, 0.25388947, 0.35565627, -0.034204755, 0.0024766966, 0.086427726, -0.054318108, 0.063218184, -0.037823644, 0.108287826, 0.14440496, 0.025134278, 0.14978257, -0.03355889, 0.02980915, -0.13764386, 0.4167542, -0.03938922, 0.026970355, 0.24595529, 0.111741625, -0.074567944, -0.057232533},
			expected: false,
		},
		strfmt.UUID("0999d109-1d5f-465a-bd8b-e3fbd46f10aa"): {
			inputVec: []float32{-0.10486144, -0.07437922, 0.069469325, -0.1438278, 0.07740161, -0.18606456, -0.09991434, -0.020051572, 0.19863395, 0.4347328, 0.297606, 0.07853262, -0.16025662, 0.023596637, 0.16935731, 0.17052403, -0.29870638, -0.10309007, -0.20055692, 0.0027809117, -0.03928043, 0.21178603, -0.13793766, 0.08118157, 0.006693433, -0.13829204, 0.14778963, -0.13180175, -0.21128704, -0.0026104634, -0.076393716, 0.22200249, 0.32417125, 0.26045212, 0.1783609, -0.114116184, 0.0100981165, 0.07233143, -0.15913877, 0.4238603, -0.036907215, 0.0595873, 0.0807002, -0.07637312, -0.12889846, 0.111177936, 0.091114685, -0.018454906, -0.12132672, 0.056664582, -0.30461523, 0.020763714, -0.10992191, -0.14430659, -0.092879646, 0.13615008, 0.33039626, -0.115675874, 0.03607886, -0.027918883, 0.19531779, -0.7211654, -0.23073879, 0.011791817, 0.1315166, -0.22779183, -0.13773227, -0.1814997, -0.09008116, 0.021698939, -0.102921166, 0.090760864, 0.011856942, -0.25561005, 0.40769714, -0.21286584, -0.018059848, 0.13812906, 0.079457305, 0.12631191, 0.0024881593, -0.4282836, 0.0619608, 0.12207897, 0.39083096, -0.009502015, 0.19990632, -0.06503092, 0.0635979, 0.27579078, -0.020699967, 0.068474516, 0.0043831975, 0.10303624, -0.1885405, 0.22989234, -0.15952443, -0.29842895, 0.006752088, 0.22831629, -0.13150804, -0.13695218, 0.5357904, 0.050116863, -0.24064547, -0.01375713, -0.096647836, -0.24984525, -0.10429946, 0.002098812, -0.08113263, 0.05237009, -0.10246039, 0.05234802, -0.13899775, -0.3439524, 0.12522809, -0.18406768, -0.09022853, -0.19954625, 0.15810682, 0.039185096, -0.13576287, 0.045047805, 0.0035671506, 0.055920787, 1.1730403, 0.24019612, -0.13423051, -0.008052084, -0.00431602, -0.17079304, -0.09064658, -0.58728856, -0.1365065, 0.22919424, 0.22795208, 0.13396585, 0.018962797, -0.0075796233, -0.072394304, 0.10908417, -0.10881145, -0.16565171, 0.10378018, 0.27296618, -0.059810717, 0.03355443, -0.22429268, -0.12499127, -0.0441017, 0.20800696, -0.29992488, -0.003536096, 0.0026575085, 0.2427503, -0.007395092, 0.13233404, -0.5494433, -0.13144702, -0.2899963, -0.27367246, -0.05257514, -0.0939783, -0.267614, 0.16651331, 0.13891254, 0.08047202, -0.14046521, -0.19062972, -0.1433134, 0.0067776316, 0.00207368, 0.12986982, -0.35847133, -0.41852546, -0.15541135, -0.09865207, 0.14805861, 0.17072491, -0.22655731, -0.6473966, -0.007884447, -0.2060257, 0.035390265, 0.02781265, -0.09760371, 0.30535778, 0.047540557, 0.14565119, 0.21733035, 0.06558403, 0.13184759, 0.044231005, -0.22218557, 0.1897204, -0.1596938, 0.017510587, -0.030249557, -0.082377456, -0.39669412, -0.18365891, 0.34806964, -0.024830062, -0.06955674, 0.21521395, 0.1201222, -0.21855503, 0.23522708, 0.038058903, -0.019610198, -0.025448406, -0.18122384, 0.26068974, -0.055872105, 0.29595166, 0.11005987, -0.00841942, 0.006325112, -0.0013332894, -0.025598384, 0.17320716, 0.03480282, -0.1504056, -0.07133905, 0.08367911, -0.41866872, 0.062191408, -0.14972427, -0.18488628, -0.37027854, -0.14803104, 0.23587811, 0.33285886, 0.059688937, 0.030515533, 0.16795416, 0.3813925, 0.0755207, 0.15504116, -0.003507182, -0.08249321, 0.24292688, 0.13771294, 0.08057683, 0.016365156, -0.12878628, 0.1833687, 0.17496476, 0.050333332, 0.008188007, 0.32129762, 0.15476923, 0.2052587, -0.060781036, -0.1502798, -0.10187848, 0.11062117, -0.41137248, 0.016532877, 0.107270226, 0.08759128, 0.011842419, -0.17039144, -0.0139911, -0.13244899, -0.23845059, 0.075682834, -0.052250806, -0.30011725, -0.28581655, -0.00055503653, 0.022204043, -0.08598292, -0.24763824, 0.08245162, -0.39607832, 0.008443992, 0.16124122, 0.08812278, 0.0335653, -0.09692297, 0.07613783, 0.033542078, 0.11447116, -0.0069911424, 0.09004892, 0.09898015, 0.14595516, 0.24977732, -0.0018444546, 0.06290809, 0.013354713, -0.10336537, -0.1028908, 0.31109008, -0.110210516, 0.07165067, 0.050161615, -0.11413514},
			expected: true,
		},
	}

	t.Run("insert test objects", func(t *testing.T) {
		for id, props := range tests {
			err := repo.PutObject(context.Background(), &models.Object{Class: className, ID: id}, props.inputVec, nil)
			require.Nil(t, err)
		}
	})

	t.Run("perform nearVector search by distance", func(t *testing.T) {
		results, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearVector: &searchparams.NearVector{
				Certainty: 0.9,
			},
			SearchVector:         searchVector,
			AdditionalProperties: additional.Properties{Certainty: true},
		})
		require.Nil(t, err)
		require.NotEmpty(t, results)
		// ensure that we receive more results than
		// the `QueryMaximumResults`, as this should
		// only apply to limited vector searches
		require.Greater(t, len(results), 1)

		for _, res := range results {
			if props, ok := tests[res.ID]; !ok {
				t.Fatalf("received unexpected result: %+v", res)
			} else {
				assert.True(t, props.expected, "result id was not intended to meet threshold %s", res.ID)
			}
		}
	})

	t.Run("perform nearObject search by distance", func(t *testing.T) {
		results, err := repo.VectorSearch(context.Background(), dto.GetParams{
			ClassName:  className,
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			NearObject: &searchparams.NearObject{
				Certainty: 0.9,
				ID:        searchObject.String(),
			},
			SearchVector:         searchVector,
			AdditionalProperties: additional.Properties{Certainty: true},
		})
		require.Nil(t, err)
		require.NotEmpty(t, results)
		// ensure that we receive more results than
		// the `QueryMaximumResults`, as this should
		// only apply to limited vector searches
		require.Greater(t, len(results), 1)

		for _, res := range results {
			if props, ok := tests[res.ID]; !ok {
				t.Fatalf("received unexpected result: %+v", res)
			} else {
				assert.True(t, props.expected, "result id was not intended to meet threshold %s", res.ID)
			}
		}
	})
}

func Test_PutPatchRestart(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	testClass := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "PutPatchRestart",
		Properties: []*models.Property{
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       100,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	defer repo.Shutdown(context.Background())
	require.Nil(t, repo.WaitForStartup(ctx))
	migrator := NewMigrator(repo, logger)

	require.Nil(t,
		migrator.AddClass(ctx, testClass, schemaGetter.shardState))

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{testClass},
		},
	}

	testID := strfmt.UUID("93c31577-922e-4184-87a5-5ac6db12f73c")
	testVec := []float32{0.1, 0.2, 0.1, 0.3}

	t.Run("create initial object", func(t *testing.T) {
		err = repo.PutObject(ctx, &models.Object{
			ID:         testID,
			Class:      testClass.Class,
			Properties: map[string]interface{}{"description": "test object init"},
		}, testVec, nil)
		require.Nil(t, err)
	})

	t.Run("repeatedly put with nil vec, patch with vec, and restart", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err = repo.PutObject(ctx, &models.Object{
				ID:    testID,
				Class: testClass.Class,
				Properties: map[string]interface{}{
					"description": fmt.Sprintf("test object, put #%d", i+1),
				},
			}, nil, nil)
			require.Nil(t, err)

			err = repo.Merge(ctx, objects.MergeDocument{
				ID:    testID,
				Class: testClass.Class,
				PrimitiveSchema: map[string]interface{}{
					"description": fmt.Sprintf("test object, patch #%d", i+1),
				},
				Vector:     testVec,
				UpdateTime: time.Now().UnixNano() / int64(time.Millisecond),
			}, nil, "")
			require.Nil(t, err)

			require.Nil(t, repo.Shutdown(ctx))
			require.Nil(t, repo.WaitForStartup(ctx))
		}
	})

	t.Run("assert the final result is correct", func(t *testing.T) {
		findByIDFilter := &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.ClassName(testClass.Class),
					Property: filters.InternalPropID,
				},
				Value: &filters.Value{
					Value: testID.String(),
					Type:  schema.DataTypeText,
				},
			},
		}
		res, err := repo.ObjectSearch(ctx, 0, 10, findByIDFilter,
			nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Len(t, res, 1)

		expectedDescription := "test object, patch #10"
		resultDescription := res[0].Schema.(map[string]interface{})["description"]
		assert.Equal(t, expectedDescription, resultDescription)
	})
}

func TestCRUDWithEmptyArrays(t *testing.T) {
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()

	class := &models.Class{
		Class:               "TestClass",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:     "textArray",
				DataType: schema.DataTypeTextArray.PropString(),
			},
			{
				Name:     "numberArray",
				DataType: []string{string(schema.DataTypeNumberArray)},
			},
			{
				Name:     "boolArray",
				DataType: []string{string(schema.DataTypeBooleanArray)},
			},
		},
	}
	classRefName := "TestRefClass"
	classRef := &models.Class{
		Class:               classRefName,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	classNameWithRefs := "TestClassWithRefs"
	classWithRefs := &models.Class{
		Class:               classNameWithRefs,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "refProp",
				DataType: []string{classRefName},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       100,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)
	require.Nil(t,
		migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	require.Nil(t,
		migrator.AddClass(context.Background(), classRef, schemaGetter.shardState))
	require.Nil(t,
		migrator.AddClass(context.Background(), classWithRefs, schemaGetter.shardState))
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class, classRef, classWithRefs},
		},
	}

	t.Run("empty arrays", func(t *testing.T) {
		objID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
		obj1 := &models.Object{
			ID:    objID,
			Class: "TestClass",
			Properties: map[string]interface{}{
				"textArray":   []string{},
				"numberArray": []float64{},
				"boolArray":   []bool{},
			},
		}
		obj2 := &models.Object{
			ID:    objID,
			Class: "TestClass",
			Properties: map[string]interface{}{
				"textArray":   []string{"value"},
				"numberArray": []float64{0.5},
				"boolArray":   []bool{true},
			},
		}

		assert.Nil(t, repo.PutObject(context.Background(), obj1, []float32{1, 3, 5, 0.4}, nil))
		assert.Nil(t, repo.PutObject(context.Background(), obj2, []float32{1, 3, 5, 0.4}, nil))

		res, err := repo.ObjectByID(context.Background(), objID, nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, obj2.Properties, res.ObjectWithVector(false).Properties)
	})

	t.Run("empty references", func(t *testing.T) {
		objRefID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390000")
		objRef := &models.Object{
			ID:    objRefID,
			Class: classRefName,
			Properties: map[string]interface{}{
				"stringProp": "string prop value",
			},
		}
		assert.Nil(t, repo.PutObject(context.Background(), objRef, []float32{1, 3, 5, 0.4}, nil))

		obj1ID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
		obj1 := &models.Object{
			ID:    obj1ID,
			Class: classNameWithRefs,
			Properties: map[string]interface{}{
				"stringProp": "some prop",
				// due to the fix introduced in https://github.com/weaviate/weaviate/pull/2320,
				// MultipleRef's can appear as empty []interface{} when no actual refs are provided for
				// an object's reference property.
				//
				// when obj1 is unmarshalled from storage, refProp will be represented as []interface{},
				// because it is an empty reference property. so when comparing obj1 with the result of
				// repo.Object, we need this refProp here to be a []interface{}. Note that this is due
				// to our usage of storobj.Object.MarshallerVersion 1, and future MarshallerVersions may
				// not have this ambiguous property type limitation.
				"refProp": []interface{}{},
			},
		}
		obj2ID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a63")
		obj2 := &models.Object{
			ID:    obj2ID,
			Class: classNameWithRefs,
			Properties: map[string]interface{}{
				"stringProp": "some second prop",
				"refProp": models.MultipleRef{
					&models.SingleRef{
						Beacon: strfmt.URI(
							crossref.NewLocalhost(classRefName, objRefID).String()),
					},
				},
			},
		}

		assert.Nil(t, repo.PutObject(context.Background(), obj1, []float32{1, 3, 5, 0.4}, nil))
		assert.Nil(t, repo.PutObject(context.Background(), obj2, []float32{1, 3, 5, 0.4}, nil))

		res, err := repo.Object(context.Background(), classNameWithRefs, obj1ID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, obj1.Properties, res.ObjectWithVector(false).Properties)

		res, err = repo.Object(context.Background(), classNameWithRefs, obj2ID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, obj2.Properties, res.ObjectWithVector(false).Properties)
	})
}

func TestOverwriteObjects(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "SomeClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{},
		&fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)
	t.Run("create the class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	now := time.Now()
	later := now.Add(time.Hour) // time-traveling ;)
	stale := &models.Object{
		ID:                 "981c09f9-67f3-4e6e-a988-c53eaefbd58e",
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties: map[string]interface{}{
			"oldValue": "how things used to be",
		},
		Vector:        []float32{1, 2, 3},
		VectorWeights: (map[string]string)(nil),
		Additional:    models.AdditionalProperties{},
	}

	fresh := &models.Object{
		ID:                 "981c09f9-67f3-4e6e-a988-c53eaefbd58e",
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: later.UnixMilli(),
		Properties: map[string]interface{}{
			"oldValue": "how things used to be",
			"newValue": "how they are now",
		},
		Vector:        []float32{4, 5, 6},
		VectorWeights: (map[string]string)(nil),
		Additional:    models.AdditionalProperties{},
	}

	t.Run("insert stale object", func(t *testing.T) {
		err := repo.PutObject(context.Background(), stale, stale.Vector, nil)
		require.Nil(t, err)
	})

	t.Run("overwrite with fresh object", func(t *testing.T) {
		input := []*objects.VObject{
			{
				LatestObject:    fresh,
				Vector:          []float32{4, 5, 6},
				StaleUpdateTime: stale.LastUpdateTimeUnix,
			},
		}

		idx := repo.GetIndex(schema.ClassName(class.Class))
		shd, err := idx.determineObjectShard(fresh.ID, "")
		require.Nil(t, err)

		received, err := idx.overwriteObjects(context.Background(), shd, input)
		assert.Nil(t, err)
		assert.ElementsMatch(t, nil, received)
	})

	t.Run("assert data was overwritten", func(t *testing.T) {
		found, err := repo.Object(context.Background(), stale.Class,
			stale.ID, nil, additional.Properties{}, nil, "")
		assert.Nil(t, err)
		assert.EqualValues(t, fresh, found.Object())
	})
}

func TestIndexDigestObjects(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "SomeClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{},
		&fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)
	t.Run("create the class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	})
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	now := time.Now()
	later := now.Add(time.Hour) // time-traveling ;)
	obj1 := &models.Object{
		ID:                 "ae48fda2-866a-4c90-94fc-fce40d5f3767",
		Class:              class.Class,
		CreationTimeUnix:   now.UnixMilli(),
		LastUpdateTimeUnix: now.UnixMilli(),
		Properties: map[string]interface{}{
			"oldValue": "how things used to be",
		},
		Vector:        []float32{1, 2, 3},
		VectorWeights: (map[string]string)(nil),
		Additional:    models.AdditionalProperties{},
	}

	obj2 := &models.Object{
		ID:                 "b71ffac8-6534-4368-9718-5410ca89ce16",
		Class:              class.Class,
		CreationTimeUnix:   later.UnixMilli(),
		LastUpdateTimeUnix: later.UnixMilli(),
		Properties: map[string]interface{}{
			"oldValue": "how things used to be",
		},
		Vector:        []float32{1, 2, 3},
		VectorWeights: (map[string]string)(nil),
		Additional:    models.AdditionalProperties{},
	}

	t.Run("insert test objects", func(t *testing.T) {
		err := repo.PutObject(context.Background(), obj1, obj1.Vector, nil)
		require.Nil(t, err)
		err = repo.PutObject(context.Background(), obj2, obj2.Vector, nil)
		require.Nil(t, err)
	})

	t.Run("get digest object", func(t *testing.T) {
		idx := repo.GetIndex(schema.ClassName(class.Class))
		shd, err := idx.determineObjectShard(obj1.ID, "")
		require.Nil(t, err)

		input := []strfmt.UUID{obj1.ID, obj2.ID}

		expected := []replica.RepairResponse{
			{
				ID:         obj1.ID.String(),
				UpdateTime: obj1.LastUpdateTimeUnix,
			},
			{
				ID:         obj2.ID.String(),
				UpdateTime: obj2.LastUpdateTimeUnix,
			},
		}

		res, err := idx.digestObjects(context.Background(), shd, input)
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})
}

func findID(list []search.Result, id strfmt.UUID) (search.Result, bool) {
	for _, item := range list {
		if item.ID == id {
			return item, true
		}
	}

	return search.Result{}, false
}

func ptFloat32(in float32) *float32 {
	return &in
}

func ptFloat64(in float64) *float64 {
	return &in
}

func randomVector(r *rand.Rand, dim int) []float32 {
	out := make([]float32, dim)
	for i := range out {
		out[i] = r.Float32()
	}

	return out
}

func TestIndexDifferentVectorLength(t *testing.T) {
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "SomeClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{},
		&fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger)
	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	obj1ID := strfmt.UUID("ae48fda2-866a-4c90-94fc-fce40d5f3767")
	objNilID := strfmt.UUID("b71ffac9-6534-4368-9718-5410ca89ce16")

	t.Run("Add object with nil vector", func(t *testing.T) {
		objNil := &models.Object{
			ID:     objNilID,
			Class:  class.Class,
			Vector: nil,
		}
		require.Nil(t, repo.PutObject(context.Background(), objNil, objNil.Vector, nil))
		found, err := repo.Object(context.Background(), class.Class, objNil.ID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.Equal(t, found.Vector, []float32{})
		require.Equal(t, objNil.ID, found.ID)
	})

	t.Run("Add object with non-nil vector after nil vector", func(t *testing.T) {
		obj1 := &models.Object{
			ID:     obj1ID,
			Class:  class.Class,
			Vector: []float32{1, 2, 3},
		}
		require.Nil(t, repo.PutObject(context.Background(), obj1, obj1.Vector, nil))
	})

	t.Run("Add object with different vector length", func(t *testing.T) {
		obj2 := &models.Object{
			ID:     "b71ffac8-6534-4368-9718-5410ca89ce16",
			Class:  class.Class,
			Vector: []float32{1, 2, 3, 4},
		}
		require.NotNil(t, repo.PutObject(context.Background(), obj2, obj2.Vector, nil))
		found, err := repo.Object(context.Background(), class.Class, obj2.ID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.Nil(t, found)
	})

	t.Run("Update object with different vector length", func(t *testing.T) {
		err = repo.Merge(context.Background(), objects.MergeDocument{
			ID:              obj1ID,
			Class:           class.Class,
			PrimitiveSchema: map[string]interface{}{},
			Vector:          []float32{1, 2, 3, 4},
			UpdateTime:      time.Now().UnixNano() / int64(time.Millisecond),
		}, nil, "")
		require.NotNil(t, err)
		found, err := repo.Object(context.Background(), class.Class,
			obj1ID, nil, additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.Len(t, found.Vector, 3)
	})

	t.Run("Update nil object with fitting vector", func(t *testing.T) {
		err = repo.Merge(context.Background(), objects.MergeDocument{
			ID:              objNilID,
			Class:           class.Class,
			PrimitiveSchema: map[string]interface{}{},
			Vector:          []float32{1, 2, 3},
			UpdateTime:      time.Now().UnixNano() / int64(time.Millisecond),
		}, nil, "")
		require.Nil(t, err)
		found, err := repo.Object(context.Background(), class.Class, objNilID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.Len(t, found.Vector, 3)
	})

	t.Run("Add nil object after objects with vector", func(t *testing.T) {
		obj2Nil := &models.Object{
			ID:     "b71ffac8-6534-4368-9718-5410ca89ce16",
			Class:  class.Class,
			Vector: nil,
		}
		require.Nil(t, repo.PutObject(context.Background(), obj2Nil, obj2Nil.Vector, nil))
		found, err := repo.Object(context.Background(), class.Class, obj2Nil.ID, nil,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		require.Equal(t, obj2Nil.ID, found.ID)
		require.Equal(t, []float32{}, found.Vector)
	})
}
