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
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCRUD(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               "TheBestThingClass",
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     []string{string(schema.DataTypeString)},
				Tokenization: "word",
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
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     []string{string(schema.DataTypeString)},
				Tokenization: "word",
			},
			{
				Name:     "refProp",
				DataType: []string{"TheBestThingClass"},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
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
		ok, err := repo.Exists(context.Background(), thingID)
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

		err := repo.PutObject(context.Background(), thing, vector)

		assert.Nil(t, err)
	})

	t.Run("validating that the thing exists now", func(t *testing.T) {
		ok, err := repo.Exists(context.Background(), thingID)
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

		err := repo.PutObject(context.Background(), thing, vector)
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

		err := repo.PutObject(context.Background(), thing, vector)
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

		res, err := repo.ObjectByID(context.Background(), thingID, nil,
			additional.Properties{})
		require.Nil(t, err)

		assert.Equal(t, expected, res.ObjectWithVector(false))
	})

	t.Run("finding the updated object by querying for an updated value",
		func(t *testing.T) {
			// This is to verify the inverted index was updated correctly
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
							Type:  dtString,
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
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
							Type:  dtString,
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
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
							Type:  dtString,
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

		err := repo.PutObject(context.Background(), thing, vector)
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
							crossref.New("localhost", thingID).String()),
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

		err := repo.PutObject(context.Background(), action, vector)

		assert.Nil(t, err)
	})

	t.Run("searching by vector", func(t *testing.T) {
		// the search vector is designed to be very close to the action, but
		// somewhat far from the thing. So it should match the action closer
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}

		res, err := repo.VectorSearch(context.Background(), searchVector, 0, 10, nil)

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

		params := traverser.GetParams{
			SearchVector: searchVector,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.VectorClassSearch(context.Background(), params)

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
		params := traverser.GetParams{
			SearchVector: nil,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.ClassSearch(context.Background(), params)

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

		err := repo.PutObject(context.Background(), thing, vector)

		assert.Nil(t, err)
	})

	t.Run("searching all things", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, additional.Properties{})
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
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, additional.Properties{Vector: true})
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
		res, err := repo.ObjectSearch(context.Background(), 0, 100, nil, params)
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
		item, err := repo.ObjectByID(context.Background(), thingID, search.SelectProperties{}, additional.Properties{})
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
		res, err := repo.MultiGet(context.Background(), query, additional.Properties{})
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
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{})
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
					crossref.New("localhost", thingID).String()),
			},
		}
		assert.Equal(t, expectedRefProp, schema["refProp"])
	})

	t.Run("searching an action by ID with Classification and Vector additional properties", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{},
			additional.Properties{Classification: true, Vector: true, RefMeta: true})
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
					crossref.New("localhost", thingID).String()),
			},
		}
		assert.Equal(t, expectedRefProp, schema["refProp"])
	})

	t.Run("searching an action by ID with only Vector additional property", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID, search.SelectProperties{}, additional.Properties{Vector: true})
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, []float32{3, 1, 0.3, 12}, item.Vector, "it should include the object meta as it was explicitly specified")
	})

	t.Run("searching all actions", func(t *testing.T) {
		res, err := repo.ObjectSearch(context.Background(), 0, 10, nil, additional.Properties{})
		require.Nil(t, err)

		item, ok := findID(res, actionID)
		require.Equal(t, true, ok, "results should contain our desired action id")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
	})

	t.Run("verifying the thing is indexed in the inverted index", func(t *testing.T) {
		// This is a control for the upcoming deletion, after the deletion it should not
		// be indexed anymore.
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
						Type:  dtString,
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
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
						Type:  dtString,
					},
				},
			},
		})
		require.Nil(t, err)
		require.Len(t, res, 1)
	})

	t.Run("deleting a thing again", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(),
			"TheBestThingClass", thingID)

		assert.Nil(t, err)
	})

	t.Run("deleting a action again", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(),
			"TheBestActionClass", actionID)

		assert.Nil(t, err)
	})

	t.Run("trying to delete from a non-existing class", func(t *testing.T) {
		err := repo.DeleteObject(context.Background(),
			"WrongClass", thingID)

		assert.Equal(t, fmt.Errorf(
			"delete from non-existing index for WrongClass"), err)
	})

	t.Run("verifying the thing is NOT indexed in the inverted index",
		func(t *testing.T) {
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
							Type:  dtString,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 0)
		})

	t.Run("verifying the action is NOT indexed in the inverted index",
		func(t *testing.T) {
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
							Type:  dtString,
						},
					},
				},
			})
			require.Nil(t, err)
			require.Len(t, res, 0)
		})

	t.Run("trying to get the deleted thing by ID", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), thingID,
			search.SelectProperties{}, additional.Properties{})
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	t.Run("trying to get the deleted action by ID", func(t *testing.T) {
		item, err := repo.ObjectByID(context.Background(), actionID,
			search.SelectProperties{}, additional.Properties{})
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	t.Run("searching by vector for a single thing class again after deletion",
		func(t *testing.T) {
			searchVector := []float32{2.9, 1.1, 0.5, 8.01}
			params := traverser.GetParams{
				SearchVector: searchVector,
				ClassName:    "TheBestThingClass",
				Pagination:   &filters.Pagination{Limit: 10},
				Filters:      nil,
			}

			res, err := repo.VectorClassSearch(context.Background(), params)

			require.Nil(t, err)
			assert.Len(t, res, 0)
		})

	t.Run("searching by vector for a single action class again after deletion", func(t *testing.T) {
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}
		params := traverser.GetParams{
			SearchVector: searchVector,
			ClassName:    "TheBestActionClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}

		res, err := repo.VectorClassSearch(context.Background(), params)

		require.Nil(t, err)
		assert.Len(t, res, 0)
	})
}

func Test_ImportWithoutVector_UpdateWithVectorLater(t *testing.T) {
	total := 100
	individual := total / 4
	className := "DeferredVector"
	var data []*models.Object
	var class *models.Class

	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	logger, _ := test.NewNullLogger()
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
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
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
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
			err := repo.PutObject(context.Background(), data[i], nil) // nil vector !
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

		res, err := repo.BatchPutObjects(context.Background(), batch)
		require.Nil(t, err)

		for _, obj := range res {
			require.Nil(t, obj.Err)
		}
	})

	t.Run("verify inverted index works correctly", func(t *testing.T) {
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
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
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
			Filters:   nil,
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(7),
		})
		require.Nil(t, err)
		assert.Len(t, res, 0) // we skipped the vector on half the elements, so we should now match half
	})

	t.Run("update some of the objects to add vectors", func(t *testing.T) {
		for i := range data {
			if i%2 == 1 {
				continue
			}

			data[i].Vector = randomVector(7)
			err := repo.PutObject(context.Background(), data[i], data[i].Vector)
			require.Nil(t, err)
		}
	})

	t.Run("perform unfiltered vector search and verify correct matches", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
			Filters:   nil,
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(7),
		})
		require.Nil(t, err)
		assert.Len(t, res, total/2) // we skipped the vector on half the elements, so we should now match half
	})

	t.Run("perform filtered vector search and verify correct matches", func(t *testing.T) {
		res, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
			Filters:   buildFilter("int_prop", 50, lt, dtInt),
			ClassName: className,
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  total,
			},
			SearchVector: randomVector(7),
		})
		require.Nil(t, err)
		// we skipped the vector on half the elements, and cut the list in half with
		// the filter, so we're only expected a quarter of the total size now
		assert.Len(t, res, total/4)
	})
}

func TestVectorSearch_ByCertainty(t *testing.T) {
	className := "SomeClass"
	var class *models.Class

	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	logger, _ := test.NewNullLogger()
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath: dirName,
		// this is set really low to ensure that search
		// by distance is conducted, which executes
		// without regard to this value
		QueryMaximumResults:       1,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
	}, &fakeRemoteClient{},
		&fakeNodeResolver{})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
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
			VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
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

	searchVector := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	searchObject := strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3")

	tests := map[strfmt.UUID]struct {
		inputVec []float32
		expected bool
	}{
		strfmt.UUID("88460290-03b2-44a3-9adb-9fa3ae11d9e6"): {
			inputVec: []float32{1, 2, 3, 4, 5, 6, 98, 99, 100},
			expected: true,
		},
		strfmt.UUID("c99bc97d-7035-4311-94f3-947dc6471f51"): {
			inputVec: []float32{1, 2, 3, 4, 5, 6, -98, -99, -100},
			expected: false,
		},
		strfmt.UUID("fe687bf4-f10f-4c23-948d-0746ea2927b3"): {
			inputVec: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
			expected: true,
		},
		strfmt.UUID("e7bf6c45-de72-493a-b273-5ef198974d61"): {
			inputVec: []float32{-1, -2, -3, -4, -5, -6, -7, -8, 0},
			expected: false,
		},
		strfmt.UUID("0999d109-1d5f-465a-bd8b-e3fbd46f10aa"): {
			inputVec: []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9},
			expected: true,
		},
	}

	t.Run("insert test objects", func(t *testing.T) {
		for id, props := range tests {
			err := repo.PutObject(context.Background(), &models.Object{Class: className, ID: id}, props.inputVec)
			require.Nil(t, err)
		}
	})

	t.Run("perform nearVector search by distance", func(t *testing.T) {
		results, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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
				assert.False(t, props.expected)
			} else {
				assert.True(t, props.expected)
			}
		}
	})

	t.Run("perform nearObject search by distance", func(t *testing.T) {
		results, err := repo.VectorClassSearch(context.Background(), traverser.GetParams{
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
				assert.False(t, props.expected)
			} else {
				assert.True(t, props.expected)
			}
		}
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

func randomVector(dim int) []float32 {
	out := make([]float32, dim)
	for i := range out {
		out[i] = rand.Float32()
	}

	return out
}
