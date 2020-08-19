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

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/multi"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCRUD(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0777)
	defer func() {
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		Class: "TheBestThingClass",
		Properties: []*models.Property{
			&models.Property{
				Name:     "stringProp",
				DataType: []string{string(schema.DataTypeString)},
			},
			&models.Property{
				Name:     "location",
				DataType: []string{string(schema.DataTypeGeoCoordinates)},
			},
			&models.Property{
				Name:     "phone",
				DataType: []string{string(schema.DataTypePhoneNumber)},
			},
		},
	}
	actionclass := &models.Class{
		Class: "TheBestActionClass",
		Properties: []*models.Property{
			&models.Property{
				Name:     "stringProp",
				DataType: []string{string(schema.DataTypeString)},
			},
		},
	}
	schemaGetter := &fakeSchemaGetter{}
	repo := New(logger, Config{RootPath: dirName})
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(30 * time.Second)
	require.Nil(t, err)
	migrator := NewMigrator(repo)

	t.Run("creating the thing class", func(t *testing.T) {

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, thingclass))
	})

	t.Run("creating the action class", func(t *testing.T) {

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Action, actionclass))
	})

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Actions: &models.Schema{
			Classes: []*models.Class{actionclass},
		},
		Things: &models.Schema{
			Classes: []*models.Class{thingclass},
		},
	}

	thingID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")

	t.Run("validating that the thing doesn't exist prior", func(t *testing.T) {
		ok, err := repo.Exists(context.Background(), thingID)
		require.Nil(t, err)
		assert.False(t, ok)
	})

	t.Run("adding a thing", func(t *testing.T) {
		thing := &models.Thing{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Schema: map[string]interface{}{
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

		err := repo.PutThing(context.Background(), thing, vector)

		assert.Nil(t, err)
	})

	t.Run("validating that the thing exists now", func(t *testing.T) {
		ok, err := repo.Exists(context.Background(), thingID)
		require.Nil(t, err)
		assert.True(t, ok)
	})

	timeMust := func(t strfmt.DateTime, err error) strfmt.DateTime {
		if err != nil {
			panic(err)
		}

		return t
	}

	t.Run("updating the thing", func(t *testing.T) {
		thing := &models.Thing{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 10000020,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Schema: map[string]interface{}{
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

		err := repo.PutThing(context.Background(), thing, vector)
		assert.Nil(t, err)
	})

	t.Run("validating the updates are reflected", func(t *testing.T) {
		expected := &models.Thing{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 10000020,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			VectorWeights:      map[string]string(nil),
			Schema: map[string]interface{}{
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

		res, err := repo.ThingByID(context.Background(), thingID, nil,
			traverser.UnderscoreProperties{})
		require.Nil(t, err)

		assert.Equal(t, expected, res.Thing())
	})

	t.Run("finding the updated object by querying for an updated value",
		func(t *testing.T) {
			// This is to verify the inverted index was updated correctly
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Kind:       kind.Thing,
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
				Kind:       kind.Thing,
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
				Kind:       kind.Thing,
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
		thing := &models.Thing{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Schema: map[string]interface{}{
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

		err := repo.PutThing(context.Background(), thing, vector)
		assert.Nil(t, err)
	})

	actionID := strfmt.UUID("022ca5ba-7c0b-4a78-85bf-26346bbcfae7")
	t.Run("adding an action", func(t *testing.T) {
		action := &models.Action{
			CreationTimeUnix:   1000002,
			LastUpdateTimeUnix: 1000003,
			ID:                 actionID,
			Class:              "TheBestActionClass",
			Schema: map[string]interface{}{
				"stringProp": "some act-citing value",
			},
			Meta: &models.UnderscoreProperties{
				Classification: &models.UnderscorePropertiesClassification{
					ID:               "foo",
					Scope:            []string{"scope1", "scope2"},
					ClassifiedFields: []string{"field1", "field2"},
					Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
				},
			},
		}
		vector := []float32{3, 1, 0.3, 12}

		err := repo.PutAction(context.Background(), action, vector)

		assert.Nil(t, err)
	})

	t.Run("searching by vector", func(t *testing.T) {
		// the search vector is designed to be very close to the action, but
		// somewhat far from the thing. So it should match the action closer
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}

		res, err := repo.VectorSearch(context.Background(), searchVector, 10, nil)

		require.Nil(t, err)
		require.Equal(t, true, len(res) >= 2)
		assert.Equal(t, actionID, res[0].ID)
		assert.Equal(t, kind.Action, res[0].Kind)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, "TheBestActionClass", res[0].ClassName)
		assert.Equal(t, int64(1000002), res[0].Created)
		assert.Equal(t, int64(1000003), res[0].Updated)
		assert.Equal(t, thingID, res[1].ID)
		assert.Equal(t, kind.Thing, res[1].Kind)
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
			Kind:         kind.Thing,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.VectorClassSearch(context.Background(), params)

		require.Nil(t, err)
		require.Len(t, res, 1, "got exactly one result")
		assert.Equal(t, thingID, res[0].ID, "extracted the ID")
		assert.Equal(t, kind.Thing, res[0].Kind, "matches the kind")
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
		assert.Nil(t, res[0].UnderscoreProperties, "no meta information should be included unless explicitly asked for")
		assert.Equal(t, thingID, schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching by class type", func(t *testing.T) {
		params := traverser.GetParams{
			SearchVector: nil,
			Kind:         kind.Thing,
			ClassName:    "TheBestThingClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}
		res, err := repo.ClassSearch(context.Background(), params)

		require.Nil(t, err)
		require.Len(t, res, 1, "got exactly one result")
		assert.Equal(t, thingID, res[0].ID, "extracted the ID")
		assert.Equal(t, kind.Thing, res[0].Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", res[0].ClassName, "matches the class name")
		schema := res[0].Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching all things", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		res, err := repo.ThingSearch(context.Background(), 100, nil, traverser.UnderscoreProperties{})
		require.Nil(t, err)

		item, ok := findID(res, thingID)
		require.Equal(t, true, ok, "results should contain our desired thing id")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Thing, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching a thing by ID", func(t *testing.T) {
		item, err := repo.ThingByID(context.Background(), thingID, traverser.SelectProperties{}, traverser.UnderscoreProperties{})
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Thing, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("listing multiple things by IDs (MultiGet)", func(t *testing.T) {
		query := []multi.Identifier{
			multi.Identifier{
				ID:        "be685717-e61e-450d-8d5c-f44f32d0336c", // this id does not exist
				ClassName: "TheBestThingClass",
				Kind:      kind.Thing,
			},
			multi.Identifier{
				ID:        thingID.String(),
				ClassName: "TheBestThingClass",
				Kind:      kind.Thing,
			},
		}
		res, err := repo.MultiGet(context.Background(), query)
		require.Nil(t, err)
		require.Len(t, res, 2, "length must match even with nil-items")

		assert.Equal(t, strfmt.UUID(""), res[0].ID, "empty object for the not-found item")

		item := res[1]
		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Thing, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{ptFloat32(1), ptFloat32(2)}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID, schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching an action by ID without meta", func(t *testing.T) {
		item, err := repo.ActionByID(context.Background(), actionID, traverser.SelectProperties{}, traverser.UnderscoreProperties{})
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Action, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		// TODO gh-1150 support meta
		// assert.Nil(t, item.Meta, "not meta information should be included unless explicitly asked for")
	})

	// TODO gh-1150 support underscore props
	// t.Run("searching an action by ID with meta==true", func(t *testing.T) {
	// 	item, err := repo.ActionByID(context.Background(), actionID, traverser.SelectProperties{}, true)
	// 	require.Nil(t, err)
	// 	require.NotNil(t, item, "must have a result")

	// 	assert.Equal(t, actionID, item.ID, "extracted the ID")
	// 	assert.Equal(t, kind.Action, item.Kind, "matches the kind")
	// 	assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
	// 	schema := item.Schema.(map[string]interface{})
	// 	assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
	// 	assert.Equal(t, &models.ObjectMeta{
	// 		Classification: &models.ObjectMetaClassification{
	// 			ID:               "foo",
	// 			Scope:            []string{"scope1", "scope2"},
	// 			ClassifiedFields: []string{"field1", "field2"},
	// 			Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
	// 		},
	// 		Vector: []float32{3, 1, 0.3, 12},
	// 	}, item.Meta, "it should include the object meta as it was explicitly specified")
	// })

	t.Run("searching all actions", func(t *testing.T) {
		res, err := repo.ActionSearch(context.Background(), 10, nil, traverser.UnderscoreProperties{})
		require.Nil(t, err)

		item, ok := findID(res, actionID)
		require.Equal(t, true, ok, "results should contain our desired action id")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Action, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
	})

	t.Run("verifying the thing is indexed in the inverted index", func(t *testing.T) {
		// This is a control for the upcoming deletion, after the deletion it should not
		// be indexed anymore.
		res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			Kind:       kind.Thing,
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
			Kind:       kind.Action,
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
		err := repo.DeleteThing(context.Background(),
			"TheBestThingClass", thingID)

		assert.Nil(t, err)
	})

	t.Run("deleting a action again", func(t *testing.T) {
		err := repo.DeleteAction(context.Background(),
			"TheBestActionClass", actionID)

		assert.Nil(t, err)
	})

	t.Run("verifying the thing is NOT indexed in the inverted index",
		func(t *testing.T) {
			res, err := repo.ClassSearch(context.Background(), traverser.GetParams{
				Kind:       kind.Thing,
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
				Kind:       kind.Action,
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
		item, err := repo.ThingByID(context.Background(), thingID,
			traverser.SelectProperties{}, traverser.UnderscoreProperties{})
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	t.Run("trying to get the deleted action by ID", func(t *testing.T) {
		item, err := repo.ActionByID(context.Background(), actionID,
			traverser.SelectProperties{}, traverser.UnderscoreProperties{})
		require.Nil(t, err)
		require.Nil(t, item, "must not have a result")
	})

	// t.Run("searching by vector for a single thing class again after deletion", func(t *testing.T) {
	// 	searchVector := []float32{2.9, 1.1, 0.5, 8.01}
	// 	params := traverser.GetParams{
	// 		SearchVector: searchVector,
	// 		Kind:         kind.Thing,
	// 		ClassName:    "TheBestThingClass",
	// 		Pagination:   &filters.Pagination{Limit: 10},
	// 		Filters:      nil,
	// 	}

	// 	res, err := repo.VectorClassSearch(context.Background(), params)

	// 	require.Nil(t, err)
	// 	assert.Len(t, res, 0)
	// })

	// t.Run("searching by vector for a single action class again after deletion", func(t *testing.T) {
	// 	searchVector := []float32{2.9, 1.1, 0.5, 8.01}
	// 	params := traverser.GetParams{
	// 		SearchVector: searchVector,
	// 		Kind:         kind.Action,
	// 		ClassName:    "TheBestActionClass",
	// 		Pagination:   &filters.Pagination{Limit: 10},
	// 		Filters:      nil,
	// 	}

	// 	res, err := repo.VectorClassSearch(context.Background(), params)

	// 	require.Nil(t, err)
	// 	assert.Len(t, res, 0)
	// })
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
