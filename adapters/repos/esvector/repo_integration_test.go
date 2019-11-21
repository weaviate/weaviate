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
	"time"

	"github.com/elastic/go-elasticsearch/v5"
	"github.com/elastic/go-elasticsearch/v5/esapi"
	"github.com/go-openapi/strfmt"
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

func TestEsVectorRepo(t *testing.T) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9201"},
	})
	require.Nil(t, err)

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{}
	repo := NewRepo(client, logger, schemaGetter, 3, 100)
	waitForEsToBeReady(t, repo)
	migrator := NewMigrator(repo)

	t.Run("creating the thing class", func(t *testing.T) {
		class := &models.Class{
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
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Thing, class))
	})

	t.Run("creating the action class", func(t *testing.T) {
		class := &models.Class{
			Class: "TheBestActionClass",
			Properties: []*models.Property{
				&models.Property{
					Name:     "stringProp",
					DataType: []string{string(schema.DataTypeString)},
				},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), kind.Action, class))
	})

	thingID := strfmt.UUID("a0b55b05-bc5b-4cc9-b646-1452d1390a62")
	t.Run("adding a thing", func(t *testing.T) {
		thing := &models.Thing{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "TheBestThingClass",
			Schema: map[string]interface{}{
				"stringProp": "some value",
				"location": &models.GeoCoordinates{
					Latitude:  1,
					Longitude: 2,
				},
			},
		}
		vector := []float32{1, 3, 5, 0.4}

		err := repo.PutThing(context.Background(), thing, vector)

		assert.Nil(t, err)
	})

	timeMust := func(t strfmt.DateTime, err error) strfmt.DateTime {
		if err != nil {
			panic(err)
		}

		return t
	}

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
			Meta: &models.ObjectMeta{
				Classification: &models.ObjectMetaClassification{
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

	refreshAll(t, client)

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
		assert.Equal(t, &models.GeoCoordinates{1, 2}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID.String(), schema["uuid"], "has id in schema as uuid field")
		assert.Nil(t, res[0].Meta, "not meta information should be included unless explicitly asked for")
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
		assert.Equal(t, &models.GeoCoordinates{1, 2}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID.String(), schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching all things", func(t *testing.T) {
		// as the test suits grow we might have to extend the limit
		res, err := repo.ThingSearch(context.Background(), 100, nil)
		require.Nil(t, err)

		item, ok := findID(res, thingID)
		require.Equal(t, true, ok, "results should contain our desired thing id")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Thing, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{1, 2}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID.String(), schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching a thing by ID", func(t *testing.T) {
		item, err := repo.ThingByID(context.Background(), thingID, traverser.SelectProperties{}, false)
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, thingID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Thing, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestThingClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.GeoCoordinates{1, 2}, schema["location"], "has correct geo prop")
		assert.Equal(t, thingID.String(), schema["uuid"], "has id in schema as uuid field")
	})

	t.Run("searching an action by ID without meta", func(t *testing.T) {
		item, err := repo.ActionByID(context.Background(), actionID, traverser.SelectProperties{}, false)
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Action, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Nil(t, item.Meta, "not meta information should be included unless explicitly asked for")
	})

	t.Run("searching an action by ID without meta", func(t *testing.T) {
		item, err := repo.ActionByID(context.Background(), actionID, traverser.SelectProperties{}, true)
		require.Nil(t, err)
		require.NotNil(t, item, "must have a result")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Action, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
		assert.Equal(t, &models.ObjectMeta{
			Classification: &models.ObjectMetaClassification{
				ID:               "foo",
				Scope:            []string{"scope1", "scope2"},
				ClassifiedFields: []string{"field1", "field2"},
				Completed:        timeMust(strfmt.ParseDateTime("2006-01-02T15:04:05.000Z")),
			},
		}, item.Meta, "it should include the object meta as it was explicitly specified")
	})

	t.Run("searching all actions", func(t *testing.T) {
		res, err := repo.ActionSearch(context.Background(), 10, nil)
		require.Nil(t, err)

		item, ok := findID(res, actionID)
		require.Equal(t, true, ok, "results should contain our desired action id")

		assert.Equal(t, actionID, item.ID, "extracted the ID")
		assert.Equal(t, kind.Action, item.Kind, "matches the kind")
		assert.Equal(t, "TheBestActionClass", item.ClassName, "matches the class name")
		schema := item.Schema.(map[string]interface{})
		assert.Equal(t, "some act-citing value", schema["stringProp"], "has correct string prop")
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

	refreshAll(t, client)

	t.Run("searching by vector for a single thing class again after deletion", func(t *testing.T) {
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
		assert.Len(t, res, 0)
	})

	t.Run("searching by vector for a single action class again after deletion", func(t *testing.T) {
		searchVector := []float32{2.9, 1.1, 0.5, 8.01}
		params := traverser.GetParams{
			SearchVector: searchVector,
			Kind:         kind.Action,
			ClassName:    "TheBestActionClass",
			Pagination:   &filters.Pagination{Limit: 10},
			Filters:      nil,
		}

		res, err := repo.VectorClassSearch(context.Background(), params)

		require.Nil(t, err)
		assert.Len(t, res, 0)
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

// not the most effecient way, but reduces the wait time in tests
func refreshAll(t *testing.T, c *elasticsearch.Client) {
	req := esapi.IndicesRefreshRequest{
		Index: []string{allClassIndices},
	}

	ctx, cancel := ctx()
	defer cancel()

	res, err := req.Do(ctx, c)
	if err != nil {
		t.Errorf("index refresh request: %v", err)
		return
	}

	log, _ := test.NewNullLogger()
	if err := errorResToErr(res, log); err != nil {
		t.Errorf("index refresh request: %v", err)
		return
	}
}

func waitForEsToBeReady(t *testing.T, repo *Repo) {
	err := repo.WaitForStartup(1 * time.Minute)
	if err != nil {
		t.Errorf("didn't start up: %v", err)
	}
}
