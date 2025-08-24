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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/weaviate/weaviate/test/docker"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// Tests that sort parameters are validated with the correct class
func TestSort(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class: "ClassToSort",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})
	defer deleteObjectClass(t, "ClassToSort")

	createObjectClass(t, &models.Class{
		Class: "OtherClass",
		Properties: []*models.Property{
			{
				Name:     "ref",
				DataType: []string{"ClassToSort"},
			},
		},
	})
	defer deleteObjectClass(t, "OtherClass")

	listParams := objects.NewObjectsListParams()
	nameClass := "ClassToSort"
	nameProp := "name"
	limit := int64(5)
	listParams.Class = &nameClass
	listParams.Sort = &nameProp
	listParams.Limit = &limit

	_, err := helper.Client(t).Objects.ObjectsList(listParams, nil)
	require.Nil(t, err, "should not error")
}

func TestObjects_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "1s").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	testObjects(t)
	asyncTestObjects(t)
}

// Tests for allocChecker nil error on dynamic indexes during shard load
func TestObjects_AsyncIndexing_LoadShard(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("PERSISTENCE_MIN_MMAP_SIZE", "20MB").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	className := "Dynamic"
	createObjectClass(t, &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{"text"},
			},
		},
	})

	tenantName := "tenant0"
	helper.CreateTenants(t, className, []*models.Tenant{{Name: tenantName, ActivityStatus: "ACTIVE"}})

	for i := 0; i < 1000; i++ {
		helper.AssertCreateObjectTenantVector(t, className, map[string]interface{}{
			"description": fmt.Sprintf("Test string %d", i),
		}, tenantName, []float32{0.0, 0.1})
	}
	time.Sleep(3 * time.Second)
	helper.UpdateTenants(t, className, []*models.Tenant{{Name: tenantName, ActivityStatus: "INACTIVE"}})

	time.Sleep(3 * time.Second)
	helper.UpdateTenants(t, className, []*models.Tenant{{Name: tenantName, ActivityStatus: "ACTIVE"}})

	deleteObjectClass(t, className)
}

func TestObjects_SyncIndexing(t *testing.T) {
	testObjects(t)
}

func testObjects(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class: "TestObject",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "testString",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "testWholeNumber",
				DataType: []string{"int"},
			},
			{
				Name:     "testReference",
				DataType: []string{"TestObject"},
			},
			{
				Name:     "testNumber",
				DataType: []string{"number"},
			},
			{
				Name:     "testDateTime",
				DataType: []string{"date"},
			},
			{
				Name:     "testTrueFalse",
				DataType: []string{"boolean"},
			},
			{
				Name:     "testPhoneNumber",
				DataType: []string{"phoneNumber"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestObjectCustomVector",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{"text"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestDeleteClassOne",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestDeleteClassTwo",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
			},
		},
	})

	// tests
	t.Run("listing objects", listingObjects)
	t.Run("searching for neighbors", searchNeighbors)
	t.Run("running a feature projection", featureProjection)
	t.Run("creating objects", creatingObjects)

	t.Run("custom vector journey", customVectors)
	t.Run("auto schema", autoSchemaObjects)
	t.Run("checking object's existence", checkObjects)
	t.Run("delete request deletes all objects with a given ID", deleteAllObjectsFromAllClasses)

	// tear down
	deleteObjectClass(t, "TestObject")
	deleteObjectClass(t, "TestObjectCustomVector")
	deleteObjectClass(t, "NonExistingClass")
	deleteObjectClass(t, "TestDeleteClassOne")
	deleteObjectClass(t, "TestDeleteClassTwo")
}

func asyncTestObjects(t *testing.T) {
	className := "Dynamic"
	createObjectClass(t, &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]interface{}{
			"threshold": 999,
			"hnsw": map[string]interface{}{
				"ef": 123,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{"text"},
			},
		},
	})

	t.Run("update dynamic hnsw ef", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload
		if vectorIndexConfig, ok := class.VectorIndexConfig.(map[string]interface{}); ok {
			if hnsw, ok := vectorIndexConfig["hnsw"].(map[string]interface{}); ok {
				if ef, ok := hnsw["ef"].(json.Number); ok {
					efFloat, err := ef.Float64()
					require.Nil(t, err)
					require.Equal(t, 123.0, efFloat)
					hnsw["ef"] = 1234.0
				} else {
					t.Errorf("type assertion failure 'ef' to json.Number")
				}
			} else {
				t.Errorf("type assertion failure 'hnsw' to map[string]interface{}")
			}
		} else {
			t.Errorf("type assertion failure 'vectorIndexConfig' to map[string]interface{}")
		}

		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)
	})
	deleteObjectClass(t, className)
}

func createObjectClass(t *testing.T, class *models.Class) {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
