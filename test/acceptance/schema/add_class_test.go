//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/config"
)

// this test prevents a regression on
// https://github.com/weaviate/weaviate/issues/981
func TestInvalidDataTypeInProperty(t *testing.T) {
	t.Parallel()
	className := t.Name()

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	t.Run("trying to import empty string as data type", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "someProperty",
					DataType: []string{""},
				},
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *clschema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Equal(t, "property 'someProperty': invalid dataType: []: dataType cannot be an empty string",
				parsed.Payload.Error[0].Message)
		})
	})
}

func TestInvalidPropertyName(t *testing.T) {
	t.Parallel()
	className := t.Name()

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	t.Run("trying to create class with invalid property name", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:         "some-property",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *clschema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Equal(t, "'some-property' is not a valid property name. Property names in Weaviate "+
				"are restricted to valid GraphQL names, which must be “/[_A-Za-z][_0-9A-Za-z]{0,230}/”",
				parsed.Payload.Error[0].Message)
		})
	})
}

func TestAddAndRemoveObjectClass(t *testing.T) {
	randomObjectClassName := "YellowCars"

	// Ensure that this name is not in the schema yet.
	t.Log("Asserting that this class does not exist yet")
	assert.NotContains(t, GetObjectClassNames(t), randomObjectClassName)

	tc := &models.Class{
		Class: randomObjectClassName,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	}

	t.Log("Creating class")
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(tc)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("Asserting that this class is now created")
	assert.Contains(t, GetObjectClassNames(t), randomObjectClassName)

	t.Run("pure http - without the auto-generated client", testGetSchemaWithoutClient)

	// Now clean up this class.
	t.Log("Remove the class")
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(randomObjectClassName)
	delResp, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the class does not exist anymore.
	assert.NotContains(t, GetObjectClassNames(t), randomObjectClassName)
}

// This test prevents a regression on
// https://github.com/weaviate/weaviate/issues/1799
//
// This was related to adding ref props. For example in the case of a circular
// dependency (A<>B), users would typically add A without refs, then add B with
// a reference back to A, finally update A with a ref to B.
//
// This last update that would set the ref prop on an existing class was missing
// module-specific defaults. So when comparing to-be-updated to existing we would
// find differences in the properties, thus triggering the above error.
func TestUpdateHNSWSettingsAfterAddingRefProps(t *testing.T) {
	className := "RefUpdateIssueClass"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	defer func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *clschema.SchemaObjectsDeleteBadRequest
			if errors.As(err, &typed) {
				fmt.Println(typed.Payload.Error[0].Message)
			}
		}
	}(t)

	t.Run("initially creating the class", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:         "string_prop",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("adding a ref prop after the fact", func(t *testing.T) {
		params := clschema.NewSchemaObjectsPropertiesAddParams().
			WithClassName(className).
			WithBody(&models.Property{
				DataType: []string{className},
				Name:     "ref_prop",
			})
		_, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(params, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making an innocent change and trying to update it", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.VectorIndexConfig.(map[string]interface{})["ef"] = float64(1234)

		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making a change to IndexNullState (immutable) property and update", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		// IndexNullState cannot be updated during runtime
		class.InvertedIndexConfig.IndexNullState = true
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
	})

	t.Run("obtaining the class, making a change to IndexPropertyLength (immutable) property and update", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		// IndexPropertyLength cannot be updated during runtime
		class.InvertedIndexConfig.IndexPropertyLength = true
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
	})
}

// This test prevents a regression of
// https://github.com/weaviate/weaviate/issues/2692
//
// In this issue, any time a class had no vector index set, any other update to
// the class would be blocked
func TestUpdateClassWithoutVectorIndex(t *testing.T) {
	className := "IAintGotNoVectorIndex"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	defer func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *clschema.SchemaObjectsDeleteBadRequest
			if errors.As(err, &typed) {
				fmt.Println(typed.Payload.Error[0].Message)
			}
		}
	}(t)

	t.Run("initially creating the class", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			InvertedIndexConfig: &models.InvertedIndexConfig{
				Stopwords: &models.StopwordConfig{
					Preset: "en",
				},
				UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
			},
			Properties: []*models.Property{
				{
					Name:     "text_prop",
					DataType: []string{"text"},
				},
			},
			VectorIndexConfig: map[string]interface{}{
				"skip": true,
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making an innocent change and trying to update it", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.InvertedIndexConfig.Stopwords.Preset = "none"

		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)
	})
}

// This test prevents a regression of
// https://github.com/weaviate/weaviate/issues//3177
//
// This test ensures that distance belongs to the immutable properties, i.e. no changes to it are possible after creating the class.
func TestUpdateDistanceSettings(t *testing.T) {
	className := "Cosine_Class"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	defer func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *clschema.SchemaObjectsDeleteBadRequest
			if errors.As(err, &typed) {
				fmt.Println(typed.Payload.Error[0].Message)
			}
		}
	}(t)

	t.Run("initially creating the class", func(t *testing.T) {
		c := &models.Class{
			Class:      className,
			Vectorizer: "none",
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
			VectorIndexConfig: map[string]interface{}{
				"distance": "cosine",
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("Trying to change the distance measurement", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.VectorIndexConfig.(map[string]interface{})["distance"] = "l2-squared"

		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
	})
}

// TODO: https://github.com/weaviate/weaviate/issues/973
// // This test prevents a regression on the fix for this bug:
// // https://github.com/weaviate/weaviate/issues/831
// func TestDeleteSingleProperties(t *testing.T) {
// 	t.Parallel()

// 	randomObjectClassName := "RedShip"

// 	// Ensure that this name is not in the schema yet.
// 	t.Log("Asserting that this class does not exist yet")
// 	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)

// 	tc := &models.Class{
// 		Class: randomThingClassName,
// 		Properties: []*models.Property{
// 			&models.Property{
// DataType:     schema.DataTypeText.PropString(),
// Tokenization: models.PropertyTokenizationWhitespace,
// 				Name:     "name",
// 			},
// 			&models.Property{
// DataType:     schema.DataTypeText.PropString(),
// Tokenization: models.PropertyTokenizationWhitespace,
// 				Name:     "description",
// 			},
// 		},
// 	}

// 	t.Log("Creating class")
// 	params := clschema.NewSchemaThingsCreateParams().WithThingClass(tc)
// 	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
// 	helper.AssertRequestOk(t, resp, err, nil)

// 	t.Log("Asserting that this class is now created")
// 	assert.Contains(t, GetThingClassNames(t), randomThingClassName)

// 	t.Log("adding an instance of this particular class that uses both properties")
// 	instanceParams := things.NewThingsCreateParams().WithBody(
// 		&models.Thing{
// 			Class: randomThingClassName,
// 			Schema: map[string]interface{}{
// 				"name":        "my name",
// 				"description": "my description",
// 			},
// 		})
// 	instanceRes, err := helper.Client(t).Things.ThingsCreate(instanceParams, nil)
// 	assert.Nil(t, err, "adding a class instance should not error")

// 	t.Log("delete a single property of the class")
// 	deleteParams := clschema.NewSchemaThingsPropertiesDeleteParams().
// 		WithClassName(randomThingClassName).
// 		WithPropertyName("description")
// 	_, err = helper.Client(t).Schema.SchemaThingsPropertiesDelete(deleteParams, nil)
// 	assert.Nil(t, err, "deleting the property should not error")

// 	t.Log("retrieve the class and make sure the property is gone")
// 	thing := assertGetThingEventually(t, instanceRes.Payload.ID)
// 	expectedSchema := map[string]interface{}{
// 		"name": "my name",
// 	}
// 	assert.Equal(t, expectedSchema, thing.Schema)

// 	t.Log("verifying that we can still retrieve the thing through graphQL")
// 	result := gql.AssertGraphQL(t, helper.RootAuth, "{  Get { Things { RedShip { name } } } }")
// 	ships := result.Get("Get", "Things", "RedShip").AsSlice()
// 	expectedShip := map[string]interface{}{
// 		"name": "my name",
// 	}
// 	assert.Contains(t, ships, expectedShip)

// 	t.Log("verifying other GQL/REST queries still work")
// 	gql.AssertGraphQL(t, helper.RootAuth, "{  Meta { Things { RedShip { name { count } } } } }")
// 	gql.AssertGraphQL(t, helper.RootAuth, `{  Aggregate { Things { RedShip(groupBy: ["name"]) { name { count } } } } }`)
// 	_, err = helper.Client(t).Things.ThingsList(things.NewThingsListParams(), nil)
// 	assert.Nil(t, err, "listing things should not error")

// 	t.Log("verifying we could re-add the property with the same name")
// 	readdParams := clschema.NewSchemaThingsPropertiesAddParams().
// 		WithClassName(randomThingClassName).
// 		WithBody(&models.Property{
// 			Name:     "description",
// DataType:     schema.DataTypeText.PropString(),
// Tokenization: models.PropertyTokenizationWhitespace,
// 		})

// 	_, err = helper.Client(t).Schema.SchemaThingsPropertiesAdd(readdParams, nil)
// 	assert.Nil(t, err, "adding the previously deleted property again should not error")

// 	// Now clean up this class.
// 	t.Log("Remove the class")
// 	delParams := clschema.NewSchemaThingsDeleteParams().WithClassName(randomThingClassName)
// 	delResp, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
// 	helper.AssertRequestOk(t, delResp, err, nil)

//		// And verify that the class does not exist anymore.
//		assert.NotContains(t, GetThingClassNames(t), randomThingClassName)
//	}
func TestMaximumAllowedCollectionsCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("with limit of 1", func(t *testing.T) {
		// Start a single node with collection limit of 1
		compose, err := docker.New().With3NodeCluster().
			WithWeaviateEnv("MAXIMUM_ALLOWED_COLLECTIONS_COUNT", "1").
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %v", err)
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()

		className := "TestCollection1"
		c1 := &models.Class{
			Class:      className,
			Vectorizer: "none",
		}

		// First class should succeed
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c1)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

		// Second class should fail
		className2 := "TestCollection2"
		c2 := &models.Class{
			Class:      className2,
			Vectorizer: "none",
		}

		params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(c2)
		resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *clschema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Contains(t, parsed.Payload.Error[0].Message, "maximum number of collections")
		})
	})

	t.Run("with default limit (unlimited collections)", func(t *testing.T) {
		// Start a single node with unlimited collections
		compose, err := docker.New().With3NodeCluster().
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %v", err)
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()

		// Create multiple classes (more than default limit)
		classNames := []string{}
		for i := 0; i < 102; i++ {
			className := fmt.Sprintf("TestCollection_%d", i)
			classNames = append(classNames, className)

			c := &models.Class{
				Class:      className,
				Vectorizer: "none",
			}

			params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		}

		// Verify all classes exist
		for _, className := range classNames {
			assert.Contains(t, GetObjectClassNames(t), className)
		}
	})

	t.Run("with limit (100)", func(t *testing.T) {
		// Start a single node with default limit
		compose, err := docker.New().With3NodeCluster().
			WithWeaviateEnv("MAXIMUM_ALLOWED_COLLECTIONS_COUNT", "100").
			Start(ctx)
		require.Nil(t, err)
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %v", err)
			}
		}()

		helper.SetupClient(compose.GetWeaviate().URI())
		defer helper.ResetClient()

		// Create classes up to the limit
		classNames := []string{}
		for i := 0; i < 100; i++ {
			className := fmt.Sprintf("TestCollection_%d", i)
			classNames = append(classNames, className)

			c := &models.Class{
				Class:      className,
				Vectorizer: "none",
			}

			params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
			resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			helper.AssertRequestOk(t, resp, err, nil)
		}

		// Attempt to create one more class (should fail)
		c := &models.Class{
			Class:      "TestCollectionExtra",
			Vectorizer: "none",
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *clschema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Contains(t, parsed.Payload.Error[0].Message, "maximum number of collections")
		})

		// Delete one class and verify we can create a new one
		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(classNames[0])
		delResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		helper.AssertRequestOk(t, delResp, err, nil)

		// Should now be able to create a new class
		params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	})
}
