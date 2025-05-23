package schema

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/config"
)

// this test prevents a regression on
// https://github.com/weaviate/weaviate/issues/981
func TestInvalidDataTypeInProperty(t *testing.T) {
	t.Parallel()
	className := "TestInvalidDataTypeInProperty"

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

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *schema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Equal(t, "property 'someProperty': invalid dataType: []: dataType cannot be an empty string",
				parsed.Payload.Error[0].Message)
		})
	})
}

func TestInvalidPropertyName(t *testing.T) {
	t.Parallel()
	className := "TestInvalidPropertyName"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	t.Run("trying to create class with invalid property name", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:         "some-property",
					DataType:     entSchema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			var parsed *schema.SchemaObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &parsed), "error should be unprocessable entity")
			assert.Equal(t, "'some-property' is not a valid property name. Property names in Weaviate "+
				"are restricted to valid GraphQL names, which must be “/[_A-Za-z][_0-9A-Za-z]{0,230}/”.",
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
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(tc)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("Asserting that this class is now created")
	assert.Contains(t, GetObjectClassNames(t), randomObjectClassName)

	t.Run("pure http - without the auto-generated client", testGetSchemaWithoutClient)

	// Now clean up this class.
	t.Log("Remove the class")
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(randomObjectClassName)
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
		params := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *schema.SchemaObjectsDeleteBadRequest
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
					DataType:     entSchema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("adding a ref prop after the fact", func(t *testing.T) {
		params := schema.NewSchemaObjectsPropertiesAddParams().
			WithClassName(className).
			WithBody(&models.Property{
				DataType: []string{className},
				Name:     "ref_prop",
			})
		_, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(params, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making an innocent change and trying to update it", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.VectorIndexConfig.(map[string]interface{})["ef"] = float64(1234)

		updateParams := schema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making a change to IndexNullState (immutable) property and update", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		// IndexNullState cannot be updated during runtime
		class.InvertedIndexConfig.IndexNullState = true
		updateParams := schema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
	})

	t.Run("obtaining the class, making a change to IndexPropertyLength (immutable) property and update", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		// IndexPropertyLength cannot be updated during runtime
		class.InvertedIndexConfig.IndexPropertyLength = true
		updateParams := schema.NewSchemaObjectsUpdateParams().
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
		params := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *schema.SchemaObjectsDeleteBadRequest
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

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("obtaining the class, making an innocent change and trying to update it", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.InvertedIndexConfig.Stopwords.Preset = "none"

		updateParams := schema.NewSchemaObjectsUpdateParams().
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
		params := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
		if err != nil {
			var typed *schema.SchemaObjectsDeleteBadRequest
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
					DataType:     entSchema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
			VectorIndexConfig: map[string]interface{}{
				"distance": "cosine",
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	t.Run("Trying to change the distance measurement", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().
			WithClassName(className)
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		class := res.Payload

		class.VectorIndexConfig.(map[string]interface{})["distance"] = "l2-squared"

		updateParams := schema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
	})
}
