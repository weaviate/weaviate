//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"acceptance_tests_with_client/internal/wvhost"
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

func TestAutoschemaCasingClass(t *testing.T) {
	ctx := t.Context()
	c := wvhost.NewClient(t)

	upperClassName := "RandomBlueTree"
	lowerClassName := "randomBlueTree"

	cases := []struct {
		className1 string
		className2 string
	}{
		{className1: upperClassName, className2: upperClassName},
		{className1: lowerClassName, className2: lowerClassName},
		{className1: upperClassName, className2: lowerClassName},
		{className1: lowerClassName, className2: upperClassName},
	}
	for _, tt := range cases {
		t.Run(tt.className1+" "+tt.className2, func(t *testing.T) {
			var err error

			_ = c.Collections.Delete(ctx, tt.className1)
			_ = c.Collections.Delete(ctx, tt.className2)

			_, err = c.Collections.Use(tt.className1).Data.Insert(ctx, nil)
			require.NoError(t, err, "insert into %s", tt.className1)

			_, err = c.Collections.Use(tt.className2).Data.Insert(ctx, nil)
			require.NoError(t, err, "insert into %s", tt.className2)

			// Regardless of whether a class exists or not, the delete operation will always return a success
			require.NoError(t, c.Collections.Delete(ctx, upperClassName))
			require.NoError(t, c.Collections.Delete(ctx, lowerClassName))
		})
	}
}

func TestAutoschemaCasingProps(t *testing.T) {
	ctx := t.Context()
	c := wvhost.NewClient(t)

	className := "RandomGreenBike"

	upperPropName := "SomeProp"
	lowerPropName := "someProp"
	cases := []struct {
		prop1 string
		prop2 string
	}{
		{prop1: upperPropName, prop2: upperPropName},
		{prop1: lowerPropName, prop2: lowerPropName},
		{prop1: upperPropName, prop2: lowerPropName},
		{prop1: lowerPropName, prop2: upperPropName},
	}
	for _, tt := range cases {
		t.Run(tt.prop1+" "+tt.prop2, func(t *testing.T) {
			c.Collections.Delete(ctx, className)

			col := c.Collections.Use(className)
			require.NotNil(t, col, "collection handle")

			{
				r, err := col.Data.Insert(ctx, nil)
				require.NoError(t, err, "insert first object")
				require.Empty(t, r.Errors, "insert first object")
			}

			{
				r, err := col.Data.Insert(ctx, &data.Object{
					Properties: map[string]any{tt.prop1: "something"},
				})
				require.NoError(t, err, "insert second object")
				require.Empty(t, r.Errors, "insert second object")
			}

			{
				r, err := col.Data.Insert(ctx, &data.Object{
					Properties: map[string]any{tt.prop2: "other value"},
				})
				require.NoError(t, err, "insert third object")
				require.Empty(t, r.Errors, "insert third object")
			}

			count, err := col.Count(ctx)
			require.NoError(t, err)
			require.EqualValues(t, count, 3)

			require.NoError(t, c.Collections.Delete(ctx, className))
		})
	}
}

func TestAutoschemaCasingUpdateProps(t *testing.T) {
	ctx := context.Background()
	c := wvhost.NewClient(t)

	objID := uuid.MustParse("67b79643-cf8b-4b22-b206-6e63dbb4e57a")
	upperPropName := "SomeProp"
	lowerPropName := "someProp"
	cases := []struct {
		prop1 string
		prop2 string
	}{
		{prop1: upperPropName, prop2: upperPropName},
		{prop1: lowerPropName, prop2: lowerPropName},
		{prop1: upperPropName, prop2: lowerPropName},
		{prop1: lowerPropName, prop2: upperPropName},
	}
	for _, tt := range cases {
		t.Run(tt.prop1+" "+tt.prop2, func(t *testing.T) {
			collectionName := "RandomOliveTree"
			c.Collections.Delete(ctx, collectionName)
			h := c.Collections.Use(collectionName)

			{
				_, err := h.Data.Insert(ctx, nil)
				require.NoError(t, err, "insert first object")
			}

			{
				_, err := h.Data.Insert(ctx, &data.Object{
					UUID:       &objID,
					Properties: map[string]any{tt.prop1: "something"},
				})
				require.NoErrorf(t, err, "insert %s", objID)
			}

			{
				err := h.Data.Update(ctx, data.Object{
					UUID:       &objID,
					Properties: map[string]any{tt.prop2: "other"},
				})
				require.NoErrorf(t, err, "update %s", objID)
			}

			count, err := h.Count(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2, count, "number of objects in collection")
		})
	}
}

func TestAutoschemaPanicOnUnregonizedDataType(t *testing.T) {
	c := wvhost.NewClient(t)
	weather := c.Collections.Use("BeautifulWeather")

	tests := []struct {
		name               string
		properties         map[string]any
		containsErrMessage string
	}{
		{
			name: "unrecognized array property type",
			properties: map[string]any{
				"panicProperty": []any{
					[]any{
						[]any{
							"panic",
						},
					},
				},
			},
			containsErrMessage: "property 'panicProperty' on class 'BeautifulWeather': element [0]: unrecognized data type of value",
		},
		{
			name: "unrecognized nil array property type",
			properties: map[string]any{
				"panicProperty": []any{
					[]any{
						[]any{
							nil,
						},
					},
				},
			},
			containsErrMessage: "property 'panicProperty' on class 'BeautifulWeather': element [0]: unrecognized data type of value",
		},
		{
			name: "array property with nil",
			properties: map[string]any{
				"nilPropertyArray": []any{nil},
			},
			containsErrMessage: "property 'nilPropertyArray' on class 'BeautifulWeather': element [0]: unrecognized data type of value '<nil>'",
		},
		{
			name: "empty string array property",
			properties: map[string]any{
				"emptyPropertyArray": []string{},
			},
		},
		{
			name: "empty interface array property",
			properties: map[string]any{
				"emptyPropertyArray": []any{},
			},
		},
		{
			name: "empty int array property",
			properties: map[string]any{
				"emptyPropertyArray": []int{},
			},
		},
		{
			name: "array property with empty string",
			properties: map[string]any{
				"emptyPropertyArray": []string{""},
			},
		},
		{
			name: "nil property",
			properties: map[string]any{
				"nilProperty": nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := weather.Data.Insert(t.Context(), &data.Object{
				Properties: tt.properties,
			})

			// FIXME(dyma): extract error message
			if tt.containsErrMessage != "" {
				assert.Nil(t, resp)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, tt.containsErrMessage)
			} else {
				assert.NotNil(t, resp)
				assert.Nil(t, err)
			}

			require.NoError(t, c.Collections.Delete(t.Context(), weather.CollectionName()))
		})
	}
}

// NOTE(dyma): this test uses the /batch/objects endpoint, not gRPC batch stream.
// The new client will not support these features. Should we rewrite the test or
// delete it?
func TestAutoschemaPanicOnUnregonizedDataTypeWithBatch(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.Nil(t, err)

	className := "Passage"
	t.Run("should not panic with properties defined as empty array, but just return error", func(t *testing.T) {
		obj := &models.Object{
			Class:      className,
			Properties: []interface{}{},
		}

		resp, err := c.Batch().ObjectsBatcher().WithObjects(obj).Do(ctx)
		require.Nil(t, err)
		require.Len(t, resp, 1)
		require.NotNil(t, resp[0].Result)
		require.NotNil(t, resp[0].Result.Errors)
		require.Len(t, resp[0].Result.Errors.Error, 1)
		assert.Equal(t, "could not recognize object's properties: []", resp[0].Result.Errors.Error[0].Message)

		objs, err := c.Data().ObjectsGetter().WithClassName(className).Do(ctx)
		require.Nil(t, err)
		require.Len(t, objs, 0)

		err = c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		require.Nil(t, err)
	})

	t.Run("should create object in batch without problems", func(t *testing.T) {
		obj := &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"stringProperty": "value",
			},
		}
		resp, err := c.Batch().ObjectsBatcher().WithObjects(obj).Do(ctx)
		require.Nil(t, err)
		require.Len(t, resp, 1)
		require.NotNil(t, resp[0].Result)
		require.Nil(t, resp[0].Result.Errors)
		require.NotNil(t, resp[0].Object)
		// auto-schema creates a "default" named vector with the none vectorizer,
		// so nothing is vectorized even though DEFAULT_VECTORIZER_MODULE is set
		assert.Empty(t, resp[0].Object.Vector)
		assert.Empty(t, resp[0].Object.Vectors)

		class, err := c.Schema().ClassGetter().WithClassName(className).Do(ctx)
		require.Nil(t, err)
		assert.Empty(t, class.Vectorizer)
		require.Len(t, class.VectorConfig, 1)
		defaultVector, ok := class.VectorConfig[modelsext.DefaultNamedVectorName]
		require.True(t, ok)
		assert.Equal(t, map[string]interface{}{"none": map[string]interface{}{}}, defaultVector.Vectorizer)

		objs, err := c.Data().ObjectsGetter().WithClassName(className).Do(ctx)
		require.Nil(t, err)
		require.Len(t, objs, 1)

		err = c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		require.Nil(t, err)
	})
}
