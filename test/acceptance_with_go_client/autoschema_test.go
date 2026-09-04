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
	"context"
	"testing"

	"acceptance_tests_with_client/internal/wvhost"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
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
			require.Equal(t, count, 3)

			require.NoError(t, c.Collections.Delete(ctx, className))
		})
	}
}

func TestAutoschemaCasingUpdateProps(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.Nil(t, err)

	objId := "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
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
			className := "RandomOliveTree"
			c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			creator := c.Data().Creator()
			_, err := creator.WithClassName(className).Do(ctx)
			require.Nil(t, err)

			creator1 := c.Data().Creator()
			_, err = creator1.WithClassName(className).WithID(objId).WithProperties(map[string]string{tt.prop1: "something"}).Do(ctx)
			require.Nil(t, err)

			updater := c.Data().Updater()
			err = updater.WithClassName(className).WithID(objId).WithProperties(map[string]string{tt.prop2: "other"}).Do(ctx)
			require.Nil(t, err)

			// two objects should have been added (with one update
			result, err := c.GraphQL().Aggregate().WithClassName(className).WithFields(graphql.Field{
				Name: "meta", Fields: []graphql.Field{
					{Name: "count"},
				},
			}).Do(ctx)
			require.Nil(t, err)
			require.Equal(t, result.Data["Aggregate"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"], 2.)

			require.Nil(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))
		})
	}
}

func TestAutoschemaPanicOnUnregonizedDataType(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.Nil(t, err)

	tests := []struct {
		name               string
		properties         map[string]interface{}
		containsErrMessage string
	}{
		{
			name: "unrecognized array property type",
			properties: map[string]interface{}{
				"panicProperty": []interface{}{
					[]interface{}{
						[]interface{}{
							"panic",
						},
					},
				},
			},
			containsErrMessage: "property 'panicProperty' on class 'BeautifulWeather': element [0]: unrecognized data type of value",
		},
		{
			name: "unrecognized nil array property type",
			properties: map[string]interface{}{
				"panicProperty": []interface{}{
					[]interface{}{
						[]interface{}{
							nil,
						},
					},
				},
			},
			containsErrMessage: "property 'panicProperty' on class 'BeautifulWeather': element [0]: unrecognized data type of value",
		},
		{
			name: "array property with nil",
			properties: map[string]interface{}{
				"nilPropertyArray": []interface{}{nil},
			},
			containsErrMessage: "property 'nilPropertyArray' on class 'BeautifulWeather': element [0]: unrecognized data type of value '<nil>'",
		},
		{
			name: "empty string array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []string{},
			},
		},
		{
			name: "empty interface array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []interface{}{},
			},
		},
		{
			name: "empty int array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []int{},
			},
		},
		{
			name: "array property with empty string",
			properties: map[string]interface{}{
				"emptyPropertyArray": []string{""},
			},
		},
		{
			name: "nil property",
			properties: map[string]interface{}{
				"nilProperty": nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.Data().
				Creator().
				WithClassName("BeautifulWeather").
				WithProperties(tt.properties).
				Do(ctx)

			if tt.containsErrMessage != "" {
				assert.Nil(t, resp)
				assert.NotNil(t, err)
				assert.ErrorContains(t, err, tt.containsErrMessage)
			} else {
				assert.NotNil(t, resp)
				assert.Nil(t, err)
			}

			err = c.Schema().ClassDeleter().WithClassName("BeautifulWeather").Do(ctx)
			require.Nil(t, err)
		})
	}
}

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
