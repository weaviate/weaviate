//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"context"
	"testing"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

func TestAutoschemaCasingClass(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

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
			c.Schema().ClassDeleter().WithClassName(tt.className1).Do(ctx)
			c.Schema().ClassDeleter().WithClassName(tt.className2).Do(ctx)
			creator := c.Data().Creator()
			_, err := creator.WithClassName(tt.className1).Do(ctx)
			require.Nil(t, err)

			_, err = creator.WithClassName(tt.className2).Do(ctx)
			require.Nil(t, err)

			// class exists only once in Uppercase, so lowercase delete has to fail
			require.Nil(t, c.Schema().ClassDeleter().WithClassName(upperClassName).Do(ctx))
			require.NotNil(t, c.Schema().ClassDeleter().WithClassName(lowerClassName).Do(ctx))
		})
	}
}

func TestAutoschemaCasingProps(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

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
			c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			creator := c.Data().Creator()
			_, err := creator.WithClassName(className).Do(ctx)
			require.Nil(t, err)

			creator1 := c.Data().Creator()
			_, err = creator1.WithClassName(className).WithProperties(map[string]string{tt.prop1: "something"}).Do(ctx)
			require.Nil(t, err)

			creator2 := c.Data().Creator()
			_, err = creator2.WithClassName(className).WithProperties(map[string]string{tt.prop2: "other value"}).Do(ctx)
			require.Nil(t, err)

			// three objects should have been added
			result, err := c.GraphQL().Aggregate().WithClassName(className).WithFields(graphql.Field{
				Name: "meta", Fields: []graphql.Field{
					{Name: "count"},
				},
			}).Do(ctx)
			require.Nil(t, err)
			require.Equal(t, result.Data["Aggregate"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"], 3.)

			require.Nil(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))
		})
	}
}

func TestAutoschemaCasingUpdateProps(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
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
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
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
			containsErrMessage: "invalid text property 'panicProperty' on class 'BeautifulWeather'",
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
			containsErrMessage: "invalid text property 'panicProperty' on class 'BeautifulWeather'",
		},
		{
			name: "array property with nil",
			properties: map[string]interface{}{
				"nilPropertyArray": []interface{}{nil},
			},
			containsErrMessage: "invalid text property 'nilPropertyArray' on class 'BeautifulWeather': not a string",
		},
		{
			name: "empty string array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []string{},
			},
			containsErrMessage: "invalid text property 'emptyPropertyArray' on class 'BeautifulWeather': not a string",
		},
		{
			name: "empty interface array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []interface{}{},
			},
			containsErrMessage: "invalid text property 'emptyPropertyArray' on class 'BeautifulWeather': not a string",
		},
		{
			name: "empty int array property",
			properties: map[string]interface{}{
				"emptyPropertyArray": []int{},
			},
			containsErrMessage: "invalid text property 'emptyPropertyArray' on class 'BeautifulWeather': not a string",
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
