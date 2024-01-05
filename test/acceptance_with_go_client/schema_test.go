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

package acceptance_with_go_client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

type testCase struct {
	className1 string
	className2 string
}

func TestSchemaCasingClass(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className1 := "RandomGreenCar"
	className2 := "RANDOMGreenCar"

	cases := []testCase{
		{className1: className1, className2: className1},
		{className1: className2, className2: className2},
		{className1: className1, className2: className2},
		{className1: className2, className2: className1},
	}
	for _, tt := range cases {
		t.Run(tt.className1+" "+tt.className2, func(t *testing.T) {
			c.Schema().ClassDeleter().WithClassName(tt.className1).Do(ctx)
			c.Schema().ClassDeleter().WithClassName(tt.className2).Do(ctx)
			class := &models.Class{Class: tt.className1, Vectorizer: "none"}
			require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

			// try to create class again with permuted-casing duplicate.
			// this should fail as it already exists
			class2 := &models.Class{Class: tt.className2, Vectorizer: "none"}
			err := c.Schema().ClassCreator().WithClass(class2).Do(ctx)
			checkDuplicateClassErrors(t, err, tt)

			// create object with both casing as class name.
			_, err = c.Data().Creator().WithClassName(tt.className1).Do(ctx)
			require.Nil(t, err)
			// this should fail if the 2nd class is a non-equal permutation of the first
			_, err = c.Data().Creator().WithClassName(tt.className2).Do(ctx)
			if tt.className1 != tt.className2 {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
			}

			result, err := c.GraphQL().Aggregate().WithClassName(tt.className1).WithFields(graphql.Field{
				Name: "meta", Fields: []graphql.Field{
					{Name: "count"},
				},
			}).Do(ctx)
			require.Nil(t, err)
			require.Empty(t, result.Errors)
			data := result.Data["Aggregate"].(map[string]interface{})[tt.className1].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
			if tt.className1 == tt.className2 {
				// two objects should have been added if the test case contains exact class name matches
				require.Equal(t, data, 2.)
			} else {
				// otherwise, only one object should have been created, since the permuted class name does not exist
				require.Equal(t, data, 1.)
			}

			// Regardless of whether a class exists or not, the delete operation will always return a success
			require.Nil(t, c.Schema().ClassDeleter().WithClassName(tt.className1).Do(ctx))
			require.Nil(t, c.Schema().ClassDeleter().WithClassName(tt.className2).Do(ctx))
		})
	}
}

func checkDuplicateClassErrors(t *testing.T, err error, tt testCase) {
	require.NotNil(t, err)
	rawError, ok := err.(*fault.WeaviateClientError)
	if ok {
		var clientErr clientError
		require.Nil(t, json.Unmarshal([]byte(rawError.Msg), &clientErr))
		require.Len(t, clientErr.Error, 1)
		if tt.className1 == tt.className2 {
			require.Equal(t, clientErr.Error[0].Message, fmt.Sprintf("class name %q already exists", tt.className2))
		} else {
			require.Equal(t, clientErr.Error[0].Message,
				fmt.Sprintf("class name %q already exists as a permutation of: %q. class names must be unique when lowercased",
					tt.className2, tt.className1))
		}
	} else {
		t.Fatalf("unexpected error: %v", err)
	}
}

type clientError struct {
	Error []struct {
		Message string `json:"message"`
	} `json:"error"`
}
