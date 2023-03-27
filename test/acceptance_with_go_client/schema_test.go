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
	"github.com/weaviate/weaviate/entities/models"

	"github.com/stretchr/testify/require"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

func TestSchemaCasingClass(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})

	upperClassName := "RandomGreenCar"
	lowerClassName := "randomGreenCar"

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
			class := &models.Class{Class: tt.className1, Vectorizer: "none"}
			require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

			// try to create class again with other casing. This should fail as it already exists
			class2 := &models.Class{Class: tt.className2, Vectorizer: "none"}
			require.NotNil(t, c.Schema().ClassCreator().WithClass(class2).Do(ctx))

			// create object with both casing as class name. Both should succeed
			_, err := c.Data().Creator().WithClassName(tt.className1).Do(ctx)
			require.Nil(t, err)
			_, err = c.Data().Creator().WithClassName(tt.className2).Do(ctx)
			require.Nil(t, err)

			// two objects should have been added
			result, err := c.GraphQL().Aggregate().WithClassName(upperClassName).WithFields(graphql.Field{
				Name: "meta", Fields: []graphql.Field{
					{Name: "count"},
				},
			}).Do(ctx)
			require.Nil(t, err)
			require.Equal(t, result.Data["Aggregate"].(map[string]interface{})[upperClassName].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"], 2.)

			// class exists only once in Uppercase, so lowercase delete has to fail
			require.Nil(t, c.Schema().ClassDeleter().WithClassName(upperClassName).Do(ctx))
			require.NotNil(t, c.Schema().ClassDeleter().WithClassName(lowerClassName).Do(ctx))
		})
	}
}
