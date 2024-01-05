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
	"testing"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

func TestCorrectErrorForIsNullFilter(t *testing.T) {
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	ctx := context.Background()

	className := "RandomClass45357"
	propName := "title"
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{string(schema.DataTypeText)}, IndexInverted: &vTrue},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: true},
		Vectorizer:          "none",
	}

	// delete class if exists and cleanup after test
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	filter := filters.Where()
	filter.WithOperator(filters.IsNull)
	filter.WithValueString("asd") // wrong type
	filter.WithPath([]string{propName})
	result, err := c.GraphQL().Get().WithClassName(className).WithWhere(filter).WithFields(graphql.Field{Name: propName}).Do(ctx)
	require.Nil(t, err)
	require.Contains(t, result.Errors[0].Message, "booleanValue")
}
