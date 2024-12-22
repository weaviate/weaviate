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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql"
	client "github.com/liutizhong/weaviate-go-client/v4/weaviate"
	"github.com/liutizhong/weaviate-go-client/v4/weaviate/grpc"
	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/schema"
)

func TestSchemaClassNames(t *testing.T) {
	ctx := context.Background()
	config := client.Config{
		Scheme: "http", Host: "localhost:8080",
		GrpcConfig: &grpc.Config{Host: "localhost:50051", Secured: false},
	}
	client, err := client.NewClient(config)
	require.Nil(t, err)

	// clean DB
	err = client.Schema().AllDeleter().Do(ctx)
	require.NoError(t, err)

	tests := []struct {
		className string
	}{
		{
			className: graphql.String.Name(),
		},
		{
			className: graphql.Float.Name(),
		},
		{
			className: graphql.Boolean.Name(),
		},
		{
			className: graphql.Int.Name(),
		},
		{
			className: graphql.DateTime.Name(),
		},
		{
			className: graphql.ID.Name(),
		},
		{
			className: graphql.FieldSet.Name(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.className, func(t *testing.T) {
			id1 := "00000000-0000-0000-0000-000000000001"
			className := tt.className
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "description",
						DataType: []string{schema.DataTypeText.String()},
					},
					{
						Name:     "number",
						DataType: []string{schema.DataTypeInt.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]interface{}{
							"text2vec-contextionary": map[string]interface{}{
								"properties":         []interface{}{"description"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			t.Run("create class", func(t *testing.T) {
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)
			})
			t.Run("batch import object", func(t *testing.T) {
				objects := []*models.Object{
					{
						ID:    strfmt.UUID(id1),
						Class: className,
						Properties: map[string]interface{}{
							"description": "some text property",
							"number":      1,
						},
					},
				}
				resp, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Len(t, resp, 1)
				require.NotNil(t, resp[0].Result)
				assert.Nil(t, resp[0].Result.Errors)
			})
			t.Run("check existence", func(t *testing.T) {
				objs, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id1).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, objs)
				require.Len(t, objs, 1)
				assert.Equal(t, className, objs[0].Class)
				props, ok := objs[0].Properties.(map[string]interface{})
				require.True(t, ok)
				require.Equal(t, 2, len(props))
			})
			t.Run("graphql check", func(t *testing.T) {
				query := fmt.Sprintf("{ Get { %s { description number _additional { id } } } }", className)
				resp, err := client.GraphQL().Raw().WithQuery(query).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)

				classMap, ok := resp.Data["Get"].(map[string]interface{})
				require.True(t, ok)

				classResult, ok := classMap[className].([]interface{})
				require.True(t, ok)
				for i := range classResult {
					resultMap, ok := classResult[i].(map[string]interface{})
					require.True(t, ok)
					description, ok := resultMap["description"].(string)
					require.True(t, ok)
					require.NotEmpty(t, description)
					number, ok := resultMap["number"].(float64)
					require.True(t, ok)
					require.Equal(t, float64(1), number)
					additional, ok := resultMap["_additional"].(map[string]interface{})
					require.True(t, ok)
					id, ok := additional["id"].(string)
					require.True(t, ok)
					require.NotEmpty(t, id)
				}
			})
		})
	}
}
