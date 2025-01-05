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

package test_suits

import (
	acceptance_with_go_client "acceptance_tests_with_client"
	"acceptance_tests_with_client/fixtures"
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func testColBERT(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("bring your own multivector", func(t *testing.T) {
			cleanup()

			className := fixtures.BringYourOwnColBERTClassName
			objects := fixtures.BringYourOwnColBERTObjects
			byoc := fixtures.BringYourOwnColBERTNamedVectorName

			_additional := graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
					{Name: fmt.Sprintf("vectors{%s}", byoc)},
				},
			}

			t.Run("create schema", func(t *testing.T) {
				class := fixtures.BringYourOwnColBERTClass(className)
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)
			})

			t.Run("batch create objects", func(t *testing.T) {
				objs := []*models.Object{}
				for _, o := range objects {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(o.ID),
						Properties: map[string]interface{}{
							"name": o.Name,
						},
						Vectors: models.Vectors{
							byoc: o.Vector,
						},
					}
					objs = append(objs, obj)
				}

				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(objs...).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				for _, r := range resp {
					require.Equal(t, "SUCCESS", *r.Result.Status)
				}
			})

			t.Run("check existence", func(t *testing.T) {
				for _, obj := range objects {
					exists, err := client.Data().Checker().
						WithID(obj.ID).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})

			t.Run("get objects with vector", func(t *testing.T) {
				for _, o := range objects {
					objs, err := client.Data().ObjectsGetter().WithID(o.ID).WithClassName(className).WithVector().Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.Len(t, objs[0].Vectors, 1)
					assert.IsType(t, [][]float32{}, objs[0].Vectors[byoc])
					assert.Equal(t, o.Vector, objs[0].Vectors[byoc])
				}
			})

			t.Run("GraphQL get object with vector", func(t *testing.T) {

				for _, o := range objects {
					resp, err := client.GraphQL().Get().
						WithClassName(className).
						WithWhere(filters.Where().WithPath([]string{"id"}).WithOperator(filters.Equal).WithValueText(o.ID)).
						WithFields(_additional).
						Do(ctx)
					require.NoError(t, err)
					vectors := acceptance_with_go_client.GetVectors(t, resp, className, false, byoc)
					require.Len(t, vectors, 1)
					require.IsType(t, [][]float32{}, vectors[byoc])
					assert.Equal(t, o.Vector, vectors[byoc])
				}
			})

			t.Run("update vector", func(t *testing.T) {
				tests := []struct {
					name string
					obj  struct {
						ID     string
						Name   string
						Vector [][]float32
					}
					withMerge bool
					vector    [][]float32
				}{
					{
						name:   "update",
						obj:    objects[0],
						vector: [][]float32{{-0.11111111, -0.12222222}, {-0.13, -0.14}, {-0.15, -0.16}},
					},
					{
						name:      "merge",
						obj:       objects[1],
						withMerge: true,
						vector:    [][]float32{{-0.000001, -0.000001}, {-0.000001, -0.000001}, {-0.000001, -0.000001}},
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						firstObj := tt.obj
						updateVectors := models.Vectors{
							byoc: tt.vector,
						}
						objs, err := client.Data().ObjectsGetter().
							WithClassName(className).WithID(firstObj.ID).WithVector().Do(ctx)
						require.NoError(t, err)
						require.NotEmpty(t, objs)
						require.Len(t, objs[0].Vectors, 1)
						assert.Equal(t, firstObj.Vector, objs[0].Vectors[byoc])
						updater := client.Data().Updater().
							WithClassName(className).WithID(firstObj.ID).WithVectors(updateVectors)
						if tt.withMerge {
							err = updater.WithMerge().Do(ctx)
						} else {
							err = updater.Do(ctx)
						}
						require.NoError(t, err)
						objs, err = client.Data().ObjectsGetter().
							WithClassName(className).WithID(firstObj.ID).WithVector().Do(ctx)
						require.NoError(t, err)
						require.NotEmpty(t, objs)
						require.Len(t, objs[0].Vectors, 1)
						assert.Equal(t, updateVectors[byoc], objs[0].Vectors[byoc])
						resp, err := client.GraphQL().Get().
							WithClassName(className).
							WithWhere(filters.Where().WithPath([]string{"id"}).WithOperator(filters.Equal).WithValueText(firstObj.ID)).
							WithFields(_additional).
							Do(ctx)
						require.NoError(t, err)
						vectors := acceptance_with_go_client.GetVectors(t, resp, className, false, byoc)
						require.Len(t, vectors, 1)
						require.IsType(t, [][]float32{}, vectors[byoc])
						assert.Equal(t, updateVectors[byoc], vectors[byoc])
					})
				}
			})
		})
	}
}
