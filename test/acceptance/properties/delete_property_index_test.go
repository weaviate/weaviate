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

package properties

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func testDeletePropertyIndex(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		bookClass := "Book"
		author := "author"
		title := "title"
		title_only_filterable := "title_only_filterable"
		year := "year"

		ptrBool := func(in bool) *bool {
			return &in
		}

		deleteIndex := func(t *testing.T, propertyName string, indexSearchable, indexFilterable, indexRangeFilters bool) {
			var indexName string
			if indexSearchable {
				indexName = "searchable"
			}
			if indexFilterable {
				indexName = "filterable"
			}
			if indexRangeFilters {
				indexName = "rangeFilters"
			}

			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).WithPropertyName(propertyName).WithIndexName(indexName)
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		}

		assertFilterByQuery := func(t *testing.T, resultsShouldExist bool, query string) {
			if resultsShouldExist {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				require.NotNil(t, result)

				get := result.Result.(map[string]any)["Get"].(map[string]any)
				cls := get["Book"].([]any)
				require.Len(t, cls, 1)
			} else {
				errs := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
				assert.True(t, len(errs) > 0)
			}
		}

		deleteIndexSearchable := func(t *testing.T, propertyName string) {
			deleteIndex(t, propertyName, true, false, false)
		}
		deleteIndexFilterable := func(t *testing.T, propertyName string) {
			deleteIndex(t, propertyName, false, true, false)
		}
		deleteIndexRangeFilters := func(t *testing.T, propertyName string) {
			deleteIndex(t, propertyName, false, false, true)
		}

		filterByTextProperty := func(t *testing.T, propertyName string, resultsShouldExist bool) {
			query := fmt.Sprintf(`
				{
					Get{
						Book(
							where:{
								valueText: "Dune"
								operator: Equal,
								path: "%s"
							}
						){
							title
							author
							year
						}
					}
				}
			`, propertyName)
			assertFilterByQuery(t, resultsShouldExist, query)
		}

		filterByTitle := func(t *testing.T, resultsShouldExist bool) {
			filterByTextProperty(t, title, resultsShouldExist)
		}

		filterByTitleOnlyFilterable := func(t *testing.T, resultsShouldExist bool) {
			filterByTextProperty(t, title_only_filterable, resultsShouldExist)
		}

		filterByYear := func(t *testing.T, resultsShouldExist bool) {
			query := `
				{
					Get{
						Book(
								where:{
									valueNumber: 1960
									operator: LessThanEqual,
									path: "year"
								}
						){
							title
							author
							year
						}
					}
				}
			`
			assertFilterByQuery(t, resultsShouldExist, query)
		}

		searchByAuthor := func(t *testing.T, resultsShouldExist bool) {
			query := `
				{
					Get{
						Book(
							bm25:{
								query:"herbert"
								properties:"author"
							}
						){
							title
							author
							year
						}
					}
				}
			`
			resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
			require.NoError(t, err)
			require.NotNil(t, resp)

			var cls []any
			book := resp.Data["Get"].(map[string]any)["Book"]
			if book != nil {
				cls = resp.Data["Get"].(map[string]any)["Book"].([]any)
			}
			if resultsShouldExist {
				require.Len(t, cls, 1)
			} else {
				require.Len(t, cls, 0)
			}
		}

		deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
		deleteClassResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		helper.AssertRequestOk(t, deleteClassResp, err, nil)

		book := &models.Class{
			Class: bookClass,
			Properties: []*models.Property{
				{
					Name:            author,
					DataType:        []string{schema.DataTypeText.String()},
					IndexFilterable: ptrBool(true),
					IndexSearchable: ptrBool(true),
				},
				{
					Name:          title,
					DataType:      []string{schema.DataTypeText.String()},
					IndexInverted: ptrBool(true),
				},
				{
					Name:              title_only_filterable,
					DataType:          []string{schema.DataTypeText.String()},
					IndexFilterable:   ptrBool(true),
					IndexSearchable:   ptrBool(false), // also where? or only bm25?
					IndexRangeFilters: ptrBool(false),
				},
				{
					Name:              year,
					DataType:          []string{schema.DataTypeNumber.String()},
					IndexFilterable:   ptrBool(true),
					IndexRangeFilters: ptrBool(true),
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				title: {
					Vectorizer: map[string]any{
						"text2vec-model2vec": map[string]any{
							"vectorizeClassName": false,
							"properties":         []any{title},
						},
					},
					VectorIndexType: "hnsw",
				},
			},
		}
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(book)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

		objCreateParams := clobjects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				Class: bookClass,
				Properties: map[string]any{
					"author":                "Frank Herbert",
					"title":                 "Dune",
					"title_only_filterable": "Dune",
					"year":                  1960,
				},
			})

		objCreateResp, err := helper.Client(t).Objects.ObjectsCreate(objCreateParams, nil)
		helper.AssertRequestOk(t, objCreateResp, err, nil)

		objCreateParams = clobjects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000002"),
				Class: bookClass,
				Properties: map[string]any{
					"author":                "Jaroslaw Grzedowicz",
					"title":                 "The Lord of the Ice Garden",
					"title_only_filterable": "The Lord of the Ice Garden",
					"year":                  2005,
				},
			})

		objCreateResp, err = helper.Client(t).Objects.ObjectsCreate(objCreateParams, nil)
		helper.AssertRequestOk(t, objCreateResp, err, nil)

		t.Run("perform search", func(t *testing.T) {
			filterByTitle(t, true)
			filterByTitleOnlyFilterable(t, true)
			searchByAuthor(t, true)
			filterByYear(t, true)
		})

		t.Run("delete title_only_filterable property index", func(t *testing.T) {
			t.Run("filterable", func(t *testing.T) {
				deleteIndexFilterable(t, title_only_filterable)
			})
		})

		t.Run("delete author property index", func(t *testing.T) {
			t.Run("searchable", func(t *testing.T) {
				deleteIndexSearchable(t, author)
			})
			t.Run("filterable", func(t *testing.T) {
				deleteIndexFilterable(t, author)
			})
		})

		t.Run("delete title property index", func(t *testing.T) {
			t.Run("filterable", func(t *testing.T) {
				deleteIndexFilterable(t, title)
			})
			t.Run("searchable", func(t *testing.T) {
				deleteIndexSearchable(t, title)
			})
		})

		t.Run("delete year property index", func(t *testing.T) {
			t.Run("filterable", func(t *testing.T) {
				deleteIndexFilterable(t, year)
			})
			t.Run("rangeFilters", func(t *testing.T) {
				deleteIndexRangeFilters(t, year)
			})
		})

		t.Run("cannot update non-existent property", func(t *testing.T) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).WithPropertyName("doesntexist").WithIndexName("rangeFilters")
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			require.Error(t, err)
			helper.AssertRequestFail(t, updateOk, err, func() {
				var deleteErr *clschema.SchemaObjectsPropertiesDeleteUnprocessableEntity
				assert.True(t, errors.As(err, &deleteErr))
				require.NotNil(t, deleteErr)
				require.NotNil(t, deleteErr.Payload)
				require.NotEmpty(t, deleteErr.Payload.Error)
				for i := range deleteErr.Payload.Error {
					assert.Contains(t, deleteErr.Payload.Error[i].Message, "property name doesntexist: not found")
				}
			})
		})

		t.Run("perform search - should not work", func(t *testing.T) {
			t.Run("title", func(t *testing.T) {
				filterByTitle(t, false)
			})
			t.Run("title_only_filterable", func(t *testing.T) {
				filterByTitleOnlyFilterable(t, false)
			})
			t.Run("author", func(t *testing.T) {
				searchByAuthor(t, false)
			})
			t.Run("year", func(t *testing.T) {
				filterByYear(t, false)
			})
		})

		if compose != nil {
			t.Run("restart Weaviate", func(t *testing.T) {
				containers := []*docker.DockerContainer{compose.GetWeaviate()}
				for _, container := range containers {
					require.Nil(t, compose.Stop(t.Context(), container.Name(), nil))
				}
				var wg sync.WaitGroup
				for _, container := range containers {
					wg.Go(func() {
						require.Nil(t, compose.Start(t.Context(), container.Name()))
					})
				}
				wg.Wait()

				helper.SetupClient(compose.GetWeaviate().URI())
				defer helper.ResetClient()

				t.Run("perform search - should not work after restart", func(t *testing.T) {
					t.Run("title", func(t *testing.T) {
						filterByTitle(t, false)
					})
					t.Run("title_only_filterable", func(t *testing.T) {
						filterByTitleOnlyFilterable(t, false)
					})
					t.Run("author", func(t *testing.T) {
						searchByAuthor(t, false)
					})
					t.Run("year", func(t *testing.T) {
						filterByYear(t, false)
					})
				})
			})
		}
	}
}
