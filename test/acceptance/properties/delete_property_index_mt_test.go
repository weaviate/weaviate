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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func testDeletePropertyIndexMultiTenant(compose *docker.DockerCompose) func(t *testing.T) {
	return func(t *testing.T) {
		bookClass := "BooksMT"
		tenantName := "tenant1"
		author := "author"
		title := "title"
		title_only_filterable := "title_only_filterable"
		year := "year"

		ptrBool := func(in bool) *bool {
			return &in
		}

		assertFilterByQuery := func(t *testing.T, resultsShouldExist bool, query string) {
			if resultsShouldExist {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				require.NotNil(t, result)

				get := result.Result.(map[string]any)["Get"].(map[string]any)
				cls := get[bookClass].([]any)
				require.Len(t, cls, 1)
			} else {
				errs := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, query)
				assert.True(t, len(errs) > 0)
			}
		}

		filterByTextProperty := func(t *testing.T, propertyName string, resultsShouldExist bool) {
			query := fmt.Sprintf(`
				{
					Get{
						%s(
							tenant: %q,
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
			`, bookClass, tenantName, propertyName)
			assertFilterByQuery(t, resultsShouldExist, query)
		}

		filterByTitle := func(t *testing.T, resultsShouldExist bool) {
			filterByTextProperty(t, title, resultsShouldExist)
		}

		filterByTitleOnlyFilterable := func(t *testing.T, resultsShouldExist bool) {
			filterByTextProperty(t, title_only_filterable, resultsShouldExist)
		}

		filterByYear := func(t *testing.T, resultsShouldExist bool) {
			query := fmt.Sprintf(`
				{
					Get{
						%s(
							tenant: %q,
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
			`, bookClass, tenantName)
			assertFilterByQuery(t, resultsShouldExist, query)
		}

		searchByAuthor := func(t *testing.T, resultsShouldExist bool) {
			query := fmt.Sprintf(`
				{
					Get{
						%s(
							tenant: %q,
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
			`, bookClass, tenantName)
			resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
			require.NoError(t, err)
			require.NotNil(t, resp)

			var cls []any
			book := resp.Data["Get"].(map[string]any)[bookClass]
			if book != nil {
				cls = resp.Data["Get"].(map[string]any)[bookClass].([]any)
			}
			if resultsShouldExist {
				require.Len(t, cls, 1)
			} else {
				require.Len(t, cls, 0)
			}
		}

		// Cleanup any existing class
		deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
		deleteClassResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		helper.AssertRequestOk(t, deleteClassResp, err, nil)

		// Create multi-tenant class with same properties as Book
		book := &models.Class{
			Class: bookClass,
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
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
					IndexSearchable:   ptrBool(false),
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

		// Create tenant
		helper.CreateTenants(t, bookClass, []*models.Tenant{
			{Name: tenantName},
		})

		// Create objects with tenant
		objCreateParams := clobjects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:     strfmt.UUID("00000000-0000-0000-0000-000000000011"),
				Class:  bookClass,
				Tenant: tenantName,
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
				ID:     strfmt.UUID("00000000-0000-0000-0000-000000000012"),
				Class:  bookClass,
				Tenant: tenantName,
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

		t.Run("deactivate tenant", func(t *testing.T) {
			helper.UpdateTenants(t, bookClass, []*models.Tenant{
				{
					Name:           tenantName,
					ActivityStatus: models.TenantActivityStatusCOLD,
				},
			})
		})

		t.Run("delete title_only_filterable filterable index", func(t *testing.T) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).
				WithPropertyName(title_only_filterable).
				WithIndexName("filterable")
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		})

		if compose != nil {
			t.Run("check that title_only_filterable filterable bucket still exists on disk", func(t *testing.T) {
				exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketFromPropNameLSM(title_only_filterable))
				assert.True(t, exists)
			})
		}

		t.Run("delete author searchable index", func(t *testing.T) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).
				WithPropertyName(author).
				WithIndexName("searchable")
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		})

		if compose != nil {
			t.Run("check that author searchable bucket still exists on disk", func(t *testing.T) {
				exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketSearchableFromPropNameLSM(author))
				assert.True(t, exists)
			})
		}

		t.Run("delete year rangeFilters index", func(t *testing.T) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).
				WithPropertyName(year).
				WithIndexName("rangeFilters")
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		})

		if compose != nil {
			t.Run("check that year rangeFilters bucket still exists on disk", func(t *testing.T) {
				exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketRangeableFromPropNameLSM(year))
				assert.True(t, exists)
			})
		}

		t.Run("activate tenant and perform search", func(t *testing.T) {
			helper.UpdateTenants(t, bookClass, []*models.Tenant{
				{
					Name:           tenantName,
					ActivityStatus: models.TenantActivityStatusHOT,
				},
			})

			t.Run("title", func(t *testing.T) {
				filterByTitle(t, true)
			})
			t.Run("title_only_filterable - should not work", func(t *testing.T) {
				filterByTitleOnlyFilterable(t, false)
				if compose != nil {
					t.Run("check that title_only_filterable filterable bucket doesn't exists on disk", func(t *testing.T) {
						exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketFromPropNameLSM(title_only_filterable))
						assert.False(t, exists)
					})
				}
			})
			t.Run("author", func(t *testing.T) {
				searchByAuthor(t, false)
				if compose != nil {
					t.Run("check that author searchable bucket doesn't exists on disk", func(t *testing.T) {
						exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketSearchableFromPropNameLSM(author))
						assert.False(t, exists)
					})
				}
			})
			t.Run("year", func(t *testing.T) {
				filterByYear(t, true)
				if compose != nil {
					t.Run("check that year rangeFilters bucket doesn't exists on disk", func(t *testing.T) {
						exists := checkFolderExistence(t, compose, bookClass, tenantName, helpers.BucketRangeableFromPropNameLSM(year))
						assert.False(t, exists)
					})
				}
			})
		})
	}
}
