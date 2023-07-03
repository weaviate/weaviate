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

package multi_tenancy_tests

import (
	"acceptance_tests_with_client/fixtures"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
)

func TestBatchCreate_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("creates objects of multi-tenancy class", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateTenantsSoup(t, client, tenants...)

		for _, tenant := range tenants {
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(
					&models.Object{
						Class: "Pizza",
						ID:    fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
						Properties: map[string]interface{}{
							"name":        "Quattro Formaggi",
							"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
							"price":       float32(1.1),
							"best_before": "2022-05-03T12:04:40+02:00",
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Pizza",
						ID:    fixtures.PIZZA_FRUTTI_DI_MARE_ID,
						Properties: map[string]interface{}{
							"name":        "Frutti di Mare",
							"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
							"price":       float32(1.2),
							"best_before": "2022-05-05T07:16:30+02:00",
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_CHICKENSOUP_ID,
						Properties: map[string]interface{}{
							"name":        "ChickenSoup",
							"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
							"price":       float32(2.1),
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_BEAUTIFUL_ID,
						Properties: map[string]interface{}{
							"name":        "Beautiful",
							"description": "Putting the game of letter soups to a whole new level.",
							"price":       float32(2.2),
						},
						Tenant: tenant,
					}).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 4)

			ids := make([]string, len(resp))
			for i := range resp {
				require.NotNil(t, resp[i])
				require.NotNil(t, resp[i].Result)
				// TODO should be success?
				// require.NotNil(t, resp[i].Result.Status)
				// assert.Equal(t, "SUCCESS", *resp[i].Result.Status)
				assert.Equal(t, tenant, resp[i].Tenant)

				ids[i] = resp[i].ID.String()
			}
			assert.ElementsMatch(t, ids, []string{
				fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
				fixtures.PIZZA_FRUTTI_DI_MARE_ID,
				fixtures.SOUP_CHICKENSOUP_ID,
				fixtures.SOUP_BEAUTIFUL_ID,
			})
		}

		t.Run("verify created", func(t *testing.T) {
			for _, tenant := range tenants {
				for id, className := range map[string]string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID: "Pizza",
					fixtures.PIZZA_FRUTTI_DI_MARE_ID:   "Pizza",
					fixtures.SOUP_CHICKENSOUP_ID:       "Soup",
					fixtures.SOUP_BEAUTIFUL_ID:         "Soup",
				} {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.True(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of multi-tenancy class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateTenantsSoup(t, client, tenants...)

		resp, err := client.Batch().ObjectsBatcher().
			WithObjects(
				&models.Object{
					Class: "Pizza",
					ID:    fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					Properties: map[string]interface{}{
						"name":        "Quattro Formaggi",
						"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
						"price":       float32(1.1),
						"best_before": "2022-05-03T12:04:40+02:00",
					},
				},
				&models.Object{
					Class: "Pizza",
					ID:    fixtures.PIZZA_FRUTTI_DI_MARE_ID,
					Properties: map[string]interface{}{
						"name":        "Frutti di Mare",
						"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
						"price":       float32(1.2),
						"best_before": "2022-05-05T07:16:30+02:00",
					},
				},
				&models.Object{
					Class: "Soup",
					ID:    fixtures.SOUP_CHICKENSOUP_ID,
					Properties: map[string]interface{}{
						"name":        "ChickenSoup",
						"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
						"price":       float32(2.1),
					},
				},
				&models.Object{
					Class: "Soup",
					ID:    fixtures.SOUP_BEAUTIFUL_ID,
					Properties: map[string]interface{}{
						"name":        "Beautiful",
						"description": "Putting the game of letter soups to a whole new level.",
						"price":       float32(2.2),
					},
				}).
			Do(context.Background())

		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 4)

		for i := range resp {
			require.NotNil(t, resp[i])
			require.NotNil(t, resp[i].Result)
			// TODO should be failed?
			// require.NotNil(t, resp[i].Result.Status)
			// assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.NotNil(t, resp[i].Result.Errors.Error)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for id, className := range map[string]string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID: "Pizza",
					fixtures.PIZZA_FRUTTI_DI_MARE_ID:   "Pizza",
					fixtures.SOUP_CHICKENSOUP_ID:       "Soup",
					fixtures.SOUP_BEAUTIFUL_ID:         "Soup",
				} {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of multi-tenancy class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateTenantsSoup(t, client, tenants...)

		resp, err := client.Batch().ObjectsBatcher().
			WithObjects(
				&models.Object{
					Class: "Pizza",
					ID:    fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					Properties: map[string]interface{}{
						"name":        "Quattro Formaggi",
						"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
						"price":       float32(1.1),
						"best_before": "2022-05-03T12:04:40+02:00",
					},
					Tenant: "nonExistentTenant",
				},
				&models.Object{
					Class: "Pizza",
					ID:    fixtures.PIZZA_FRUTTI_DI_MARE_ID,
					Properties: map[string]interface{}{
						"name":        "Frutti di Mare",
						"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
						"price":       float32(1.2),
						"best_before": "2022-05-05T07:16:30+02:00",
					},
					Tenant: "nonExistentTenant",
				},
				&models.Object{
					Class: "Soup",
					ID:    fixtures.SOUP_CHICKENSOUP_ID,
					Properties: map[string]interface{}{
						"name":        "ChickenSoup",
						"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
						"price":       float32(2.1),
					},
					Tenant: "nonExistentTenant",
				},
				&models.Object{
					Class: "Soup",
					ID:    fixtures.SOUP_BEAUTIFUL_ID,
					Properties: map[string]interface{}{
						"name":        "Beautiful",
						"description": "Putting the game of letter soups to a whole new level.",
						"price":       float32(2.2),
					},
					Tenant: "nonExistentTenant",
				}).
			Do(context.Background())

		require.Nil(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp, 4)
		for i := range resp {
			require.NotNil(t, resp[i])
			require.NotNil(t, resp[i].Result)
			// TODO should be failed?
			// require.NotNil(t, resp[i].Result.Status)
			// assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.NotNil(t, resp[i].Result.Errors.Error)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "no tenant found with key")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for id, className := range map[string]string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID: "Pizza",
					fixtures.PIZZA_FRUTTI_DI_MARE_ID:   "Pizza",
					fixtures.SOUP_CHICKENSOUP_ID:       "Soup",
					fixtures.SOUP_BEAUTIFUL_ID:         "Soup",
				} {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateSchemaSoup(t, client)

		for _, tenant := range tenants {
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(
					&models.Object{
						Class: "Pizza",
						ID:    fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
						Properties: map[string]interface{}{
							"name":        "Quattro Formaggi",
							"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
							"price":       float32(1.1),
							"best_before": "2022-05-03T12:04:40+02:00",
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Pizza",
						ID:    fixtures.PIZZA_FRUTTI_DI_MARE_ID,
						Properties: map[string]interface{}{
							"name":        "Frutti di Mare",
							"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
							"price":       float32(1.2),
							"best_before": "2022-05-05T07:16:30+02:00",
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_CHICKENSOUP_ID,
						Properties: map[string]interface{}{
							"name":        "ChickenSoup",
							"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
							"price":       float32(2.1),
						},
						Tenant: tenant,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_BEAUTIFUL_ID,
						Properties: map[string]interface{}{
							"name":        "Beautiful",
							"description": "Putting the game of letter soups to a whole new level.",
							"price":       float32(2.2),
						},
						Tenant: tenant,
					}).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 4)
			for i := range resp {
				require.NotNil(t, resp[i])
				require.NotNil(t, resp[i].Result)
				// TODO should be failed?
				// require.NotNil(t, resp[i].Result.Status)
				// assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.NotNil(t, resp[i].Result.Errors.Error)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy disabled, but request was with tenant")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for id, className := range map[string]string{
				fixtures.PIZZA_QUATTRO_FORMAGGI_ID: "Pizza",
				fixtures.PIZZA_FRUTTI_DI_MARE_ID:   "Pizza",
				fixtures.SOUP_CHICKENSOUP_ID:       "Soup",
				fixtures.SOUP_BEAUTIFUL_ID:         "Soup",
			} {
				exists, err := client.Data().Checker().
					WithID(id).
					WithClassName(className).
					Do(context.Background())

				require.Nil(t, err)
				require.False(t, exists)
			}
		})
	})
}

func TestBatchDelete_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("deletes objects from multi-tenancy class", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants...)

		for _, tenant := range tenants {
			for className, ids := range fixtures.IdsByClass {
				resp, err := client.Batch().ObjectsBatchDeleter().
					WithClassName(className).
					WithWhere(filters.Where().
						WithOperator(filters.Like).
						WithPath([]string{"name"}).
						WithValueText("*")).
					WithOutput("minimal").
					WithTenant(tenant).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.Results)
				assert.Equal(t, int64(len(ids)), resp.Results.Matches)
				assert.Equal(t, int64(len(ids)), resp.Results.Successful)
			}
		}

		t.Run("verify deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for className, ids := range fixtures.IdsByClass {
					for _, id := range ids {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant).
							Do(context.Background())

						require.Nil(t, err)
						require.False(t, exists)
					}
				}
			}
		})
	})

	t.Run("fails deleting objects from multi-tenancy class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants...)

		for className := range fixtures.IdsByClass {
			resp, err := client.Batch().ObjectsBatchDeleter().
				WithClassName(className).
				WithWhere(filters.Where().
					WithOperator(filters.Like).
					WithPath([]string{"name"}).
					WithValueText("*")).
				WithOutput("minimal").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			require.Nil(t, resp)
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for className, ids := range fixtures.IdsByClass {
					for _, id := range ids {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant).
							Do(context.Background())

						require.Nil(t, err)
						require.True(t, exists)
					}
				}
			}
		})
	})

	t.Run("fails deleting objects from multi-tenancy class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants...)

		for className := range fixtures.IdsByClass {
			resp, err := client.Batch().ObjectsBatchDeleter().
				WithClassName(className).
				WithWhere(filters.Where().
					WithOperator(filters.Like).
					WithPath([]string{"name"}).
					WithValueText("*")).
				WithOutput("minimal").
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "no tenant found with key")
			require.Nil(t, resp)
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for className, ids := range fixtures.IdsByClass {
					for _, id := range ids {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant).
							Do(context.Background())

						require.Nil(t, err)
						require.True(t, exists)
					}
				}
			}
		})
	})

	t.Run("fails deleting objects from non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaFood(t, client)
		fixtures.CreateDataFood(t, client)

		for className := range fixtures.IdsByClass {
			resp, err := client.Batch().ObjectsBatchDeleter().
				WithClassName(className).
				WithWhere(filters.Where().
					WithOperator(filters.Like).
					WithPath([]string{"name"}).
					WithValueText("*")).
				WithOutput("minimal").
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
			require.Nil(t, resp)
		}

		t.Run("verify non deleted", func(t *testing.T) {
			for className, ids := range fixtures.IdsByClass {
				for _, id := range ids {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(context.Background())

					require.Nil(t, err)
					require.True(t, exists)
				}
			}
		})
	})
}
