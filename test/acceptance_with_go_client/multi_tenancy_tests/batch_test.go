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

package multi_tenancy_tests

import (
	"context"
	"testing"

	"acceptance_tests_with_client/fixtures"

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

	t.Run("creates objects of MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

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
						Tenant: tenant.Name,
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
						Tenant: tenant.Name,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_CHICKENSOUP_ID,
						Properties: map[string]interface{}{
							"name":        "ChickenSoup",
							"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
							"price":       float32(2.1),
						},
						Tenant: tenant.Name,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_BEAUTIFUL_ID,
						Properties: map[string]interface{}{
							"name":        "Beautiful",
							"description": "Putting the game of letter soups to a whole new level.",
							"price":       float32(2.2),
						},
						Tenant: tenant.Name,
					}).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 4)

			ids := make([]string, len(resp))
			for i := range resp {
				require.NotNil(t, resp[i])
				require.NotNil(t, resp[i].Result)
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "SUCCESS", *resp[i].Result.Status)
				assert.Equal(t, tenant.Name, resp[i].Tenant)

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
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.True(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of MT class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.NotNil(t, resp[i].Result.Errors.Error)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy enabled, but request was without tenant")
			assert.Empty(t, resp[i].Tenant)
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
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.NotNil(t, resp[i].Result.Errors.Error)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "tenant not found")
			assert.Equal(t, "nonExistentTenant", resp[i].Tenant)
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
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

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
						Tenant: tenant.Name,
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
						Tenant: tenant.Name,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_CHICKENSOUP_ID,
						Properties: map[string]interface{}{
							"name":        "ChickenSoup",
							"description": "Used by humans when their inferior genetics are attacked by microscopic organisms.",
							"price":       float32(2.1),
						},
						Tenant: tenant.Name,
					},
					&models.Object{
						Class: "Soup",
						ID:    fixtures.SOUP_BEAUTIFUL_ID,
						Properties: map[string]interface{}{
							"name":        "Beautiful",
							"description": "Putting the game of letter soups to a whole new level.",
							"price":       float32(2.2),
						},
						Tenant: tenant.Name,
					}).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 4)
			for i := range resp {
				require.NotNil(t, resp[i])
				require.NotNil(t, resp[i].Result)
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.NotNil(t, resp[i].Result.Errors.Error)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy disabled, but request was with tenant")
				assert.Equal(t, tenant.Name, resp[i].Tenant)
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

	t.Run("deletes objects from MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		for _, tenant := range tenants {
			for className, ids := range fixtures.IdsByClass {
				resp, err := client.Batch().ObjectsBatchDeleter().
					WithClassName(className).
					WithWhere(filters.Where().
						WithOperator(filters.Like).
						WithPath([]string{"name"}).
						WithValueText("*")).
					WithOutput("minimal").
					WithTenant(tenant.Name).
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
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.False(t, exists)
					}
				}
			}
		})
	})

	t.Run("fails deleting objects from MT class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

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
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.True(t, exists)
					}
				}
			}
		})
	})

	t.Run("fails deleting objects from MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

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
			assert.Contains(t, clientErr.Msg, "tenant not found")
			require.Nil(t, resp)
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for className, ids := range fixtures.IdsByClass {
					for _, id := range ids {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant.Name).
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

	t.Run("deletes objects from MT class by ref", func(t *testing.T) {
		defer cleanup()

		tenant1 := models.Tenant{Name: "tenantNo1"}
		tenant2 := models.Tenant{Name: "tenantNo2"}
		soupIdByTenantAndPizza := map[string]map[string]string{
			tenant1.Name: {
				fixtures.PIZZA_QUATTRO_FORMAGGI_ID: fixtures.SOUP_CHICKENSOUP_ID,
				fixtures.PIZZA_FRUTTI_DI_MARE_ID:   fixtures.SOUP_CHICKENSOUP_ID,
			},
			tenant2.Name: {
				fixtures.PIZZA_HAWAII_ID: fixtures.SOUP_BEAUTIFUL_ID,
				fixtures.PIZZA_DOENER_ID: fixtures.SOUP_BEAUTIFUL_ID,
			},
		}

		t.Run("add data", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenant1, tenant2)
			fixtures.CreateDataPizzaQuattroFormaggiForTenants(t, client, tenant1.Name)
			fixtures.CreateDataPizzaFruttiDiMareForTenants(t, client, tenant1.Name)
			fixtures.CreateDataPizzaHawaiiForTenants(t, client, tenant2.Name)
			fixtures.CreateDataPizzaDoenerForTenants(t, client, tenant2.Name)

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenant1, tenant2)
			fixtures.CreateDataSoupChickenForTenants(t, client, tenant1.Name)
			fixtures.CreateDataSoupBeautifulForTenants(t, client, tenant2.Name)
		})

		t.Run("create ref property", func(t *testing.T) {
			err := client.Schema().PropertyCreator().
				WithClassName("Soup").
				WithProperty(&models.Property{
					Name:     "relatedToPizza",
					DataType: []string{"Pizza"},
				}).
				Do(context.Background())

			require.Nil(t, err)
		})

		t.Run("create refs", func(t *testing.T) {
			references := []*models.BatchReference{}

			for tenant, pizzaToSoup := range soupIdByTenantAndPizza {
				for pizzaId, soupId := range pizzaToSoup {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromRefProp("relatedToPizza").
						WithFromID(soupId).
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenant)

					references = append(references, rpb.Payload())
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, resp)
			assert.Len(t, resp, len(references))
			for i := range resp {
				require.NotNil(t, resp[i].Result)
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for tenant, pizzaToSoup := range soupIdByTenantAndPizza {
			i := 0
			for pizzaId := range pizzaToSoup {
				resp, err := client.Batch().ObjectsBatchDeleter().
					WithClassName("Soup").
					WithWhere(filters.Where().
						WithOperator(filters.Like).
						WithPath([]string{"relatedToPizza", "Pizza", "_id"}).
						WithValueText(pizzaId)).
					WithOutput("minimal").
					WithTenant(tenant).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.Results)

				// single soup references two pizzas, so soup will be deleted only for 1st pizza
				if i == 0 {
					assert.Equal(t, int64(1), resp.Results.Matches)
					assert.Equal(t, int64(1), resp.Results.Successful)
				} else {
					assert.Equal(t, int64(0), resp.Results.Matches)
					assert.Equal(t, int64(0), resp.Results.Successful)
				}
				i++
			}
		}

		t.Run("verify deleted", func(t *testing.T) {
			for tenant, pizzaToSoup := range soupIdByTenantAndPizza {
				for _, soupId := range pizzaToSoup {
					exists, err := client.Data().Checker().
						WithID(soupId).
						WithClassName("Soup").
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.False(t, exists)
				}
			}
		})
	})
}
