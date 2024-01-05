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
	"fmt"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
)

func TestData_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("creates objects of MT class", func(t *testing.T) {
		defer cleanup()

		className := "Pizza"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		for _, tenant := range tenants {
			wrap, err := client.Data().Creator().
				WithClassName(className).
				WithID(fixtures.PIZZA_QUATTRO_FORMAGGI_ID).
				WithProperties(map[string]interface{}{
					"name":        "Quattro Formaggi",
					"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
					"price":       float32(1.1),
					"best_before": "2022-05-03T12:04:40+02:00",
				}).
				WithTenant(tenant.Name).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, wrap)
			require.NotNil(t, wrap.Object)
			assert.Equal(t, strfmt.UUID(fixtures.PIZZA_QUATTRO_FORMAGGI_ID), wrap.Object.ID)
			assert.Equal(t, "Quattro Formaggi", wrap.Object.Properties.(map[string]interface{})["name"])
			assert.Equal(t, tenant.Name, wrap.Object.Tenant)

			wrap, err = client.Data().Creator().
				WithClassName(className).
				WithID(fixtures.PIZZA_FRUTTI_DI_MARE_ID).
				WithProperties(map[string]interface{}{
					"name":        "Frutti di Mare",
					"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
					"price":       float32(1.2),
					"best_before": "2022-05-05T07:16:30+02:00",
				}).
				WithTenant(tenant.Name).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, wrap)
			require.NotNil(t, wrap.Object)
			assert.Equal(t, strfmt.UUID(fixtures.PIZZA_FRUTTI_DI_MARE_ID), wrap.Object.ID)
			assert.Equal(t, "Frutti di Mare", wrap.Object.Properties.(map[string]interface{})["name"])
			assert.Equal(t, tenant.Name, wrap.Object.Tenant)
		}

		t.Run("verify created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, id := range []string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					fixtures.PIZZA_FRUTTI_DI_MARE_ID,
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

		className := "Pizza"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		wrap, err := client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_QUATTRO_FORMAGGI_ID).
			WithProperties(map[string]interface{}{
				"name":        "Quattro Formaggi",
				"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
				"price":       float32(1.1),
				"best_before": "2022-05-03T12:04:40+02:00",
			}).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
		require.Nil(t, wrap)

		wrap, err = client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_FRUTTI_DI_MARE_ID).
			WithProperties(map[string]interface{}{
				"name":        "Frutti di Mare",
				"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
				"price":       float32(1.2),
				"best_before": "2022-05-05T07:16:30+02:00",
			}).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
		require.Nil(t, wrap)

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, id := range []string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					fixtures.PIZZA_FRUTTI_DI_MARE_ID,
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

		className := "Pizza"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		wrap, err := client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_QUATTRO_FORMAGGI_ID).
			WithProperties(map[string]interface{}{
				"name":        "Quattro Formaggi",
				"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
				"price":       float32(1.1),
				"best_before": "2022-05-03T12:04:40+02:00",
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")
		require.Nil(t, wrap)

		wrap, err = client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_FRUTTI_DI_MARE_ID).
			WithProperties(map[string]interface{}{
				"name":        "Frutti di Mare",
				"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
				"price":       float32(1.2),
				"best_before": "2022-05-05T07:16:30+02:00",
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")
		require.Nil(t, wrap)

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, id := range []string{
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					fixtures.PIZZA_FRUTTI_DI_MARE_ID,
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

		className := "Pizza"

		fixtures.CreateSchemaPizza(t, client)

		wrap, err := client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_QUATTRO_FORMAGGI_ID).
			WithProperties(map[string]interface{}{
				"name":        "Quattro Formaggi",
				"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
				"price":       float32(1.1),
				"best_before": "2022-05-03T12:04:40+02:00",
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
		require.Nil(t, wrap)

		wrap, err = client.Data().Creator().
			WithClassName(className).
			WithID(fixtures.PIZZA_FRUTTI_DI_MARE_ID).
			WithProperties(map[string]interface{}{
				"name":        "Frutti di Mare",
				"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
				"price":       float32(1.2),
				"best_before": "2022-05-05T07:16:30+02:00",
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
		require.Nil(t, wrap)

		t.Run("verify not created", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			assert.Len(t, objects, 0)
		})
	})

	t.Run("gets objects of MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		extractIds := func(objs []*models.Object) []string {
			ids := make([]string, len(objs))
			for i, obj := range objs {
				ids[i] = obj.ID.String()
			}
			return ids
		}

		for _, tenant := range tenants {
			for className, ids := range fixtures.IdsByClass {
				for _, id := range ids {
					t.Run("single object by class+id", func(t *testing.T) {
						objects, err := client.Data().ObjectsGetter().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.NotNil(t, objects)
						require.Len(t, objects, 1)
						assert.Equal(t, strfmt.UUID(id), objects[0].ID)
						assert.Equal(t, tenant.Name, objects[0].Tenant)
					})
				}

				t.Run("list objects by class", func(t *testing.T) {
					objects, err := client.Data().ObjectsGetter().
						WithClassName(className).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, len(ids))
					assert.ElementsMatch(t, ids, extractIds(objects))
				})
			}

			t.Run("list all objects", func(t *testing.T) {
				objects, err := client.Data().ObjectsGetter().
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, len(fixtures.AllIds))
				assert.ElementsMatch(t, fixtures.AllIds, extractIds(objects))
			})
		}
	})

	t.Run("fails getting objects of MT class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				t.Run("single object by class+id", func(t *testing.T) {
					objects, err := client.Data().ObjectsGetter().
						WithID(id).
						WithClassName(className).
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
					assert.Nil(t, objects)
				})
			}

			t.Run("list objects by class", func(t *testing.T) {
				objects, err := client.Data().ObjectsGetter().
					WithClassName(className).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
				assert.Nil(t, objects)
			})
		}

		t.Run("list all objects", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			assert.Len(t, objects, 0)
		})
	})

	t.Run("fails getting objects of MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				t.Run("single object by class+id", func(t *testing.T) {
					objects, err := client.Data().ObjectsGetter().
						WithID(id).
						WithClassName(className).
						WithTenant("nonExistentTenant").
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "tenant not found")
					assert.Nil(t, objects)
				})
			}

			t.Run("list objects by class", func(t *testing.T) {
				objects, err := client.Data().ObjectsGetter().
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")
				assert.Nil(t, objects)
			})
		}

		t.Run("list all objects", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			assert.Len(t, objects, 0)
		})
	})

	t.Run("fails getting objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaFood(t, client)
		fixtures.CreateDataFood(t, client)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				t.Run("single object by class+id", func(t *testing.T) {
					objects, err := client.Data().ObjectsGetter().
						WithID(id).
						WithClassName(className).
						WithTenant("nonExistentTenant").
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
					require.Nil(t, objects)
				})
			}

			t.Run("list objects by class", func(t *testing.T) {
				objects, err := client.Data().ObjectsGetter().
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
				require.Nil(t, objects)
			})
		}

		t.Run("list all objects", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			assert.Len(t, objects, 0)
		})
	})

	t.Run("checks objects of MT class", func(t *testing.T) {
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

	t.Run("fails checking objects of MT class without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				exists, err := client.Data().Checker().
					WithID(id).
					WithClassName(className).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Empty(t, clientErr.Msg) // no body in HEAD
				assert.False(t, exists)
			}
		}
	})

	t.Run("fails checking objects of MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaFoodForTenants(t, client)
		fixtures.CreateTenantsFood(t, client, tenants...)
		fixtures.CreateDataFoodForTenants(t, client, tenants.Names()...)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				exists, err := client.Data().Checker().
					WithID(id).
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Empty(t, clientErr.Msg) // no body in HEAD
				assert.False(t, exists)
			}
		}
	})

	t.Run("fails checking objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaFood(t, client)
		fixtures.CreateDataFood(t, client)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				exists, err := client.Data().Checker().
					WithID(id).
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Empty(t, clientErr.Msg) // no body in HEAD
				require.False(t, exists)
			}
		}
	})

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
				expectedLeft := len(ids)

				for _, id := range ids {
					err := client.Data().Deleter().
						WithID(id).
						WithClassName(className).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					expectedLeft--

					t.Run("verify deleted", func(t *testing.T) {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.False(t, exists)
					})

					t.Run("verify left", func(t *testing.T) {
						objects, err := client.Data().ObjectsGetter().
							WithClassName(className).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.NotNil(t, objects)
						assert.Len(t, objects, expectedLeft)
					})
				}
			}
		}
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

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				err := client.Data().Deleter().
					WithID(id).
					WithClassName(className).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

				t.Run("verify not deleted", func(t *testing.T) {
					for _, tenant := range tenants {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.True(t, exists)
					}
				})
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				assert.Len(t, objects, len(fixtures.AllIds))
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

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				err := client.Data().Deleter().
					WithID(id).
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")

				t.Run("verify not deleted", func(t *testing.T) {
					for _, tenant := range tenants {
						exists, err := client.Data().Checker().
							WithID(id).
							WithClassName(className).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.True(t, exists)
					}
				})
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				assert.Len(t, objects, len(fixtures.AllIds))
			}
		})
	})

	t.Run("fails deleting objects from non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaFood(t, client)
		fixtures.CreateDataFood(t, client)

		for className, ids := range fixtures.IdsByClass {
			for _, id := range ids {
				err := client.Data().Deleter().
					WithID(id).
					WithClassName(className).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")

				t.Run("verify not deleted", func(t *testing.T) {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(context.Background())

					require.Nil(t, err)
					require.True(t, exists)
				})
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			assert.Len(t, objects, len(fixtures.AllIds))
		})
	})

	t.Run("updates objects of MT class", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		for _, tenant := range tenants {
			err := client.Data().Updater().
				WithClassName(className).
				WithID(fixtures.SOUP_CHICKENSOUP_ID).
				WithProperties(map[string]interface{}{
					"name":        "ChickenSoup",
					"description": fmt.Sprintf("updated ChickenSoup description [%s]", tenant),
					"price":       float32(2.1),
				}).
				WithTenant(tenant.Name).
				Do(context.Background())

			require.Nil(t, err)

			err = client.Data().Updater().
				WithClassName(className).
				WithID(fixtures.SOUP_BEAUTIFUL_ID).
				WithProperties(map[string]interface{}{
					"name":        "Beautiful",
					"description": fmt.Sprintf("updated Beautiful description [%s]", tenant),
					"price":       float32(2.2),
				}).
				WithTenant(tenant.Name).
				Do(context.Background())

			require.Nil(t, err)
		}

		t.Run("verify updated", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, fmt.Sprintf("updated ChickenSoup description [%s]", tenant),
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, fmt.Sprintf("updated Beautiful description [%s]", tenant),
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails updating objects of MT class without tenant", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"name":        "ChickenSoup",
				"description": "updated ChickenSoup description",
				"price":       float32(2.1),
			}).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"name":        "Beautiful",
				"description": "updated Beautiful description",
				"price":       float32(2.2),
			}).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

		t.Run("verify not updated", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Putting the game of letter soups to a whole new level.",
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails updating objects of MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"name":        "ChickenSoup",
				"description": "updated ChickenSoup description",
				"price":       float32(2.1),
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"name":        "Beautiful",
				"description": "updated Beautiful description",
				"price":       float32(2.2),
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")

		t.Run("verify not updated", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Putting the game of letter soups to a whole new level.",
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails updating objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		className := "Soup"

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"name":        "ChickenSoup",
				"description": "updated ChickenSoup description",
				"price":       float32(2.1),
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"name":        "Beautiful",
				"description": "updated Beautiful description",
				"price":       float32(2.2),
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")

		t.Run("verify not updated", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				WithID(fixtures.SOUP_CHICKENSOUP_ID).
				WithClassName(className).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects, 1)
			assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
				objects[0].Properties.(map[string]interface{})["description"])

			objects, err = client.Data().ObjectsGetter().
				WithID(fixtures.SOUP_BEAUTIFUL_ID).
				WithClassName(className).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects, 1)
			assert.Equal(t, "Putting the game of letter soups to a whole new level.",
				objects[0].Properties.(map[string]interface{})["description"])
		})
	})

	t.Run("merges objects of MT class", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		for _, tenant := range tenants {
			err := client.Data().Updater().
				WithClassName(className).
				WithID(fixtures.SOUP_CHICKENSOUP_ID).
				WithProperties(map[string]interface{}{
					"description": fmt.Sprintf("merged ChickenSoup description [%s]", tenant),
				}).
				WithTenant(tenant.Name).
				WithMerge().
				Do(context.Background())

			require.Nil(t, err)

			err = client.Data().Updater().
				WithClassName(className).
				WithID(fixtures.SOUP_BEAUTIFUL_ID).
				WithProperties(map[string]interface{}{
					"description": fmt.Sprintf("merged Beautiful description [%s]", tenant),
				}).
				WithTenant(tenant.Name).
				WithMerge().
				Do(context.Background())

			require.Nil(t, err)
		}

		t.Run("verify merged", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, fmt.Sprintf("merged ChickenSoup description [%s]", tenant),
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, fmt.Sprintf("merged Beautiful description [%s]", tenant),
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails merging objects of MT class without tenant", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"description": "merged ChickenSoup description",
			}).
			WithMerge().
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"description": "merged Beautiful description",
			}).
			WithMerge().
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

		t.Run("verify not merged", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Putting the game of letter soups to a whole new level.",
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails merging objects of MT class with non existent tenant", func(t *testing.T) {
		defer cleanup()

		className := "Soup"
		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"description": "merged ChickenSoup description",
			}).
			WithMerge().
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"description": "merged Beautiful description",
			}).
			WithMerge().
			WithTenant("nonExistentTenant").
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")

		t.Run("verify not merged", func(t *testing.T) {
			for _, tenant := range tenants {
				objects, err := client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_CHICKENSOUP_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
					objects[0].Properties.(map[string]interface{})["description"])

				objects, err = client.Data().ObjectsGetter().
					WithID(fixtures.SOUP_BEAUTIFUL_ID).
					WithClassName(className).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Equal(t, tenant.Name, objects[0].Tenant)
				assert.Equal(t, "Putting the game of letter soups to a whole new level.",
					objects[0].Properties.(map[string]interface{})["description"])
			}
		})
	})

	t.Run("fails merging objects of non-MT class when tenant given", func(t *testing.T) {
		defer cleanup()

		className := "Soup"

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		err := client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_CHICKENSOUP_ID).
			WithProperties(map[string]interface{}{
				"description": "merged ChickenSoup description [%s]",
			}).
			WithTenant("nonExistentTenant").
			WithMerge().
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")

		err = client.Data().Updater().
			WithClassName(className).
			WithID(fixtures.SOUP_BEAUTIFUL_ID).
			WithProperties(map[string]interface{}{
				"description": "merged Beautiful description [%s]",
			}).
			WithTenant("nonExistentTenant").
			WithMerge().
			Do(context.Background())

		require.NotNil(t, err)
		clientErr = err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")

		t.Run("verify not merged", func(t *testing.T) {
			objects, err := client.Data().ObjectsGetter().
				WithID(fixtures.SOUP_CHICKENSOUP_ID).
				WithClassName(className).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects, 1)
			assert.Equal(t, "Used by humans when their inferior genetics are attacked by microscopic organisms.",
				objects[0].Properties.(map[string]interface{})["description"])

			objects, err = client.Data().ObjectsGetter().
				WithID(fixtures.SOUP_BEAUTIFUL_ID).
				WithClassName(className).
				Do(context.Background())

			require.Nil(t, err)
			require.NotNil(t, objects)
			require.Len(t, objects, 1)
			assert.Equal(t, "Putting the game of letter soups to a whole new level.",
				objects[0].Properties.(map[string]interface{})["description"])
		})
	})
}
