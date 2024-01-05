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
	"strings"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func TestDataReference_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("creates references between MT classes", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]
		pizzaBeacons := fixtures.BeaconsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

		for _, tenant := range tenants {
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		}

		for _, tenant := range tenants {
			_, err := client.Data().Creator().
				WithClassName("Soup").
				WithID(fixtures.SOUP_TRIPE_ID).
				WithProperties(map[string]interface{}{
					"name":           "Tripe",
					"description":    "Tripe soup is a speciality of Romanian cuisine where it is known as Ciorbă de Burtă.",
					"price":          float32(2.3),
					"relatedToPizza": pizzaBeacons,
				}).
				WithTenant(tenant.Name).
				Do(context.Background())
			require.Nil(t, err)
		}
		allSoupIds := append(soupIds, fixtures.SOUP_TRIPE_ID)

		t.Run("verify created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range allSoupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIds))
				}
			}
		})

		t.Run("verify graphql search", func(t *testing.T) {
			for _, tenant := range tenants {
				resp, err := client.GraphQL().Get().
					WithClassName("Soup").
					WithTenant(tenant.Name).
					WithFields(graphql.Field{
						Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
					}).
					WithWhere(filters.Where().
						WithPath([]string{"relatedToPizza", "Pizza", "name"}).
						WithOperator(filters.Equal).
						WithValueString("Quattro Formaggi")).
					Do(context.Background())

				require.NoError(t, err)
				assertGraphqlGetIds(t, resp, "Soup", allSoupIds)
			}
		})
	})

	t.Run("fails creating references between MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]
		pizzaBeacons := fixtures.BeaconsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			}
		}

		_, err := client.Data().Creator().
			WithClassName("Soup").
			WithID(fixtures.SOUP_TRIPE_ID).
			WithProperties(map[string]interface{}{
				"name":           "Tripe",
				"description":    "Tripe soup is a speciality of Romanian cuisine where it is known as Ciorbă de Burtă.",
				"price":          float32(2.3),
				"relatedToPizza": pizzaBeacons,
			}).
			Do(context.Background())
		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
				_, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(fixtures.SOUP_TRIPE_ID).
					WithTenant(tenant.Name).
					Do(context.Background())
				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 404, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "")
			}
		})
	})

	t.Run("fails creating references between MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]
		pizzaBeacons := fixtures.BeaconsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")
			}
		}

		_, err := client.Data().Creator().
			WithClassName("Soup").
			WithID(fixtures.SOUP_TRIPE_ID).
			WithProperties(map[string]interface{}{
				"name":           "Tripe",
				"description":    "Tripe soup is a speciality of Romanian cuisine where it is known as Ciorbă de Burtă.",
				"price":          float32(2.3),
				"relatedToPizza": pizzaBeacons,
			}).
			WithTenant("nonExistentTenant").
			Do(context.Background())
		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "tenant not found")

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
				_, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(fixtures.SOUP_TRIPE_ID).
					WithTenant(tenant.Name).
					Do(context.Background())
				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 404, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "")
			}
		})
	})

	t.Run("fails creating references between MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]
		pizzaBeacons := fixtures.BeaconsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0].Name)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "no object with id")
			}
		}

		_, err := client.Data().Creator().
			WithClassName("Soup").
			WithID(fixtures.SOUP_TRIPE_ID).
			WithProperties(map[string]interface{}{
				"name":           "Tripe",
				"description":    "Tripe soup is a speciality of Romanian cuisine where it is known as Ciorbă de Burtă.",
				"price":          float32(2.3),
				"relatedToPizza": pizzaBeacons,
			}).
			WithTenant(tenants[1].Name).
			Do(context.Background())
		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "no object with id")

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
			_, err := client.Data().ObjectsGetter().
				WithClassName("Soup").
				WithID(fixtures.SOUP_TRIPE_ID).
				WithTenant(tenants[0].Name).
				Do(context.Background())
			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 404, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "")
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Pizza").
					WithID(pizzaId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT classes and different tenants", func(t *testing.T) {
		tenantPizza := models.Tenant{Name: "tenantPizza"}
		tenantSoup := models.Tenant{Name: "tenantSoup"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		t.Run("with SRC tenant (common tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name). // SRC tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "no object with id")
				}
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with SRC tenant (separate tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name). // SRC tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "tenant not found")
				}
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with DEST tenant (common tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantPizza.Name). // DEST tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 404, clientErr.StatusCode)
					assert.Empty(t, clientErr.Msg)
				}
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with DEST tenant (separate tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantPizza.Name). // DEST tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "tenant not found")
				}
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			})
		})
	})

	t.Run("deletes references between MT classes", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIds {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, tenant := range tenants {
			for _, soupId := range soupIds {
				expectedRefsLeft := len(pizzaIds)

				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceDeleter().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)

					t.Run("verify deleted one by one", func(t *testing.T) {
						expectedRefsLeft--
						objects, err := client.Data().ObjectsGetter().
							WithClassName("Soup").
							WithID(soupId).
							WithTenant(tenant.Name).
							Do(context.Background())

						require.Nil(t, err)
						require.NotNil(t, objects)
						require.Len(t, objects, 1)
						assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
							expectedRefsLeft)
					})
				}
			}
		}
	})

	t.Run("fails deleting references between MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIds {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIds))
				}
			}
		})
	})

	t.Run("fails deleting references between MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIds {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant("nonExistentTenant").
					Do(context.Background())
				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIds))
				}
			}
		})
	})

	t.Run("fails deleting references between MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0].Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromID(soupId).
						WithFromRefProp("relatedToPizza").
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenants[0].Name)

					references = append(references, rpb.Payload())
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 404, clientErr.StatusCode)
				assert.Empty(t, clientErr.Msg)
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})
	})

	t.Run("replaces references between MT classes", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIdsBefore {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, tenant := range tenants {
			for _, soupId := range soupIds {
				var refs models.MultipleRef
				for _, pizzaId := range pizzaIdsAfter {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()
					refs = append(refs, ref)
				}

				err := client.Data().ReferenceReplacer().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReferences(&refs).
					WithTenant(tenant.Name).
					Do(context.Background())

				require.Nil(t, err)
			}
		}

		t.Run("verify replaced", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIdsAfter))

					for _, pizzaId := range pizzaIdsAfter {
						found := false
						for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
							if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
								found = true
								break
							}
						}
						assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
					}
				}
			}
		})

		t.Run("verify graphql search", func(t *testing.T) {
			for _, tenant := range tenants {
				resp, err := client.GraphQL().Get().
					WithClassName("Soup").
					WithTenant(tenant.Name).
					WithFields(graphql.Field{
						Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
					}).
					WithWhere(filters.Where().
						WithPath([]string{"relatedToPizza", "Pizza", "name"}).
						WithOperator(filters.Equal).
						WithValueString("Quattro Formaggi")).
					Do(context.Background())

				require.NoError(t, err)
				assertGraphqlGetIds(t, resp, "Soup", []string{})
			}
		})
	})

	t.Run("fails replacing references between MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIdsBefore {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIdsBefore))

					for _, pizzaId := range pizzaIdsBefore {
						found := false
						for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
							if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
								found = true
								break
							}
						}
						assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
					}
				}
			}
		})
	})

	t.Run("fails replacing references between MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants.Names()...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants.Names()...)

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

			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					for _, pizzaId := range pizzaIdsBefore {
						rpb := client.Batch().ReferencePayloadBuilder().
							WithFromClassName("Soup").
							WithFromID(soupId).
							WithFromRefProp("relatedToPizza").
							WithToClassName("Pizza").
							WithToID(pizzaId).
							WithTenant(tenant.Name)

						references = append(references, rpb.Payload())
					}
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "tenant not found")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						len(pizzaIdsBefore))

					for _, pizzaId := range pizzaIdsBefore {
						found := false
						for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
							if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
								found = true
								break
							}
						}
						assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
					}
				}
			}
		})
	})

	t.Run("fails replacing references between MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0].Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIdsBefore {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromID(soupId).
						WithFromRefProp("relatedToPizza").
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenants[0].Name)

					references = append(references, rpb.Payload())
				}
			}

			resp, err := client.Batch().ReferencesBatcher().
				WithReferences(references...).
				Do(context.Background())

			require.Nil(t, err)
			require.Len(t, resp, len(references))
			for i := range resp {
				assert.Nil(t, resp[i].Result.Errors)
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant(tenants[1].Name).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 404, clientErr.StatusCode)
			assert.Empty(t, clientErr.Msg)
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIdsBefore))

				for _, pizzaId := range pizzaIdsBefore {
					found := false
					for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
						if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
							found = true
							break
						}
					}
					assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
				}
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, pizzaId := range fixtures.IdsByClass["Pizza"] {
				exists, err := client.Data().Checker().
					WithClassName("Pizza").
					WithID(pizzaId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails replacing references between MT classes and different tenants", func(t *testing.T) {
		tenantPizza := models.Tenant{Name: "tenantPizza"}
		tenantSoup := models.Tenant{Name: "tenantSoup"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		t.Run("with SRC tenant (common tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					refs := models.MultipleRef{
						client.Data().ReferencePayloadBuilder().
							WithClassName("Pizza").
							WithID(pizzaId).
							Payload(),
					}

					err := client.Data().ReferenceReplacer().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReferences(&refs).
						WithTenant(tenantSoup.Name). // SRC tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "no object with id")
				}
			}

			t.Run("verify not replaced", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with SRC tenant (separate tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					refs := models.MultipleRef{
						client.Data().ReferencePayloadBuilder().
							WithClassName("Pizza").
							WithID(pizzaId).
							Payload(),
					}

					err := client.Data().ReferenceReplacer().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReferences(&refs).
						WithTenant(tenantSoup.Name). // SRC tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "tenant not found")
				}
			}

			t.Run("verify not replaced", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with DEST tenant (common tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					refs := models.MultipleRef{
						client.Data().ReferencePayloadBuilder().
							WithClassName("Pizza").
							WithID(pizzaId).
							Payload(),
					}

					err := client.Data().ReferenceReplacer().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReferences(&refs).
						WithTenant(tenantPizza.Name). // DEST tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 404, clientErr.StatusCode)
					assert.Empty(t, clientErr.Msg)
				}
			}

			t.Run("verify not replaced", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}
			})
		})

		t.Run("with DEST tenant (separate tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					refs := models.MultipleRef{
						client.Data().ReferencePayloadBuilder().
							WithClassName("Pizza").
							WithID(pizzaId).
							Payload(),
					}

					err := client.Data().ReferenceReplacer().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReferences(&refs).
						WithTenant(tenantPizza.Name). // DEST tenant
						Do(context.Background())

					require.NotNil(t, err)
					clientErr := err.(*fault.WeaviateClientError)
					assert.Equal(t, 422, clientErr.StatusCode)
					assert.Contains(t, clientErr.Msg, "tenant not found")
				}
			}

			t.Run("verify not replaced", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			})

			t.Run("verify new objects not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantPizza.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			})
		})
	})

	t.Run("creates references between MT and non-MT classes", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
			}
		}

		t.Run("verify created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})

		t.Run("verify graphql search", func(t *testing.T) {
			resp, err := client.GraphQL().Get().
				WithClassName("Soup").
				WithTenant(tenantSoup.Name).
				WithFields(graphql.Field{
					Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
				}).
				WithWhere(filters.Where().
					WithPath([]string{"relatedToPizza", "Pizza", "name"}).
					WithOperator(filters.Equal).
					WithValueString("Quattro Formaggi")).
				Do(context.Background())

			require.NoError(t, err)
			assertGraphqlGetIds(t, resp, "Soup", soupIds)
		})
	})

	t.Run("fails creating references between MT and non-MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Pizza").
					WithID(pizzaId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT and non-MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Pizza").
					WithID(pizzaId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT and non-MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 404, clientErr.StatusCode) // TODO 422?
				assert.Empty(t, clientErr.Msg)
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}

			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, tenant := range tenants {
				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}
		})
	})

	t.Run("deletes references between MT and non-MT classes", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			expectedRefsLeft := len(pizzaIds)

			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)

				t.Run("verify deleted one by one", func(t *testing.T) {
					expectedRefsLeft--
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
						expectedRefsLeft)
				})
			}
		}
	})

	t.Run("fails deleting references between MT and non-MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})
	})

	t.Run("fails deleting references between MT and non-MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "tenant not found")
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})
	})

	t.Run("fails deleting references between MT and non-MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenants[0].Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceDeleter().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 404, clientErr.StatusCode)
				assert.Empty(t, clientErr.Msg)
			}
		}

		t.Run("verify not deleted", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})
	})

	t.Run("replaces references between MT and non-MT classes", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIdsBefore {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant(tenantSoup.Name).
				Do(context.Background())

			require.Nil(t, err)
		}

		t.Run("verify replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIdsAfter))

				for _, pizzaId := range pizzaIdsAfter {
					found := false
					for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
						if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
							found = true
							break
						}
					}
					assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
				}
			}
		})

		t.Run("verify graphql search", func(t *testing.T) {
			resp, err := client.GraphQL().Get().
				WithClassName("Soup").
				WithTenant(tenantSoup.Name).
				WithFields(graphql.Field{
					Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
				}).
				WithWhere(filters.Where().
					WithPath([]string{"relatedToPizza", "Pizza", "name"}).
					WithOperator(filters.Equal).
					WithValueString("Quattro Formaggi")).
				Do(context.Background())

			require.NoError(t, err)
			assertGraphqlGetIds(t, resp, "Soup", []string{})
		})
	})

	t.Run("fails replacing references between MT and non-MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIdsBefore {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIdsBefore))

				for _, pizzaId := range pizzaIdsBefore {
					found := false
					for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
						if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
							found = true
							break
						}
					}
					assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
				}
			}
		})
	})

	t.Run("fails replacing references between MT and non-MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := models.Tenant{Name: "tenantSoup"}
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup.Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIdsBefore {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenantSoup.Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "tenant not found")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup.Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIdsBefore))

				for _, pizzaId := range pizzaIdsBefore {
					found := false
					for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
						if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
							found = true
							break
						}
					}
					assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
				}
			}
		})
	})

	t.Run("fails replacing references between MT and non-MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIdsBefore := fixtures.IdsByClass["Pizza"][:2]
		pizzaIdsAfter := fixtures.IdsByClass["Pizza"][2:]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0].Name)

		fixtures.CreateSchemaPizza(t, client)
		fixtures.CreateDataPizza(t, client)

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
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIdsBefore {
					ref := client.Data().ReferencePayloadBuilder().
						WithClassName("Pizza").
						WithID(pizzaId).
						Payload()

					err := client.Data().ReferenceCreator().
						WithClassName("Soup").
						WithID(soupId).
						WithReferenceProperty("relatedToPizza").
						WithReference(ref).
						WithTenant(tenants[0].Name).
						Do(context.Background())

					require.Nil(t, err)
				}
			}
		})

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIdsAfter {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant(tenants[1].Name).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 404, clientErr.StatusCode)
			assert.Empty(t, clientErr.Msg)
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0].Name).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				require.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIdsBefore))

				for _, pizzaId := range pizzaIdsBefore {
					found := false
					for _, ref := range objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}) {
						if strings.Contains(ref.(map[string]interface{})["beacon"].(string), pizzaId) {
							found = true
							break
						}
					}
					assert.True(t, found, fmt.Sprintf("ref to '%s' not found", pizzaId))
				}
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, tenant := range tenants {
				for _, pizzaId := range fixtures.IdsByClass["Pizza"] {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				// TODO valid error?
				assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant("nonExistentTenant").
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				// TODO valid error?
				assert.Contains(t, clientErr.Msg, "tenant not found")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0].Name)

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

		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()

				err := client.Data().ReferenceCreator().
					WithClassName("Soup").
					WithID(soupId).
					WithReferenceProperty("relatedToPizza").
					WithReference(ref).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				clientErr := err.(*fault.WeaviateClientError)
				assert.Equal(t, 422, clientErr.StatusCode)
				// TODO valid error?
				assert.Contains(t, clientErr.Msg, "no object with id")
			}
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(pizzaId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails replacing references between non-MT and MT classes", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant(tenantPizza.Name).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails replacing references between non-MT and MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			// TODO valid error?
			assert.Contains(t, clientErr.Msg, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails replacing references between non-MT and MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := models.Tenant{Name: "tenantPizza"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza.Name)

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

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant("nonExistentTenant").
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantPizza.Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails replacing references between non-MT and MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0].Name)

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

		for _, soupId := range soupIds {
			var refs models.MultipleRef
			for _, pizzaId := range pizzaIds {
				ref := client.Data().ReferencePayloadBuilder().
					WithClassName("Pizza").
					WithID(pizzaId).
					Payload()
				refs = append(refs, ref)
			}

			err := client.Data().ReferenceReplacer().
				WithClassName("Soup").
				WithID(soupId).
				WithReferenceProperty("relatedToPizza").
				WithReferences(&refs).
				WithTenant(tenants[1].Name).
				Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "has multi-tenancy disabled, but request was with tenant")
		}

		t.Run("verify not replaced", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
			}
		})

		t.Run("verify new objects not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					exists, err := client.Data().Checker().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant.Name).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(pizzaId).
					WithTenant(tenants[1].Name).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})
}

func assertGraphqlGetIds(t *testing.T, resp *models.GraphQLResponse, className string,
	expectedIds []string,
) {
	require.NotNil(t, resp)
	require.Nil(t, resp.Errors)
	require.NotNil(t, resp.Data)
	require.Contains(t, resp.Data, "Get")

	get := resp.Data["Get"]
	require.NotNil(t, get)

	classes, ok := get.(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, classes, className)

	objects, ok := classes[className].([]interface{})
	require.True(t, ok)

	ids := make([]string, len(objects))
	for i := range objects {
		props, ok := objects[i].(map[string]interface{})
		require.True(t, ok)
		require.Contains(t, props, "_additional")

		add, ok := props["_additional"].(map[string]interface{})
		require.True(t, ok)
		require.Contains(t, add, "id")

		id, ok := add["id"].(string)
		require.True(t, ok)
		ids[i] = id
	}
	require.ElementsMatch(t, expectedIds, ids)
}
