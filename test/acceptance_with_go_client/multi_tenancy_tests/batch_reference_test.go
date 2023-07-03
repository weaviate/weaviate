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
	"github.com/weaviate/weaviate/entities/models"
)

func TestBatchReferenceCreate_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("creates references between MT classes", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants...)

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

		references := []*models.BatchReference{}
		for _, tenant := range tenants {
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
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
		}

		resp, err := client.Batch().ReferencesBatcher().
			WithReferences(references...).
			Do(context.Background())

		require.Nil(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp, len(references))
		for i := range resp {
			require.NotNil(t, resp[i].Result)
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "SUCCESS", *resp[i].Result.Status)
			assert.Nil(t, resp[i].Result.Errors)
		}

		t.Run("verify created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant).
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

	t.Run("fails creating references between MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants...)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId)

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			}
		})
	})

	t.Run("fails creating references between MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants...)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants...)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant("nonExistentTenant")

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "no tenant found with key")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, tenant := range tenants {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenant).
						Do(context.Background())

					require.Nil(t, err)
					require.NotNil(t, objects)
					require.Len(t, objects, 1)
					assert.Nil(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"])
				}
			}
		})
	})

	t.Run("fails creating references between MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0])

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0])

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant(tenants[1])

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "not found for tenant")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0]).
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
					WithTenant(tenants[1]).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Pizza").
					WithID(pizzaId).
					WithTenant(tenants[1]).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT classes and different tenants", func(t *testing.T) {
		tenantPizza := "tenantPizza"
		tenantSoup := "tenantSoup"
		soupIds := fixtures.IdsByClass["Soup"]
		pizzaIds := fixtures.IdsByClass["Pizza"]

		t.Run("with SRC tenant (common tenants)", func(t *testing.T) {
			defer cleanup()

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

			references := []*models.BatchReference{}
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromRefProp("relatedToPizza").
						WithFromID(soupId).
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenantSoup) // SRC tenant

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
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "not found for tenant")
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup).
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
						WithTenant(tenantPizza).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup).
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
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

			references := []*models.BatchReference{}
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromRefProp("relatedToPizza").
						WithFromID(soupId).
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenantSoup) // SRC tenant

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
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "no tenant found with key")
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup).
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
						WithTenant(tenantPizza).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup).
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
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza, tenantSoup)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

			references := []*models.BatchReference{}
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromRefProp("relatedToPizza").
						WithFromID(soupId).
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenantPizza) // DEST tenant

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
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "not found for tenant")
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup).
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
						WithTenant(tenantPizza).
						Do(context.Background())

					require.Nil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup).
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
			fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenantPizza)
			fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

			references := []*models.BatchReference{}
			for _, soupId := range soupIds {
				for _, pizzaId := range pizzaIds {
					rpb := client.Batch().ReferencePayloadBuilder().
						WithFromClassName("Soup").
						WithFromRefProp("relatedToPizza").
						WithFromID(soupId).
						WithToClassName("Pizza").
						WithToID(pizzaId).
						WithTenant(tenantPizza) // DEST tenant

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
				require.NotNil(t, resp[i].Result.Status)
				assert.Equal(t, "FAILED", *resp[i].Result.Status)
				require.NotNil(t, resp[i].Result.Errors)
				require.Len(t, resp[i].Result.Errors.Error, 1)
				assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "no tenant found with key")
			}

			t.Run("verify not created", func(t *testing.T) {
				for _, soupId := range soupIds {
					objects, err := client.Data().ObjectsGetter().
						WithClassName("Soup").
						WithID(soupId).
						WithTenant(tenantSoup).
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
						WithTenant(tenantPizza).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}

				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenantSoup).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			})
		})
	})

	t.Run("creates references between MT and non-MT classes", func(t *testing.T) {
		defer cleanup()

		tenantSoup := "tenantSoup"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant(tenantSoup)

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "SUCCESS", *resp[i].Result.Status)
			assert.Nil(t, resp[i].Result.Errors)
		}

		t.Run("verify created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup).
					Do(context.Background())

				require.Nil(t, err)
				require.NotNil(t, objects)
				require.Len(t, objects, 1)
				assert.Len(t, objects[0].Properties.(map[string]interface{})["relatedToPizza"].([]interface{}),
					len(pizzaIds))
			}
		})
	})

	t.Run("fails creating references between MT and non-MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := "tenantSoup"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId)

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "has multi-tenancy enabled, but request was without tenant")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup).
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
					WithTenant(tenantSoup).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT and non-MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantSoup := "tenantSoup"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenantSoup)
		fixtures.CreateDataSoupForTenants(t, client, tenantSoup)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant("nonExistentTenant")

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "no tenant found with key")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenantSoup).
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
					WithTenant(tenantSoup).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between MT and non-MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoupForTenants(t, client)
		fixtures.CreateTenantsSoup(t, client, tenants...)
		fixtures.CreateDataSoupForTenants(t, client, tenants[0])

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant(tenants[1])

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "not found for tenant")
		}

		t.Run("verify not created", func(t *testing.T) {
			for _, soupId := range soupIds {
				objects, err := client.Data().ObjectsGetter().
					WithClassName("Soup").
					WithID(soupId).
					WithTenant(tenants[0]).
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
					WithTenant(tenants[1]).
					Do(context.Background())

				require.Nil(t, err)
				assert.False(t, exists)
			}

			for _, tenant := range tenants {
				for _, pizzaId := range pizzaIds {
					exists, err := client.Data().Checker().
						WithClassName("Pizza").
						WithID(pizzaId).
						WithTenant(tenant).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes", func(t *testing.T) {
		defer cleanup()

		tenantPizza := "tenantPizza"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant(tenantPizza)

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "cannot reference a multi-tenant enabled class from a non multi-tenant enabled class")
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
					WithTenant(tenantPizza).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes without tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := "tenantPizza"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId)

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "cannot reference a multi-tenant enabled class from a non multi-tenant enabled class")
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
					WithTenant(tenantPizza).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes with non existent tenant", func(t *testing.T) {
		defer cleanup()

		tenantPizza := "tenantPizza"
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenantPizza)
		fixtures.CreateDataPizzaForTenants(t, client, tenantPizza)

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant("nonExistentTenant")

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "cannot reference a multi-tenant enabled class from a non multi-tenant enabled class")
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
					WithTenant(tenantPizza).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})

	t.Run("fails creating references between non-MT and MT classes with different existing tenant", func(t *testing.T) {
		defer cleanup()

		tenants := []string{"tenantNo1", "tenantNo2"}
		pizzaIds := fixtures.IdsByClass["Pizza"]
		soupIds := fixtures.IdsByClass["Soup"]

		fixtures.CreateSchemaSoup(t, client)
		fixtures.CreateDataSoup(t, client)

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)
		fixtures.CreateDataPizzaForTenants(t, client, tenants[0])

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

		references := []*models.BatchReference{}
		for _, soupId := range soupIds {
			for _, pizzaId := range pizzaIds {
				rpb := client.Batch().ReferencePayloadBuilder().
					WithFromClassName("Soup").
					WithFromRefProp("relatedToPizza").
					WithFromID(soupId).
					WithToClassName("Pizza").
					WithToID(pizzaId).
					WithTenant(tenants[1])

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
			require.NotNil(t, resp[i].Result.Status)
			assert.Equal(t, "FAILED", *resp[i].Result.Status)
			require.NotNil(t, resp[i].Result.Errors)
			require.Len(t, resp[i].Result.Errors.Error, 1)
			assert.Contains(t, resp[i].Result.Errors.Error[0].Message, "cannot reference a multi-tenant enabled class from a non multi-tenant enabled class")
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
						WithTenant(tenant).
						Do(context.Background())

					require.NotNil(t, err)
					assert.False(t, exists)
				}
			}

			for _, pizzaId := range pizzaIds {
				exists, err := client.Data().Checker().
					WithClassName("Soup").
					WithID(pizzaId).
					WithTenant(tenants[1]).
					Do(context.Background())

				require.NotNil(t, err)
				assert.False(t, exists)
			}
		})
	})
}
