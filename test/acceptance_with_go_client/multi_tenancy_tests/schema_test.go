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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSchema_MultiTenancyConfig(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("class with MT config - MT enabled", func(t *testing.T) {
		defer cleanup()

		className := "MultiTenantClass"
		schemaClass := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "someProperty",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		}

		err := client.Schema().ClassCreator().
			WithClass(schemaClass).
			Do(context.Background())
		require.Nil(t, err)

		t.Run("verify class created", func(t *testing.T) {
			loadedClass, err := client.Schema().ClassGetter().WithClassName(className).Do(context.Background())
			require.Nil(t, err)
			require.NotNil(t, loadedClass.MultiTenancyConfig)
			assert.Equal(t, true, loadedClass.MultiTenancyConfig.Enabled)
		})
	})

	t.Run("class with MT config - MT disabled", func(t *testing.T) {
		defer cleanup()

		className := "MultiTenantClassDisabled"
		schemaClass := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "someProperty",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: false,
			},
		}

		err := client.Schema().ClassCreator().
			WithClass(schemaClass).
			Do(context.Background())
		require.Nil(t, err)

		t.Run("verify class created", func(t *testing.T) {
			loadedClass, err := client.Schema().ClassGetter().WithClassName(className).Do(context.Background())
			require.Nil(t, err)
			require.NotNil(t, loadedClass.MultiTenancyConfig)
			assert.Equal(t, false, loadedClass.MultiTenancyConfig.Enabled)
		})
	})

	t.Run("class without MT config", func(t *testing.T) {
		defer cleanup()

		className := "NonMultiTenantClass"
		schemaClass := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "someProperty",
					DataType: schema.DataTypeText.PropString(),
				},
			},
		}

		err := client.Schema().ClassCreator().
			WithClass(schemaClass).
			Do(context.Background())
		require.Nil(t, err)

		t.Run("verify class created", func(t *testing.T) {
			loadedClass, err := client.Schema().ClassGetter().WithClassName(className).Do(context.Background())
			require.Nil(t, err)
			assert.NotNil(t, loadedClass.MultiTenancyConfig)
			assert.False(t, loadedClass.MultiTenancyConfig.Enabled)
		})
	})
}

func TestSchema_Tenants(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	className := "Pizza"

	t.Run("adds tenants to MT class", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaPizzaForTenants(t, client)

		t.Run("adds single tenant", func(t *testing.T) {
			tenant := models.Tenant{
				Name: "tenantNo1",
			}

			err := client.Schema().TenantsCreator().
				WithClassName(className).
				WithTenants(tenant).
				Do(context.Background())

			require.Nil(t, err)
		})

		t.Run("adds multiple tenants", func(t *testing.T) {
			tenants := fixtures.Tenants{
				{Name: "tenantNo2"},
				{Name: "tenantNo3"},
			}

			err := client.Schema().TenantsCreator().
				WithClassName(className).
				WithTenants(tenants...).
				Do(context.Background())

			require.Nil(t, err)
		})
	})

	t.Run("fails adding tenants to non-MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizza(t, client)

		err := client.Schema().TenantsCreator().
			WithClassName(className).
			WithTenants(tenants...).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "multi-tenancy is not enabled for class")
	})

	t.Run("gets tenants of MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		gotTenants, err := client.Schema().TenantsGetter().
			WithClassName(className).
			Do(context.Background())

		require.Nil(t, err)
		require.Len(t, gotTenants, len(tenants))

		assert.ElementsMatch(t, tenants.Names(), fixtures.Tenants(gotTenants).Names())
	})

	t.Run("fails getting tenants from non-MT class", func(t *testing.T) {
		defer cleanup()

		fixtures.CreateSchemaPizza(t, client)

		gotTenants, err := client.Schema().TenantsGetter().
			WithClassName(className).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "multi-tenancy is not enabled for class")
		require.Nil(t, gotTenants)
	})

	t.Run("updates tenants of MT class", func(t *testing.T) {
		defer cleanup()

		tenants := []models.Tenant{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		t.Run("fails updating non existent tenant", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           "nonExistentTenant",
					ActivityStatus: models.TenantActivityStatusCOLD,
				}).Do(context.Background())

			require.NotNil(t, err)
			clientErr := err.(*fault.WeaviateClientError)
			assert.Equal(t, 422, clientErr.StatusCode)
			assert.Contains(t, clientErr.Msg, "not found")
		})

		t.Run("updates existent tenants", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(
					models.Tenant{
						Name:           tenants[0].Name,
						ActivityStatus: models.TenantActivityStatusCOLD,
					},
					models.Tenant{
						Name:           tenants[1].Name,
						ActivityStatus: models.TenantActivityStatusCOLD,
					},
				).Do(context.Background())

			require.Nil(t, err)
		})
	})

	t.Run("fails updating tenants of non-MT class", func(t *testing.T) {
		defer cleanup()

		tenants := []models.Tenant{
			{
				Name:           "tenantNo1",
				ActivityStatus: models.TenantActivityStatusCOLD,
			},
			{
				Name:           "tenantNo2",
				ActivityStatus: models.TenantActivityStatusCOLD,
			},
		}

		fixtures.CreateSchemaPizza(t, client)

		err := client.Schema().TenantsUpdater().
			WithClassName(className).
			WithTenants(tenants...).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "multi-tenancy is not enabled for class")
	})

	t.Run("deletes tenants from MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
			{Name: "tenantNo3"},
		}

		fixtures.CreateSchemaPizzaForTenants(t, client)
		fixtures.CreateTenantsPizza(t, client, tenants...)

		t.Run("does not error on deleting non existent tenant", func(t *testing.T) {
			err := client.Schema().TenantsDeleter().
				WithClassName(className).
				WithTenants(tenants[0].Name, "nonExistentTenant").
				Do(context.Background())

			require.Nil(t, err)
		})

		t.Run("deletes multiple tenants", func(t *testing.T) {
			err := client.Schema().TenantsDeleter().
				WithClassName(className).
				WithTenants(tenants[1:].Names()...).
				Do(context.Background())

			require.Nil(t, err)
		})
	})

	t.Run("fails deleting tenants from non-MT class", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{Name: "tenantNo1"},
			{Name: "tenantNo2"},
		}

		fixtures.CreateSchemaPizza(t, client)

		err := client.Schema().TenantsDeleter().
			WithClassName(className).
			WithTenants(tenants.Names()...).
			Do(context.Background())

		require.NotNil(t, err)
		clientErr := err.(*fault.WeaviateClientError)
		assert.Equal(t, 422, clientErr.StatusCode)
		assert.Contains(t, clientErr.Msg, "multi-tenancy is not enabled for class")
	})
}
