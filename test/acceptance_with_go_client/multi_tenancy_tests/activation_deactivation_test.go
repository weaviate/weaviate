package multi_tenancy_tests

import (
	"acceptance_tests_with_client/fixtures"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
)

func TestActivationDeactivation(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("deactivate / activate journey", func(t *testing.T) {
		defer cleanup()

		tenants := fixtures.Tenants{
			{
				Name: "tenantNo1",
				// default status HOT
			},
			{
				Name:           "tenantNo2",
				ActivityStatus: models.TenantActivityStatusHOT,
			},
			{
				Name:           "tenantNo3",
				ActivityStatus: models.TenantActivityStatusCOLD,
			},
		}
		className := "Pizza"
		ctx := context.Background()
		ids := fixtures.IdsByClass[className]

		t.Run("create tenants (1,2,3), populate active tenants (1,2)", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenants...)
			fixtures.CreateDataPizzaForTenants(t, client, tenants[:2].Names()...)

			assertTenantActive(t, client, className, tenants[0].Name)
			assertTenantActive(t, client, className, tenants[1].Name)
			assertTenantInactive(t, client, className, tenants[2].Name)

			assertActiveTenantObjects(t, client, className, tenants[0].Name, ids)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, ids)
			assertInactiveTenantObjects(t, client, className, tenants[2].Name)
		})

		t.Run("deactivate tenant (1)", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           tenants[0].Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}).
				Do(ctx)
			require.Nil(t, err)

			assertTenantInactive(t, client, className, tenants[0].Name)
			assertTenantActive(t, client, className, tenants[1].Name)
			assertTenantInactive(t, client, className, tenants[2].Name)

			assertInactiveTenantObjects(t, client, className, tenants[0].Name)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, ids)
			assertInactiveTenantObjects(t, client, className, tenants[2].Name)
		})

		t.Run("activate and populate tenant (3)", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           tenants[2].Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}).
				Do(ctx)
			require.Nil(t, err)

			fixtures.CreateDataPizzaForTenants(t, client, tenants[2].Name)

			assertTenantInactive(t, client, className, tenants[0].Name)
			assertTenantActive(t, client, className, tenants[1].Name)
			assertTenantActive(t, client, className, tenants[2].Name)

			assertInactiveTenantObjects(t, client, className, tenants[0].Name)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, ids)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, ids)
		})

		t.Run("activate tenant (1)", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           tenants[0].Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}).
				Do(ctx)
			require.Nil(t, err)

			assertTenantActive(t, client, className, tenants[0].Name)
			assertTenantActive(t, client, className, tenants[1].Name)
			assertTenantActive(t, client, className, tenants[2].Name)

			assertActiveTenantObjects(t, client, className, tenants[0].Name, ids)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, ids)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, ids)
		})

		t.Run("deactivate tenant (2)", func(t *testing.T) {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           tenants[1].Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}).
				Do(ctx)
			require.Nil(t, err)

			assertTenantActive(t, client, className, tenants[0].Name)
			assertTenantInactive(t, client, className, tenants[1].Name)
			assertTenantActive(t, client, className, tenants[2].Name)

			assertActiveTenantObjects(t, client, className, tenants[0].Name, ids)
			assertInactiveTenantObjects(t, client, className, tenants[1].Name)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, ids)
		})

		t.Run("delete tenants", func(t *testing.T) {
			err := client.Schema().TenantsDeleter().
				WithClassName(className).
				WithTenants(tenants.Names()...).
				Do(ctx)

			require.Nil(t, err)
		})
	})
}

func assertTenantActive(t *testing.T, client *wvt.Client, className, tenantName string) {
	gotTenants, err := client.Schema().TenantsGetter().
		WithClassName(className).
		Do(context.Background())
	require.Nil(t, err)
	require.NotEmpty(t, gotTenants)

	byName := fixtures.Tenants(gotTenants).ByName(tenantName)
	require.NotNil(t, byName)
	require.Equal(t, models.TenantActivityStatusHOT, byName.ActivityStatus)
}

func assertTenantInactive(t *testing.T, client *wvt.Client, className, tenantName string) {
	gotTenants, err := client.Schema().TenantsGetter().
		WithClassName(className).
		Do(context.Background())
	require.Nil(t, err)
	require.NotEmpty(t, gotTenants)

	byName := fixtures.Tenants(gotTenants).ByName(tenantName)
	require.NotNil(t, byName)
	require.Equal(t, models.TenantActivityStatusCOLD, byName.ActivityStatus)
}

func assertActiveTenantObjects(t *testing.T, client *wvt.Client, className, tenantName string, expectedIds []string) {
	objects, err := client.Data().ObjectsGetter().
		WithClassName(className).
		WithTenant(tenantName).
		Do(context.Background())

	require.Nil(t, err)
	require.NotNil(t, objects)
	require.Len(t, objects, len(expectedIds))

	ids := make([]string, len(objects))
	for i, object := range objects {
		ids[i] = string(object.ID)
	}
	assert.ElementsMatch(t, expectedIds, ids)
}

func assertInactiveTenantObjects(t *testing.T, client *wvt.Client, className, tenantName string) {
	objects, err := client.Data().ObjectsGetter().
		WithClassName(className).
		WithTenant(tenantName).
		Do(context.Background())

	require.NotNil(t, err)
	clientErr := err.(*fault.WeaviateClientError)
	assert.Equal(t, 422, clientErr.StatusCode)
	assert.Contains(t, clientErr.Msg, "tenant not active")
	require.Nil(t, objects)
}
