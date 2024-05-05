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
	"time"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

func autoTenantActivationJourney(t *testing.T,
	composeFn func(t *testing.T, ctx context.Context) (
		client *wvt.Client,
		cleanupFn func(t *testing.T, ctx context.Context),
	),
	replicationFactor int64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, composeCleanup := composeFn(t, ctx)
	defer composeCleanup(t, ctx)

	className := "Pizza"
	tenantNames := []string{"t1", "t2", "t3", "t4", "t5", "t6"}
	tenantsFromNames := func(names []string, status string) []models.Tenant {
		tenants := make([]models.Tenant, len(names))
		for i, name := range names {
			tenants[i] = models.Tenant{
				Name:           name,
				ActivityStatus: status,
			}
		}
		return tenants
	}

	t.Run("create collection with implicit activation deactivated", func(t *testing.T) {
		// The schema contains default values for multi-tenancy config which means
		// implicit activation is off.
		fixtures.CreateSchemaPizzaForTenantsWithReplication(t, client, replicationFactor)
		fixtures.CreateTenantsPizza(t, client,
			tenantsFromNames(tenantNames, models.TenantActivityStatusHOT)...)
	})

	t.Run("import data while tenants are hot", func(t *testing.T) {
		fixtures.CreateDataPizzaFruttiDiMareForTenants(t, client, tenantNames...)
	})

	t.Run("verify data is queryable while tenants are hot", func(t *testing.T) {
		for _, tenantName := range tenantNames {
			res, err := client.GraphQL().
				Get().WithClassName(className).WithBM25(
				client.GraphQL().Bm25ArgBuilder().WithQuery("squid"),
			).WithTenant(tenantName).WithFields(graphql.Field{Name: "name"}).WithLimit(10).Do(ctx)
			require.Nil(t, err)
			assert.Equal(t, 1, len(res.Data["Get"].(map[string]any)[className].([]any)))
		}
	})

	turnTenantsColdWaitAndVerify(t, client, className, tenantNames, true)

	t.Run("allow implicit activation now", func(t *testing.T) {
		c, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
		require.Nil(t, err)

		c.MultiTenancyConfig.AutoTenantActivation = true
		err = client.Schema().ClassUpdater().WithClass(c).Do(ctx)
		require.Nil(t, err)

		// These kind of schema changes are not expected to be strongly
		// consistent, so we wait a bit. This is fine as this setting should very
		// rarely change.
		time.Sleep(1 * time.Second)
	})

	t.Run("assert all tenants are queryable despite originally being COLD", func(t *testing.T) {
		for _, tenantName := range tenantNames {
			assertActiveTenantObjects(t, client, className, tenantName,
				[]string{fixtures.PIZZA_FRUTTI_DI_MARE_ID})
		}
	})

	t.Run("assert all tenants are reporting as HOT now", func(t *testing.T) {
		// they were implicitly activated as a side-effect of the last query
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			for _, tenantName := range tenantNames {
				assertTenantActiveNoRequire(t, client, className, tenantName)
			}
		}, 5*time.Second, 100*time.Millisecond)
	})

	turnTenantsColdWaitAndVerify(t, client, className, tenantNames, false)

	t.Run("try to import more data into cold tenants", func(t *testing.T) {
		// This should work because auto activation is enabled
		fixtures.CreateDataPizzaQuattroFormaggiForTenants(t, client, tenantNames...)
	})

	t.Run("assert all tenants are reporting as HOT now", func(t *testing.T) {
		// they were implicitly activated as a side-effect of the last import
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			for _, tenantName := range tenantNames {
				assertTenantActiveNoRequire(t, client, className, tenantName)
			}
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("assert all tenants are queryable and the new data is present", func(t *testing.T) {
		for _, tenantName := range tenantNames {
			assertActiveTenantObjectsNoRequire(t, client, className, tenantName,
				[]string{
					fixtures.PIZZA_FRUTTI_DI_MARE_ID,
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
				})
		}
	})
}

func TestImplicitActivation(t *testing.T) {
	composeFn := func(t *testing.T, ctx context.Context) (
		client *wvt.Client,
		cleanupFn func(t *testing.T, ctx context.Context),
	) {
		compose, err := docker.New().WithWeaviateCluster().Start(ctx)
		require.Nil(t, err)

		client, err = wvt.NewClient(wvt.Config{Scheme: "http", Host: compose.ContainerURI(0)})
		require.Nil(t, err)

		cleanupFn = func(t *testing.T, ctx context.Context) {
			err := compose.Terminate(ctx)
			require.Nil(t, err)
		}

		return
	}

	t.Run("without replication", func(t *testing.T) {
		autoTenantActivationJourney(t, composeFn, 1)
	})

	t.Run("with replication", func(t *testing.T) {
		autoTenantActivationJourney(t, composeFn, 2)
	})
}

func turnTenantsColdWaitAndVerify(t *testing.T, client *wvt.Client, className string, tenantNames []string, attemptQuery bool) {
	t.Run("turn tenants cold", func(t *testing.T) {
		for _, tenantName := range tenantNames {
			err := client.Schema().TenantsUpdater().
				WithClassName(className).
				WithTenants(models.Tenant{
					Name:           tenantName,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}).
				Do(context.Background())
			require.Nil(t, err)
		}
	})

	t.Run("assert all tenants are COLD", func(t *testing.T) {
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			for _, tenantName := range tenantNames {
				assertTenantInactiveNoRequire(t, client, className, tenantName)
				if attemptQuery {
					// we can only attempt to query when implicit tenant activation is
					// off, otherwise the attempt to query would implicitly activate the
					// tenant again ;-)
					assertInactiveTenantObjectsNoRequire(t, client, className, tenantName)
				}
			}
		}, 5*time.Second, 100*time.Millisecond)
	})
}

// assertTenantActiveNoRequire is a copy of assertTenantActive that is modified
// to work with assert.EventuallyWithT. For this it can't use require.
func assertTenantActiveNoRequire(t assert.TestingT, client *wvt.Client, className, tenantName string) {
	gotTenants, err := client.Schema().TenantsGetter().
		WithClassName(className).
		Do(context.Background())
	assert.Nil(t, err)
	assert.NotEmpty(t, gotTenants)

	byName := fixtures.Tenants(gotTenants).ByName(tenantName)
	assert.NotNil(t, byName)
	assert.Equal(t, models.TenantActivityStatusHOT, byName.ActivityStatus)
}

// assertTenantInactiveNoRequire is a copy of assertTenantInactive that is
// modified to work with assert.EventuallyWithT. For this it can't use require.
func assertTenantInactiveNoRequire(t assert.TestingT, client *wvt.Client, className, tenantName string) {
	gotTenants, err := client.Schema().TenantsGetter().
		WithClassName(className).
		Do(context.Background())
	assert.Nil(t, err)
	assert.NotEmpty(t, gotTenants)

	byName := fixtures.Tenants(gotTenants).ByName(tenantName)
	assert.NotNil(t, byName)
	assert.Equal(t, models.TenantActivityStatusCOLD, byName.ActivityStatus)
}

// assertActiveTenantObjectsNoRequire is a copy of assertActiveTenantObjects that
// is modified to work with assert.EventuallyWithT. For this it can't use require.
func assertActiveTenantObjectsNoRequire(t assert.TestingT, client *wvt.Client, className, tenantName string, expectedIds []string) {
	objects, err := client.Data().ObjectsGetter().
		WithClassName(className).
		WithTenant(tenantName).
		Do(context.Background())
	if err != nil {
		assert.Fail(t, "unexpected error", err)
		return
	}

	assert.NotNil(t, objects)
	assert.Len(t, objects, len(expectedIds))

	ids := make([]string, len(objects))
	for i, object := range objects {
		ids[i] = string(object.ID)
	}
	assert.ElementsMatch(t, expectedIds, ids)
}

// assertInactiveTenantObjectsNoRequire is a copy of assertInactiveTenantObjects
// that is modified to work with assert.EventuallyWithT. For this it can't use
// require.
func assertInactiveTenantObjectsNoRequire(t assert.TestingT, client *wvt.Client, className, tenantName string) {
	objects, err := client.Data().ObjectsGetter().
		WithClassName(className).
		WithTenant(tenantName).
		Do(context.Background())

	if err == nil {
		assert.Fail(t, "expected a client error")
		return
	}
	clientErr := err.(*fault.WeaviateClientError)
	assert.Equal(t, 422, clientErr.StatusCode)
	assert.Contains(t, clientErr.Msg, "tenant not active")
	assert.Nil(t, objects)
}
