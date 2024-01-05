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
	"time"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
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
		pizzaIds := fixtures.IdsByClass[className]

		t.Run("create tenants (1,2,3), populate active tenants (1,2)", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenants...)
			fixtures.CreateDataPizzaForTenants(t, client, tenants[:2].Names()...)

			assertTenantActive(t, client, className, tenants[0].Name)
			assertTenantActive(t, client, className, tenants[1].Name)
			assertTenantInactive(t, client, className, tenants[2].Name)

			assertActiveTenantObjects(t, client, className, tenants[0].Name, pizzaIds)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, pizzaIds)
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
			assertActiveTenantObjects(t, client, className, tenants[1].Name, pizzaIds)
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
			assertActiveTenantObjects(t, client, className, tenants[1].Name, pizzaIds)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, pizzaIds)
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

			assertActiveTenantObjects(t, client, className, tenants[0].Name, pizzaIds)
			assertActiveTenantObjects(t, client, className, tenants[1].Name, pizzaIds)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, pizzaIds)
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

			assertActiveTenantObjects(t, client, className, tenants[0].Name, pizzaIds)
			assertInactiveTenantObjects(t, client, className, tenants[1].Name)
			assertActiveTenantObjects(t, client, className, tenants[2].Name, pizzaIds)
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

type composeFn func(t *testing.T, ctx context.Context) (
	client *wvt.Client,
	cleanupFn func(t *testing.T, ctx context.Context),
	restartFn func(t *testing.T, ctx context.Context) *wvt.Client)

func TestActivationDeactivation_Restarts(t *testing.T) {
	t.Run("single node", func(t *testing.T) {
		composeFn := func(t *testing.T, ctx context.Context) (
			client *wvt.Client,
			cleanupFn func(t *testing.T, ctx context.Context),
			restartFn func(t *testing.T, ctx context.Context) *wvt.Client,
		) {
			compose, err := docker.New().WithWeaviate().Start(ctx)
			require.Nil(t, err)

			container := compose.GetWeaviate()
			client, err = wvt.NewClient(wvt.Config{Scheme: "http", Host: container.URI()})
			require.Nil(t, err)

			cleanupFn = func(t *testing.T, ctx context.Context) {
				err := compose.Terminate(ctx)
				require.Nil(t, err)
			}

			restartFn = func(t *testing.T, ctx context.Context) *wvt.Client {
				require.Nil(t, compose.Stop(ctx, container.Name(), nil))
				require.Nil(t, compose.Start(ctx, container.Name()))

				client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: container.URI()})
				require.Nil(t, err)

				return client
			}

			return
		}

		testActivationDeactivationWithRestarts(t, composeFn)
	})

	t.Run("multiple nodes", func(t *testing.T) {
		composeFn := func(t *testing.T, ctx context.Context) (
			client *wvt.Client,
			cleanupFn func(t *testing.T, ctx context.Context),
			restartFn func(t *testing.T, ctx context.Context) *wvt.Client,
		) {
			compose, err := docker.New().WithWeaviateCluster().Start(ctx)
			require.Nil(t, err)

			container1 := compose.GetWeaviate()
			container2 := compose.GetWeaviateNode2()
			client, err = wvt.NewClient(wvt.Config{Scheme: "http", Host: container1.URI()})
			require.Nil(t, err)

			cleanupFn = func(t *testing.T, ctx context.Context) {
				err := compose.Terminate(ctx)
				require.Nil(t, err)
			}

			restartFn = func(t *testing.T, ctx context.Context) *wvt.Client {
				require.Nil(t, compose.Stop(ctx, container1.Name(), nil))
				require.Nil(t, compose.Start(ctx, container1.Name()))
				require.Nil(t, compose.Stop(ctx, container2.Name(), nil))
				require.Nil(t, compose.Start(ctx, container2.Name()))

				client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: container1.URI()})
				require.Nil(t, err)

				return client
			}

			return
		}

		testActivationDeactivationWithRestarts(t, composeFn)
	})
}

func testActivationDeactivationWithRestarts(t *testing.T, composeFn composeFn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	client, composeCleanup, restart := composeFn(t, ctx)
	defer composeCleanup(t, ctx)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	classPizza := "Pizza"
	classSoup := "Soup"

	t.Run("deactivate / activate journey", func(t *testing.T) {
		defer cleanup()

		createTenants := func(className string, groupId, count int, status string) fixtures.Tenants {
			tenants := make(fixtures.Tenants, count)
			for i := 0; i < count; i++ {
				tenants[i] = models.Tenant{
					Name:           fmt.Sprintf("tenant_%s_%d_%d", className, groupId, i),
					ActivityStatus: status,
				}
			}
			return tenants
		}
		assertActiveTenants := func(t *testing.T, tenants fixtures.Tenants, className string, expectedIds []string) {
			for _, tenant := range tenants {
				assertTenantActive(t, client, className, tenant.Name)
				assertActiveTenantObjects(t, client, className, tenant.Name, expectedIds)
			}
		}
		assertInactiveTenants := func(t *testing.T, tenants fixtures.Tenants, className string) {
			for _, tenant := range tenants {
				assertTenantInactive(t, client, className, tenant.Name)
				assertInactiveTenantObjects(t, client, className, tenant.Name)
			}
		}

		tenants1Pizza := createTenants(classPizza, 1, 5, "") // default status HOT
		tenants2Pizza := createTenants(classPizza, 2, 4, models.TenantActivityStatusHOT)
		tenants3Pizza := createTenants(classPizza, 3, 3, models.TenantActivityStatusCOLD)
		tenants1Soup := createTenants(classSoup, 1, 4, "") // default status HOT
		tenants2Soup := createTenants(classSoup, 2, 3, models.TenantActivityStatusHOT)
		tenants3Soup := createTenants(classSoup, 3, 2, models.TenantActivityStatusCOLD)
		idsPizza := fixtures.IdsByClass[classPizza]
		idsSoup := fixtures.IdsByClass[classSoup]

		t.Run("create tenants (1,2,3), populate active tenants (1,2)", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenants1Pizza...)
			fixtures.CreateTenantsPizza(t, client, tenants2Pizza...)
			fixtures.CreateTenantsPizza(t, client, tenants3Pizza...)
			fixtures.CreateDataPizzaForTenants(t, client, tenants1Pizza.Names()...)
			fixtures.CreateDataPizzaForTenants(t, client, tenants2Pizza.Names()...)

			fixtures.CreateSchemaSoupForTenants(t, client)
			fixtures.CreateTenantsSoup(t, client, tenants1Soup...)
			fixtures.CreateTenantsSoup(t, client, tenants2Soup...)
			fixtures.CreateTenantsSoup(t, client, tenants3Soup...)
			fixtures.CreateDataSoupForTenants(t, client, tenants1Soup.Names()...)
			fixtures.CreateDataSoupForTenants(t, client, tenants2Soup.Names()...)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants3Pizza, classPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants3Soup, classSoup)
		})

		t.Run("deactivate tenants (1)", func(t *testing.T) {
			tenants := make(fixtures.Tenants, len(tenants1Pizza))
			for i, tenant := range tenants1Pizza {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			tenants = make(fixtures.Tenants, len(tenants1Soup))
			for i, tenant := range tenants1Soup {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			assertInactiveTenants(t, tenants1Pizza, classPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants3Pizza, classPizza)

			assertInactiveTenants(t, tenants1Soup, classSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants3Soup, classSoup)
		})

		t.Run("restart db, nothing changed", func(t *testing.T) {
			client = restart(t, ctx)

			assertInactiveTenants(t, tenants1Pizza, classPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants3Pizza, classPizza)

			assertInactiveTenants(t, tenants1Soup, classSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants3Soup, classSoup)
		})

		t.Run("activate and populate tenants (3)", func(t *testing.T) {
			tenants := make(fixtures.Tenants, len(tenants3Pizza))
			for i, tenant := range tenants3Pizza {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			fixtures.CreateDataPizzaForTenants(t, client, tenants3Pizza.Names()...)

			tenants = make(fixtures.Tenants, len(tenants3Soup))
			for i, tenant := range tenants3Soup {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			fixtures.CreateDataSoupForTenants(t, client, tenants3Soup.Names()...)

			assertInactiveTenants(t, tenants1Pizza, classPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertInactiveTenants(t, tenants1Soup, classSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("activate tenants (1)", func(t *testing.T) {
			tenants := make(fixtures.Tenants, len(tenants1Pizza))
			for i, tenant := range tenants1Pizza {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			tenants = make(fixtures.Tenants, len(tenants1Soup))
			for i, tenant := range tenants1Soup {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("deactivate tenants (2)", func(t *testing.T) {
			tenants := make(fixtures.Tenants, len(tenants2Pizza))
			for i, tenant := range tenants2Pizza {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			tenants = make(fixtures.Tenants, len(tenants2Soup))
			for i, tenant := range tenants2Soup {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				}
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants2Pizza, classPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants2Soup, classSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("restart db, nothing changed", func(t *testing.T) {
			client = restart(t, ctx)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants2Pizza, classPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants2Soup, classSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("activate already active (1,3), deactivate already inactive (2), nothing changed", func(t *testing.T) {
			tenants := make(fixtures.Tenants, 0, len(tenants1Pizza)+len(tenants2Pizza)+len(tenants3Pizza))
			for _, tenant := range tenants1Pizza {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				})
			}
			for _, tenant := range tenants2Pizza {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				})
			}
			for _, tenant := range tenants3Pizza {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				})
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			tenants = make(fixtures.Tenants, 0, len(tenants1Soup)+len(tenants2Soup)+len(tenants3Soup))
			for _, tenant := range tenants1Soup {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				})
			}
			for _, tenant := range tenants2Soup {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusCOLD,
				})
			}
			for _, tenant := range tenants3Soup {
				tenants = append(tenants, models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				})
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertInactiveTenants(t, tenants2Pizza, classPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertInactiveTenants(t, tenants2Soup, classSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("activate tenants (2)", func(t *testing.T) {
			tenants := make(fixtures.Tenants, len(tenants2Pizza))
			for i, tenant := range tenants2Pizza {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err := client.Schema().TenantsUpdater().
				WithClassName(classPizza).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			tenants = make(fixtures.Tenants, len(tenants2Soup))
			for i, tenant := range tenants2Soup {
				tenants[i] = models.Tenant{
					Name:           tenant.Name,
					ActivityStatus: models.TenantActivityStatusHOT,
				}
			}

			err = client.Schema().TenantsUpdater().
				WithClassName(classSoup).
				WithTenants(tenants...).
				Do(ctx)
			require.Nil(t, err)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
		})

		t.Run("restart db, nothing changed", func(t *testing.T) {
			client = restart(t, ctx)

			assertActiveTenants(t, tenants1Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants2Pizza, classPizza, idsPizza)
			assertActiveTenants(t, tenants3Pizza, classPizza, idsPizza)

			assertActiveTenants(t, tenants1Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants2Soup, classSoup, idsSoup)
			assertActiveTenants(t, tenants3Soup, classSoup, idsSoup)
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
