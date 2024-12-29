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

package test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	eschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	uschema "github.com/weaviate/weaviate/usecases/schema"
)

var verbose = verbosity.OutputVerbose

func TestCreateTenants(t *testing.T) {
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}

	t.Run("Create tenant", func(Z *testing.T) {
		expectedTenants := []string{
			"Tenant1", "Tenant2", "Tenant3",
		}

		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)

		tenants := make([]*models.Tenant, len(expectedTenants))
		for i := range tenants {
			tenants[i] = &models.Tenant{
				Name:           expectedTenants[i],
				ActivityStatus: models.TenantActivityStatusHOT,
			}
		}
		helper.CreateTenants(t, testClass.Class, tenants)

		respGet, errGet := helper.GetTenants(t, testClass.Class)
		require.Nil(t, errGet)
		require.NotNil(t, respGet)
		require.ElementsMatch(t, respGet.Payload, tenants)

		for _, tenant := range expectedTenants {
			resp, err := helper.TenantExists(t, testClass.Class, tenant)
			require.Nil(t, err)
			require.True(t, resp.IsSuccess())
		}

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 3)

		var foundTenants []string
		for _, found := range resp.Payload.Nodes[0].Shards {
			assert.Equal(t, testClass.Class, found.Class)
			// Creating a tenant alone should not result in a loaded shard.
			// This check also ensures that the nods api did not cause a
			// force load.
			assert.False(t, found.Loaded)
			foundTenants = append(foundTenants, found.Name)
		}
		assert.ElementsMatch(t, expectedTenants, foundTenants)
	})

	t.Run("Create duplicate tenant once", func(t *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)
		err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "DoubleTenant"}, {Name: "DoubleTenant"}})
		require.NotNil(t, err)

		err = helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "DoubleTenant"}})
		require.Nil(t, err)
		// only added once
		respGet, errGet := helper.GetTenants(t, testClass.Class)
		require.Nil(t, errGet)
		require.NotNil(t, respGet)
		require.Len(t, respGet.Payload, 1)
	})

	t.Run("Create same tenant multiple times", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)
		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: "AddTenantAgain"}})

		// idempotent operation
		err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "AddTenantAgain"}})
		require.Nil(t, err)
	})

	t.Run("Fail to create tenant with forbidden activity status:", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)

		activityStatuses := []string{
			models.TenantActivityStatusFROZEN,
			models.TenantActivityStatusFREEZING,
			models.TenantActivityStatusUNFREEZING,
		}
		for _, activityStatus := range activityStatuses {
			Z.Run(activityStatus, func(z *testing.T) {
				err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: activityStatus}})
				require.NotNil(t, err)
			})
		}
	})

	t.Run("Create and update more than 100 tenant", func(Z *testing.T) {
		expectedTenants := make([]string, 101)

		for idx := 0; idx < 101; idx++ {
			expectedTenants[idx] = fmt.Sprintf("Tenant%d", idx)
		}

		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()

		helper.CreateClass(t, &testClass)

		tenants := make([]*models.Tenant, len(expectedTenants))

		for i := range tenants {
			tenants[i] = &models.Tenant{
				Name:           expectedTenants[i],
				ActivityStatus: models.TenantActivityStatusCOLD,
			}
		}

		err := helper.CreateTenantsReturnError(t, testClass.Class, tenants)
		require.Nil(t, err)

		err = helper.UpdateTenantsReturnError(t, testClass.Class, tenants)
		require.NotNil(t, err)
		ee := &eschema.TenantsUpdateUnprocessableEntity{}
		require.True(t, errors.As(err, &ee))
		require.Equal(t, uschema.ErrMsgMaxAllowedTenants, ee.Payload.Error[0].Message)
	})

	t.Run("Create same tenant with different status", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()

		helper.CreateClass(t, &testClass)

		err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{
			{
				Name:           "Tenant1",
				ActivityStatus: models.TenantActivityStatusCOLD,
			},
			{
				Name:           "Tenant1",
				ActivityStatus: models.TenantActivityStatusHOT,
			},
		})
		require.NotNil(t, err)
		ee := &eschema.TenantsCreateUnprocessableEntity{}
		as := errors.As(err, &ee)
		require.True(t, as)
		require.Contains(t, ee.Payload.Error[0].Message, "existed multiple times")
	})
}

func TestDeleteTenants(t *testing.T) {
	testClass := models.Class{
		Class:              "MultiTenantClassDelete",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	helper.CreateClass(t, &testClass)

	tenants := []*models.Tenant{
		{Name: "tenant1"},
		{Name: "tenant2"},
		{Name: "tenant3"},
		{Name: "tenant4"},
	}
	helper.CreateTenants(t, testClass.Class, tenants)

	t.Run("Delete same tenant multiple times", func(t *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant4"})
		require.Nil(t, err)

		// deleted once
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		for _, shard := range resp.Payload.Nodes[0].Shards {
			// Creating a tenant alone should not result in a loaded shard.
			// This check also ensures that the nods api did not cause a
			// force load.
			assert.False(t, shard.Loaded)
			assert.NotEqual(t, "tenant4", shard.Name)
		}
		respExist, errExist := helper.TenantExists(t, testClass.Class, "tenant4")
		require.Nil(t, respExist)
		require.NotNil(t, errExist)

		// idempotent operation
		err = helper.DeleteTenants(t, testClass.Class, []string{"tenant4"})
		require.Nil(t, err)
	})

	t.Run("Delete duplicate tenant once", func(Z *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant1", "tenant1"})
		// idempotent operation
		require.Nil(t, err)

		// deleted once
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 2)
	})

	t.Run("Delete non-existent tenant alongside existing", func(Z *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant1", "tenant5"})
		require.Nil(t, err)

		// idempotent - deleting multiple times works - tenant1 is removed
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 2)
	})

	t.Run("Delete tenants", func(Z *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant1", "tenant3"})
		require.Nil(t, err)

		// successfully deleted
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 1)
	})
}

func TestTenantsNonMultiTenant(t *testing.T) {
	testClass := models.Class{
		Class: "TenantsNoMultiClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: false,
		},
	}
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	helper.CreateClass(t, &testClass)

	err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "doesNotMatter"}})
	require.NotNil(t, err)

	_, err = helper.GetTenants(t, testClass.Class)
	require.NotNil(t, err)

	err = helper.DeleteTenants(t, testClass.Class, []string{"doesNotMatter"})
	require.NotNil(t, err)
}

func TestTenantsClassDoesNotExist(t *testing.T) {
	err := helper.CreateTenantsReturnError(t, "DoesNotExist", []*models.Tenant{{Name: "doesNotMatter"}})
	require.NotNil(t, err)

	_, err = helper.GetTenants(t, "DoesNotExist")
	require.NotNil(t, err)

	_, err = helper.TenantExists(t, "DoesNotExist", "SomeTenant")
	require.NotNil(t, err)

	err = helper.DeleteTenants(t, "DoesNotExist", []string{"doesNotMatter"})
	require.NotNil(t, err)
}

// Testing of tenant updating from HOT/COLD to FROZEN is handled in test/modules/offload-s3
func TestUpdateTenants(t *testing.T) {
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}

	t.Run("Update tenant to COLD from HOT", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)

		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: models.TenantActivityStatusHOT}})

		err := helper.UpdateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: models.TenantActivityStatusCOLD}})
		require.Nil(t, err)
	})

	t.Run("Update tenant to HOT from COLD", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)

		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: models.TenantActivityStatusCOLD}})

		err := helper.UpdateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: models.TenantActivityStatusHOT}})
		require.Nil(t, err)
	})

	t.Run("Fail to update tenant with forbidden activity status:", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)

		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: models.TenantActivityStatusHOT}})

		activityStatuses := []string{
			models.TenantActivityStatusFREEZING,
			models.TenantActivityStatusUNFREEZING,
		}
		for _, activityStatus := range activityStatuses {
			Z.Run(activityStatus, func(z *testing.T) {
				err := helper.UpdateTenantsReturnError(t, testClass.Class, []*models.Tenant{{Name: "tenant", ActivityStatus: activityStatus}})
				require.NotNil(t, err)
			})
		}
	})
}
