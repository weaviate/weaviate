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

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

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
			tenants[i] = &models.Tenant{Name: expectedTenants[i]}
		}
		helper.CreateTenants(t, testClass.Class, tenants)

		respGet, errGet := helper.GetTenants(t, testClass.Class)
		require.Nil(t, errGet)
		require.NotNil(t, respGet)
		require.ElementsMatch(t, respGet.Payload, tenants)

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 3)

		var foundTenants []string
		for _, found := range resp.Payload.Nodes[0].Shards {
			assert.Equal(t, testClass.Class, found.Class)
			foundTenants = append(foundTenants, found.Name)
		}
		assert.ElementsMatch(t, expectedTenants, foundTenants)
	})

	t.Run("Create duplicate tenant multiple times", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)
		err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{"DoubleTenant"}, {"DoubleTenant"}})
		require.NotNil(t, err)
	})

	t.Run("Create same tenant multiple times", func(Z *testing.T) {
		defer func() {
			helper.DeleteClass(t, testClass.Class)
		}()
		helper.CreateClass(t, &testClass)
		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{"AddTenantAgain"}})

		err := helper.CreateTenantsReturnError(t, testClass.Class, []*models.Tenant{{"AddTenantAgain"}})
		require.NotNil(t, err)
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

	tenants := []string{"tenant1", "tenant2", "tenant3"}
	var tenantsObject []*models.Tenant
	for _, tenant := range tenants {
		tenantsObject = append(tenantsObject, &models.Tenant{Name: tenant})
	}
	helper.CreateTenants(t, testClass.Class, tenantsObject)

	t.Run("Delete same tenant multiple times", func(Z *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant1", "tenant1"})
		require.NotNil(t, err)

		// nothing deleted
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		require.NotNil(t, resp.Payload.Nodes)
		require.Len(t, resp.Payload.Nodes, 1)
		require.Len(t, resp.Payload.Nodes[0].Shards, 3)
	})

	t.Run("Delete non-existent tenant alongside existing", func(Z *testing.T) {
		err := helper.DeleteTenants(t, testClass.Class, []string{"tenant1", "tenant5"})
		require.Nil(t, err)

		// idempotent - deleting multiple times works - tenant1 is removed
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
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
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
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

	err = helper.DeleteTenants(t, "DoesNotExist", []string{"doesNotMatter"})
	require.NotNil(t, err)
}
