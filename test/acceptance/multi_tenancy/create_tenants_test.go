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
