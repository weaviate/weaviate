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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAddTenantObjects(t *testing.T) {
	className := "MultiTenantClass"
	tenantKey := "tenantName"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
		},
	}
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[0],
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[1],
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[2],
			},
		},
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
	})

	t.Run("create tenants", func(t *testing.T) {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i := range tenants {
			tenants[i] = &models.Tenant{tenantNames[i]}
		}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		for i, obj := range tenantObjects {
			helper.CreateTenantObject(t, obj, tenantNames[i])
		}
	})

	t.Run("verify object creation", func(t *testing.T) {
		for i, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
			require.Nil(t, err)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
		}
	})
}

func TestAddTenantObjects_MissingTenantKey(t *testing.T) {
	className := "MultiTenantClass"
	tenantKey := "tenantName"
	tenantName := "Tenant1"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
		},
	}
	tenantObject := models.Object{
		ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
		Class: className,
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
	})

	t.Run("create tenants", func(t *testing.T) {
		helper.CreateTenants(t, className, []*models.Tenant{{tenantName}})
	})

	t.Run("add tenant object", func(t *testing.T) {
		params := objects.NewObjectsCreateParams().
			WithBody(&tenantObject).WithTenantKey(&tenantName)
		resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
		require.Nil(t, resp)
		require.NotNil(t, err)
		parsedErr, ok := err.(*objects.ObjectsCreateUnprocessableEntity)
		require.True(t, ok)
		expected := "put object: import into index multitenantclass: " +
			"tenant_key query param value \"Tenant1\" conflicts with object " +
			"body value \"\" for class \"MultiTenantClass\" tenant key \"tenantName\""
		require.NotNil(t, parsedErr.Payload)
		require.NotNil(t, parsedErr.Payload.Error)
		require.Len(t, parsedErr.Payload.Error, 1)
		assert.Contains(t, err.Error(), fmt.Sprint(http.StatusUnprocessableEntity))
		assert.Equal(t, expected, parsedErr.Payload.Error[0].Message)
	})
}
