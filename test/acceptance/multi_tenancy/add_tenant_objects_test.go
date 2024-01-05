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
	"testing"

	"github.com/weaviate/weaviate/client/objects"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAddTenantObjects(t *testing.T) {
	className := "MultiTenantClass"
	testClass := models.Class{
		Class: className,
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
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[1],
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[2],
			},
			Tenant: tenantNames[2],
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
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		for _, obj := range tenantObjects {
			helper.CreateObject(t, obj)
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

func TestAddTenantObjectsToNonMultiClass(t *testing.T) {
	className := "NoTenantClass"
	tenantName := "randomTenant"
	defer func() {
		helper.DeleteClass(t, className)
	}()

	testClass := models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
	}
	helper.CreateClass(t, &testClass)

	objWithTenant := &models.Object{
		ID:     "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
		Class:  className,
		Tenant: tenantName,
	}
	params := objects.NewObjectsCreateParams().WithBody(objWithTenant)
	_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	require.NotNil(t, err)
}

func TestAddNonTenantObjectsToMultiClass(t *testing.T) {
	className := "TenantClassFail"
	defer func() {
		helper.DeleteClass(t, className)
	}()

	testClass := models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	helper.CreateClass(t, &testClass)
	objWithTenant := &models.Object{
		ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
		Class: className,
	}
	params := objects.NewObjectsCreateParams().WithBody(objWithTenant)
	_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	require.NotNil(t, err)
}

func TestAddObjectWithNonexistentTenantToMultiClass(t *testing.T) {
	className := "TenantClass"
	defer func() {
		helper.DeleteClass(t, className)
	}()

	testClass := models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	helper.CreateClass(t, &testClass)
	helper.CreateTenants(t, className, []*models.Tenant{{Name: "randomTenant1"}})

	objWithTenant := &models.Object{
		ID:     "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
		Class:  className,
		Tenant: "randomTenant2",
	}
	params := objects.NewObjectsCreateParams().WithBody(objWithTenant)
	_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	require.NotNil(t, err)
}
