package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestPatchTenantObjects(t *testing.T) {
	tenantKey := "tenantName"
	mutableProp := "mutableProp"
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			}, {
				Name:     mutableProp,
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
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey:   tenantNames[0],
				mutableProp: "obj#0",
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey:   tenantNames[1],
				mutableProp: "obj#1",
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey:   tenantNames[2],
				mutableProp: "obj#2",
			},
		},
	}

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
	})

	t.Run("create tenants", func(t *testing.T) {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i := range tenants {
			tenants[i] = &models.Tenant{tenantNames[i]}
		}
		helper.CreateTenants(t, testClass.Class, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		for i, obj := range tenantObjects {
			helper.CreateTenantObject(t, obj, tenantNames[i])
		}

		t.Run("verify tenant object creation", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				require.Equal(t, obj.ID, resp.ID)
				require.Equal(t, obj.Class, resp.Class)
				require.Equal(t, obj.Properties, resp.Properties)
			}
		})
	})

	t.Run("patch tenant objects", func(t *testing.T) {
		for i, obj := range tenantObjects {
			mut := obj.Properties.(map[string]interface{})[mutableProp]
			toUpdate := &models.Object{
				Class: testClass.Class,
				ID:    obj.ID,
				Properties: map[string]interface{}{
					tenantKey:   tenantNames[i],
					mutableProp: fmt.Sprintf("%s--patched", mut),
				},
			}
			helper.PatchTenantObject(t, toUpdate, tenantNames[i])
		}

		t.Run("assert tenant object updates", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				require.Equal(t, obj.ID, resp.ID)
				require.Equal(t, obj.Class, resp.Class)
				expectedProps := obj.Properties.(map[string]interface{})
				expectedProps[mutableProp] = fmt.Sprintf("%s--patched", expectedProps[mutableProp])
				require.Equal(t, expectedProps, resp.Properties)
			}
		})
	})
}
