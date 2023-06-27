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

func TestPatchTenantObjects_ChangeTenantKey(t *testing.T) {
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
		Properties: map[string]interface{}{
			tenantKey: tenantName,
		},
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
		helper.CreateTenants(t, className, []*models.Tenant{{tenantName}})
	})

	t.Run("add tenant object", func(t *testing.T) {
		params := objects.NewObjectsCreateParams().
			WithBody(&tenantObject).WithTenantKey(&tenantName)
		_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
		require.Nil(t, err)
	})

	t.Run("patch tenant object", func(t *testing.T) {
		toUpdate := models.Object{
			Class: testClass.Class,
			ID:    tenantObject.ID,
			Properties: map[string]interface{}{
				tenantKey: "updatedTenantName",
			},
		}
		params := objects.NewObjectsClassPatchParams().WithClassName(toUpdate.Class).
			WithID(toUpdate.ID).WithBody(&toUpdate).WithTenantKey(&tenantName)
		resp, err := helper.Client(t).Objects.ObjectsClassPatch(params, nil)
		require.Nil(t, resp)
		require.NotNil(t, err)
		parsedErr, ok := err.(*objects.ObjectsClassPatchUnprocessableEntity)
		require.True(t, ok)
		require.NotNil(t, parsedErr.Payload.Error)
		require.Len(t, parsedErr.Payload.Error, 1)
		assert.Contains(t, err.Error(), fmt.Sprint(http.StatusUnprocessableEntity))
		expected := "tenant key \"tenantName\" is immutable"
		assert.Contains(t, parsedErr.Payload.Error[0].Message, expected)
	})
}
