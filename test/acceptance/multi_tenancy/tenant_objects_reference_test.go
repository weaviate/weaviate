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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestTenantObjectsReference(t *testing.T) {
	className := "MultiTenantClass"
	mutableProp := "mutableProp"
	refProp := "refProp"
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
			{
				Name:     mutableProp,
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     refProp,
				DataType: []string{className},
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
				"name":      tenantNames[0],
				mutableProp: "obj#0",
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: className,
			Properties: map[string]interface{}{
				"name":      tenantNames[1],
				mutableProp: "obj#1",
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: className,
			Properties: map[string]interface{}{
				"name":      tenantNames[2],
				mutableProp: "obj#2",
			},
			Tenant: tenantNames[2],
		},
	}
	tenantRefs := []*models.Object{
		{
			ID:    "169b62a7-ef1c-481d-8fb0-27f11716bde7",
			Class: className,
			Properties: map[string]interface{}{
				"name":      tenantNames[0],
				mutableProp: "ref#0",
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "4d78424d-f7bd-479b-bd8a-52510e2db0fd",
			Class: className,
			Properties: map[string]interface{}{
				"name":      tenantNames[1],
				mutableProp: "ref#1",
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "c1db0a06-d5f9-4f77-aa3c-08a44f16e358",
			Class: className,
			Properties: map[string]interface{}{
				"name":      tenantNames[2],
				mutableProp: "ref#2",
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
		for i, obj := range tenantObjects {
			helper.CreateObject(t, obj)
			helper.CreateObject(t, tenantRefs[i])
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

	t.Run("add tenant object references", func(t *testing.T) {
		for i, obj := range tenantObjects {
			ref := &models.SingleRef{Beacon: helper.NewBeacon(className, tenantRefs[i].ID)}
			helper.AddReferenceTenant(t, obj, ref, refProp, tenantNames[i])
		}

		t.Run("assert tenant object references", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				require.Equal(t, obj.ID, resp.ID)
				require.Equal(t, obj.Class, resp.Class)
				refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
				require.Len(t, refs, 1)
				expectedBeacon := helper.NewBeacon(className, tenantRefs[i].ID).String()
				assert.Equal(t, expectedBeacon, refs[0].(map[string]interface{})["beacon"])
			}
		})
	})

	t.Run("delete tenant object references", func(Z *testing.T) {
		for i, obj := range tenantObjects {
			ref := &models.SingleRef{Beacon: helper.NewBeacon(className, tenantRefs[i].ID)}
			helper.DeleteReferenceTenant(t, obj, ref, refProp, tenantNames[i])
		}

		t.Run("assert tenant object references", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				require.Equal(t, obj.ID, resp.ID)
				require.Equal(t, obj.Class, resp.Class)
				refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
				require.Len(t, refs, 0)
			}
		})
	})
}
