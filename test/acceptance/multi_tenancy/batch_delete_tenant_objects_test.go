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
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBatchDeleteTenantObjects(t *testing.T) {
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
		helper.CreateObjectsBatch(t, tenantObjects)

		t.Run("verify tenant objects", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				require.Nil(t, err)
				require.Equal(t, obj.ID, resp.ID)
				require.Equal(t, obj.Class, resp.Class)
				require.Equal(t, obj.Properties, resp.Properties)
			}
		})
	})

	t.Run("batch delete tenant objects", func(t *testing.T) {
		glob := "*"
		where := models.WhereFilter{
			Operator:    filters.OperatorLike.Name(),
			Path:        []string{"id"},
			ValueString: &glob,
		}
		match := models.BatchDeleteMatch{
			Class: className,
			Where: &where,
		}
		batch := models.BatchDelete{Match: &match}
		helper.DeleteObjectsBatch(t, &batch)

		t.Run("verify tenant object deletion", func(t *testing.T) {
			for i, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
				assert.Nil(t, resp)
				assert.NotNil(t, err)
				assert.EqualError(t, objects.NewObjectsClassGetNotFound(), err.Error())
			}
		})
	})
}
