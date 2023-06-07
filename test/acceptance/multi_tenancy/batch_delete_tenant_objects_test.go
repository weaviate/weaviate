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
	"github.com/weaviate/weaviate/client/batch"
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
	tenantName := "Tenant1"
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
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
		tenants := make([]*models.Tenant, len(tenantObjects))
		for i := range tenants {
			tenants[i] = &models.Tenant{tenantName}
		}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		resp, err := helper.CreateTenantObjectsBatch(t, tenantObjects, tenantName)
		require.Nil(t, err)
		for _, elem := range resp {
			if !assert.Nil(t, elem.Result.Errors) {
				t.Logf("expected nil, got: %v",
					elem.Result.Errors.Error[0].Message)
			}
		}

		t.Run("verify tenant objects", func(t *testing.T) {
			for _, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
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
		resp, err := helper.DeleteTenantObjectsBatch(t, &batch, tenantName)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Results)
		assert.Nil(t, resp.Results.Objects)
		assert.Equal(t, int64(3), resp.Results.Successful)
		assert.Equal(t, int64(0), resp.Results.Failed)

		t.Run("verify tenant object deletion", func(t *testing.T) {
			for _, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
				assert.Nil(t, resp)
				assert.NotNil(t, err)
				assert.EqualError(t, objects.NewObjectsClassGetNotFound(), err.Error())
			}
		})
	})
}

func TestBatchDeleteTenantObjects_MissingTenantKey(t *testing.T) {
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
	tenantName := "Tenant1"
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
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
		tenants := make([]*models.Tenant, len(tenantObjects))
		for i := range tenants {
			tenants[i] = &models.Tenant{tenantName}
		}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		resp, err := helper.CreateTenantObjectsBatch(t, tenantObjects, tenantName)
		require.Nil(t, err)
		for _, elem := range resp {
			if !assert.Nil(t, elem.Result.Errors) {
				t.Logf("expected nil, got: %v",
					elem.Result.Errors.Error[0].Message)
			}
		}

		t.Run("verify tenant objects", func(t *testing.T) {
			for _, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
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
		params := batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{Match: &match})
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		badReqErr, ok := err.(*batch.BatchObjectsDeleteUnprocessableEntity)
		require.Nil(t, resp)
		require.NotNil(t, err)
		require.True(t, ok, "expected *batch.BatchObjectsDeleteUnprocessableEntity, got: %T", err)
		require.NotNil(t, badReqErr.Payload)
		require.NotNil(t, badReqErr.Payload.Error)
		require.Len(t, badReqErr.Payload.Error, 1)
		assert.Contains(t, badReqErr.Payload.Error[0].Message,
			`class "MultiTenantClass" has multi-tenancy enabled, tenant_key "tenantName" required`)
	})
}
