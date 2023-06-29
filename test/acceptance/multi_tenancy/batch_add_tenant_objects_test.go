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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/client/batch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBatchAddTenantObjects(t *testing.T) {
	tenantKey := "tenantName"
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
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
			TenantName: tenantName,
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
			TenantName: tenantName,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
			TenantName: tenantName,
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	tenants := make([]*models.Tenant, len(tenantObjects))
	for i := range tenants {
		tenants[i] = &models.Tenant{tenantName}
	}
	helper.CreateTenants(t, testClass.Class, tenants)

	t.Run("add and get tenant objects", func(t *testing.T) {
		helper.CreateObjectsBatch(t, tenantObjects)

		for _, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
			assert.Equal(t, obj.TenantName, resp.Properties.(map[string]interface{})[tenantKey])
		}
	})
}

func TestBatchWithMixedTenants(t *testing.T) {
	className := "MultiTenantClassMixedBatchFail"
	classes := []models.Class{
		{
			Class: className + "1",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		}, {
			Class: className + "2",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		},
	}
	tenants := []string{"tenant1", "tenant2", "tenant3"}
	for i := range classes {
		helper.CreateClass(t, &classes[i])
		for k := range tenants {
			helper.CreateTenants(t, classes[i].Class, []*models.Tenant{{tenants[k]}})
		}
	}
	defer func() {
		for i := range classes {
			helper.DeleteClass(t, classes[i].Class)
		}
	}()

	var tenantObjects []*models.Object

	for i := 0; i < 9; i++ {
		tenantObjects = append(tenantObjects, &models.Object{
			ID:         strfmt.UUID(uuid.New().String()),
			Class:      classes[i%2].Class,
			TenantName: tenants[i%len(tenants)],
		},
		)
	}
	helper.CreateObjectsBatch(t, tenantObjects)

	for _, obj := range tenantObjects {
		resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.TenantName)
		require.Nil(t, err)
		assert.Equal(t, obj.ID, resp.ID)
		assert.Equal(t, obj.Class, resp.Class)
	}
}

func TestAddNonTenantBatchToMultiClass(t *testing.T) {
	className := "MultiTenantClassBatchFail"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	nonTenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass.Class,
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	helper.CreateTenants(t, className, []*models.Tenant{{"randomTenant1"}})
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: nonTenantObjects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.Nil(t, err)
	for i := range resp.Payload {
		require.NotNil(t, resp.Payload[i].Result.Errors)
	}
}

func TestAddBatchToNonMultiClass(t *testing.T) {
	className := "MultiTenantClassBatchFail"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: false,
		},
	}
	tenantObjects := []*models.Object{
		{
			ID:         "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class:      testClass.Class,
			TenantName: "something",
		},
		{
			ID:         "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class:      testClass.Class,
			TenantName: "something",
		},
		{
			ID:         "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class:      testClass.Class,
			TenantName: "something",
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: tenantObjects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.Nil(t, err)
	for i := range resp.Payload {
		require.NotNil(t, resp.Payload[i].Result.Errors)
	}
}

func TestAddBatchWithNonExistentTenant(t *testing.T) {
	className := "MultiTenantClassBatchFail"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	nonTenantObjects := []*models.Object{
		{
			ID:         "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class:      testClass.Class,
			TenantName: "something",
		},
		{
			ID:         "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class:      testClass.Class,
			TenantName: "something",
		},
		{
			ID:         "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class:      testClass.Class,
			TenantName: "something",
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	helper.CreateTenants(t, className, []*models.Tenant{{"somethingElse"}})

	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: nonTenantObjects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.Nil(t, err)
	for i := range resp.Payload {
		require.NotNil(t, resp.Payload[i].Result.Errors)
	}
}
