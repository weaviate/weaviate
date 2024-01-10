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
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBatchAddTenantObjects(t *testing.T) {
	tests := []func(*testing.T, bool){
		testBatchAddTenantObjects,
		testBatchAddTenantObjectsWithMixedClasses,
		testBatchWithMixedTenants,
		testAddNonTenantBatchToMultiClass,
		testAddBatchWithNonExistentTenant,
	}

	withImplicitTenantCreation(t, tests)
	withExplicitTenantCreation(t, tests)
}

func withImplicitTenantCreation(t *testing.T, tests []func(*testing.T, bool)) {
	for _, test := range tests {
		t.Run("with implicit tenant creation", func(t *testing.T) {
			test(t, true)
		})
	}
}

func withExplicitTenantCreation(t *testing.T, tests []func(*testing.T, bool)) {
	for _, test := range tests {
		t.Run("with explicit tenant creation", func(t *testing.T) {
			test(t, false)
		})
	}
}

func testBatchAddTenantObjects(t *testing.T, implicitTenants bool) {
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: implicitTenants,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	tenantName := "Tenant1"
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	if !implicitTenants {
		helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: tenantName}})
	}

	t.Run("add and get tenant objects", func(t *testing.T) {
		helper.CreateObjectsBatch(t, tenantObjects)

		for _, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Tenant, resp.Tenant)
		}
	})
}

func testBatchAddTenantObjectsWithMixedClasses(t *testing.T, implicitTenants bool) {
	testClass1 := models.Class{
		Class: "MultiTenantClass1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: implicitTenants,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	testClass2 := models.Class{
		Class: "MultiTenantClass2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: implicitTenants,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	tenantName := "Tenant1"
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass1.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass2.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass1.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
		{
			ID:    "dd5a3cdb-1bba-4a2b-b173-dad4fabd0326",
			Class: testClass2.Class,
			Properties: map[string]interface{}{
				"name": tenantName,
			},
			Tenant: tenantName,
		},
	}

	helper.CreateClass(t, &testClass1)
	helper.CreateClass(t, &testClass2)
	if !implicitTenants {
		helper.CreateTenants(t, testClass1.Class, []*models.Tenant{{Name: tenantName}})
		helper.CreateTenants(t, testClass2.Class, []*models.Tenant{{Name: tenantName}})
	}

	defer func() {
		helper.DeleteClass(t, testClass1.Class)
		helper.DeleteClass(t, testClass2.Class)
	}()

	t.Run("add and get tenant objects", func(t *testing.T) {
		helper.CreateObjectsBatch(t, tenantObjects)

		for _, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Tenant, resp.Tenant)
		}
	})
}

func testBatchWithMixedTenants(t *testing.T, implicitTenants bool) {
	className := "MultiTenantClassMixedBatchFail"
	classes := []models.Class{
		{
			Class: className + "1",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled:            true,
				AutoTenantCreation: implicitTenants,
			},
		}, {
			Class: className + "2",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled:            true,
				AutoTenantCreation: implicitTenants,
			},
		},
	}
	tenants := []string{"tenant1", "tenant2", "tenant3"}
	for i := range classes {
		helper.CreateClass(t, &classes[i])
		if !implicitTenants {
			for k := range tenants {
				helper.CreateTenants(t, classes[i].Class, []*models.Tenant{{Name: tenants[k]}})
			}
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
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  classes[i%2].Class,
			Tenant: tenants[i%len(tenants)],
		},
		)
	}
	helper.CreateObjectsBatch(t, tenantObjects)

	for _, obj := range tenantObjects {
		resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
		require.Nil(t, err)
		assert.Equal(t, obj.ID, resp.ID)
		assert.Equal(t, obj.Class, resp.Class)
	}
}

func testAddNonTenantBatchToMultiClass(t *testing.T, implicitTenants bool) {
	className := "MultiTenantClassBatchFail"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: implicitTenants,
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
	if !implicitTenants {
		helper.CreateTenants(t, className, []*models.Tenant{{Name: "randomTenant1"}})
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: nonTenantObjects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	if implicitTenants {
		require.Nil(t, resp)
		require.NotNil(t, err)
		batchErr := &batch.BatchObjectsCreateInternalServerError{}
		as := errors.As(err, &batchErr)
		require.True(t, as)
		require.NotNil(t, batchErr.Payload)
		require.Len(t, batchErr.Payload.Error, 1)
		require.Contains(t, batchErr.Payload.Error[0].Message, "empty tenant name")
	} else {
		require.NotNil(t, resp)
		require.Nil(t, err)
		for _, r := range resp.Payload {
			require.NotEmpty(t, r.Result.Errors.Error[0].Message)
		}
	}
}

func testAddBatchWithNonExistentTenant(t *testing.T, implicitTenants bool) {
	className := "MultiTenantClassBatchFail"
	testClass := models.Class{
		Class: className,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: implicitTenants,
		},
	}
	nonTenantObjects := []*models.Object{
		{
			ID:     "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class:  testClass.Class,
			Tenant: "something",
		},
		{
			ID:     "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class:  testClass.Class,
			Tenant: "something",
		},
		{
			ID:     "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class:  testClass.Class,
			Tenant: "something",
		},
	}

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	if !implicitTenants {
		helper.CreateTenants(t, className, []*models.Tenant{{Name: "somethingElse"}})
	}

	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: nonTenantObjects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.Nil(t, err)
	for i := range resp.Payload {
		if !implicitTenants {
			require.NotNil(t, resp.Payload[i].Result.Errors)
		} else {
			require.Nil(t, resp.Payload[i].Result.Errors)
		}
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
			ID:     "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class:  testClass.Class,
			Tenant: "something",
		},
		{
			ID:     "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class:  testClass.Class,
			Tenant: "something",
		},
		{
			ID:     "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class:  testClass.Class,
			Tenant: "something",
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
