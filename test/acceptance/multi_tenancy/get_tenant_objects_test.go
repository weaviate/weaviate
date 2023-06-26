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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestGetTenantObjects(t *testing.T) {
	tenantKey := "tenantName"
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
				tenantKey: tenantNames[0],
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[1],
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[2],
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
	})

	t.Run("get tenant objects", func(t *testing.T) {
		for i, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantNames[i])
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
		}
	})
}

func TestListTenantObjects(t *testing.T) {
	tenantKey := "tenantName"
	tenantNames := []string{
		"Tenant1", "Tenant2",
	}

	tenantClass1 := models.Class{
		Class: "MultiTenantClass1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	tenantClass2 := models.Class{
		Class: "MultiTenantClass2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	nonTenantClass1 := models.Class{
		Class: "NonTenantClass1",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	nonTenantClass2 := models.Class{
		Class: "NonTenantClass2",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}

	tenant1Objects := []*models.Object{
		{
			ID:    "b1d19f8a-2158-4c41-b648-ba77a0ea7074",
			Class: tenantClass1.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[0],
				"name":    "Obj1_Class1_Tenant1",
			},
		},
		{
			ID:    "a95c027c-07fb-4175-b726-4d5cfd55a7cf",
			Class: tenantClass1.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[0],
				"name":    "Obj2_Class1_Tenant1",
			},
		},
		{
			ID:    "026890f5-8623-4d31-b295-b2820a81b85a",
			Class: tenantClass2.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[0],
				"name":    "Obj3_Class2_Tenant1",
			},
		},
	}
	tenant2Objects := []*models.Object{
		{
			ID:    "7baead88-a42b-4876-a185-e0ccc61c58ca",
			Class: tenantClass1.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[1],
				"name":    "Obj1_Class1_Tenant2",
			},
		},
		{
			ID:    "7fa1fd17-a883-465a-ae22-44f103250b27",
			Class: tenantClass2.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[1],
				"name":    "Obj2_Class2_Tenant2",
			},
		},
		{
			ID:    "fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
			Class: tenantClass2.Class,
			Properties: map[string]interface{}{
				tenantKey: tenantNames[1],
				"name":    "Obj3_Class2_Tenant2",
			},
		},
	}
	nonTenantObjects := []*models.Object{
		{
			ID:    "6f019424-bacf-4539-b1be-fc1d3eccb50a",
			Class: nonTenantClass1.Class,
			Properties: map[string]interface{}{
				"name": "Obj1_NonTenant1",
			},
		},
		{
			ID:    "8d02b16c-478c-4cae-9384-3b686bae0f4e",
			Class: nonTenantClass1.Class,
			Properties: map[string]interface{}{
				"name": "Obj2_NonTenant1",
			},
		},
		{
			ID:    "865a820a-c325-4d10-8d8c-4b991bc43778",
			Class: nonTenantClass2.Class,
			Properties: map[string]interface{}{
				"name": "Obj3_NonTenant2",
			},
		},
	}

	defer func() {
		helper.DeleteClass(t, tenantClass1.Class)
		helper.DeleteClass(t, tenantClass2.Class)
		helper.DeleteClass(t, nonTenantClass1.Class)
		helper.DeleteClass(t, nonTenantClass2.Class)
	}()

	extractIds := func(objs []*models.Object) []string {
		ids := make([]string, len(objs))
		for i, obj := range objs {
			ids[i] = obj.ID.String()
		}
		return ids
	}

	t.Run("create MT and non-MT classes", func(t *testing.T) {
		helper.CreateClass(t, &tenantClass1)
		helper.CreateClass(t, &tenantClass2)
		helper.CreateClass(t, &nonTenantClass1)
		helper.CreateClass(t, &nonTenantClass2)
	})

	t.Run("create tenants for MT classes", func(t *testing.T) {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, tenantClass1.Class, tenants)
		helper.CreateTenants(t, tenantClass2.Class, tenants)
	})

	t.Run("add objects", func(t *testing.T) {
		helper.CreateTenantObjectsBatch(t, tenant1Objects, tenantNames[0])
		helper.CreateTenantObjectsBatch(t, tenant2Objects, tenantNames[1])
		helper.CreateObjectsBatch(t, nonTenantObjects)
	})

	t.Run("list objects for tenant 1", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, "", tenantNames[0])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(3), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"b1d19f8a-2158-4c41-b648-ba77a0ea7074",
				"a95c027c-07fb-4175-b726-4d5cfd55a7cf",
				"026890f5-8623-4d31-b295-b2820a81b85a",
			}, extractIds(res.Objects))
		})

		t.Run("class 1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, tenantClass1.Class, tenantNames[0])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"b1d19f8a-2158-4c41-b648-ba77a0ea7074",
				"a95c027c-07fb-4175-b726-4d5cfd55a7cf",
			}, extractIds(res.Objects))
		})

		t.Run("class 2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, tenantClass2.Class, tenantNames[0])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"026890f5-8623-4d31-b295-b2820a81b85a",
			}, extractIds(res.Objects))
		})
	})

	t.Run("list objects for tenant 2", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, "", tenantNames[1])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(3), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7baead88-a42b-4876-a185-e0ccc61c58ca",
				"7fa1fd17-a883-465a-ae22-44f103250b27",
				"fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
			}, extractIds(res.Objects))
		})

		t.Run("class 1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, tenantClass1.Class, tenantNames[1])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7baead88-a42b-4876-a185-e0ccc61c58ca",
			}, extractIds(res.Objects))
		})

		t.Run("class 2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, tenantClass2.Class, tenantNames[1])

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7fa1fd17-a883-465a-ae22-44f103250b27",
				"fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
			}, extractIds(res.Objects))
		})
	})

	t.Run("list objects no tenant", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.ListObjects(t, "")

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(3), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"6f019424-bacf-4539-b1be-fc1d3eccb50a",
				"8d02b16c-478c-4cae-9384-3b686bae0f4e",
				"865a820a-c325-4d10-8d8c-4b991bc43778",
			}, extractIds(res.Objects))
		})

		t.Run("class 1", func(t *testing.T) {
			res, err := helper.ListObjects(t, nonTenantClass1.Class)

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"6f019424-bacf-4539-b1be-fc1d3eccb50a",
				"8d02b16c-478c-4cae-9384-3b686bae0f4e",
			}, extractIds(res.Objects))
		})

		t.Run("class 2", func(t *testing.T) {
			res, err := helper.ListObjects(t, nonTenantClass2.Class)

			assert.Nil(t, err)
			require.NotNil(t, res)
			assert.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"865a820a-c325-4d10-8d8c-4b991bc43778",
			}, extractIds(res.Objects))
		})
	})
}
