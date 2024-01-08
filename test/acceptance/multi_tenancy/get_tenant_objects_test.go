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
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func TestGetTenantObjects(t *testing.T) {
	testClass := models.Class{
		Class: "MultiTenantClass",
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
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantNames[1],
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenantNames[2],
			},
			Tenant: tenantNames[2],
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
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, testClass.Class, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		for _, obj := range tenantObjects {
			helper.CreateObject(t, obj)
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

	t.Run("get tenant objects with include", func(t *testing.T) {
		for i, obj := range tenantObjects {
			resp, err := helper.TenantObjectWithInclude(t, obj.Class, obj.ID, tenantNames[i], "vector")
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
		}
	})
}

func TestListTenantObjects(t *testing.T) {
	tenantNames := []string{
		"Tenant1", "Tenant2",
	}

	classMT_1 := models.Class{
		Class: "MultiTenantClass1",
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
	classMT_2 := models.Class{
		Class: "MultiTenantClass2",
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
	classMT_3 := models.Class{
		Class: "SingleTenantClass3",
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
	classMT_4 := models.Class{
		Class: "SingleTenantClass4",
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
	classNonMT_1 := models.Class{
		Class: "NonTenantClass1",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	classNonMT_2 := models.Class{
		Class: "NonTenantClass2",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}

	objectsMT_T1 := []*models.Object{
		{
			ID:    "b1d19f8a-2158-4c41-b648-ba77a0ea7074",
			Class: classMT_1.Class,
			Properties: map[string]interface{}{
				"name": "Obj1_Class1_Tenant1",
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "a95c027c-07fb-4175-b726-4d5cfd55a7cf",
			Class: classMT_1.Class,
			Properties: map[string]interface{}{
				"name": "Obj2_Class1_Tenant1",
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "026890f5-8623-4d31-b295-b2820a81b85a",
			Class: classMT_2.Class,
			Properties: map[string]interface{}{
				"name": "Obj3_Class2_Tenant1",
			},
			Tenant: tenantNames[0],
		},
		{
			ID:    "d697d6b6-d7e6-47e6-a268-42e917b614e1",
			Class: classMT_3.Class,
			Properties: map[string]interface{}{
				"name": "Obj4_Class3_Tenant1",
			},
			Tenant: tenantNames[0],
		},
	}
	objectsMT_T2 := []*models.Object{
		{
			ID:    "7baead88-a42b-4876-a185-e0ccc61c58ca",
			Class: classMT_1.Class,
			Properties: map[string]interface{}{
				"name": "Obj1_Class1_Tenant2",
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "7fa1fd17-a883-465a-ae22-44f103250b27",
			Class: classMT_2.Class,
			Properties: map[string]interface{}{
				"name": "Obj2_Class2_Tenant2",
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
			Class: classMT_2.Class,
			Properties: map[string]interface{}{
				"name": "Obj3_Class2_Tenant2",
			},
			Tenant: tenantNames[1],
		},
		{
			ID:    "b33d8f4c-30f9-426d-94a5-fa256f3fb5e7",
			Class: classMT_4.Class,
			Properties: map[string]interface{}{
				"name": "Obj4_Class4_Tenant2",
			},
			Tenant: tenantNames[1],
		},
	}
	objectsNonMT := []*models.Object{
		{
			ID:    "6f019424-bacf-4539-b1be-fc1d3eccb50a",
			Class: classNonMT_1.Class,
			Properties: map[string]interface{}{
				"name": "Obj1_NonTenant1",
			},
		},
		{
			ID:    "8d02b16c-478c-4cae-9384-3b686bae0f4e",
			Class: classNonMT_1.Class,
			Properties: map[string]interface{}{
				"name": "Obj2_NonTenant1",
			},
		},
		{
			ID:    "865a820a-c325-4d10-8d8c-4b991bc43778",
			Class: classNonMT_2.Class,
			Properties: map[string]interface{}{
				"name": "Obj3_NonTenant2",
			},
		},
	}

	defer func() {
		helper.DeleteClass(t, classMT_1.Class)
		helper.DeleteClass(t, classMT_2.Class)
		helper.DeleteClass(t, classMT_3.Class)
		helper.DeleteClass(t, classMT_4.Class)
		helper.DeleteClass(t, classNonMT_1.Class)
		helper.DeleteClass(t, classNonMT_2.Class)
	}()

	extractIds := func(objs []*models.Object) []string {
		ids := make([]string, len(objs))
		for i, obj := range objs {
			ids[i] = obj.ID.String()
		}
		return ids
	}

	t.Run("create MT and non-MT classes", func(t *testing.T) {
		helper.CreateClass(t, &classMT_1)
		helper.CreateClass(t, &classMT_2)
		helper.CreateClass(t, &classMT_3)
		helper.CreateClass(t, &classMT_4)
		helper.CreateClass(t, &classNonMT_1)
		helper.CreateClass(t, &classNonMT_2)
	})

	t.Run("create tenants for MT classes", func(t *testing.T) {
		tenants := make([]*models.Tenant, len(tenantNames))
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, classMT_1.Class, tenants)
		helper.CreateTenants(t, classMT_2.Class, tenants)
		helper.CreateTenants(t, classMT_3.Class, tenants[:1])
		helper.CreateTenants(t, classMT_4.Class, tenants[1:])
	})

	t.Run("add objects", func(t *testing.T) {
		objects := append(objectsMT_T1, objectsMT_T2...)
		objects = append(objects, objectsNonMT...)

		helper.CreateObjectsBatch(t, objects)
	})

	t.Run("list objects for tenant 1", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, "", tenantNames[0])
			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(4), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"b1d19f8a-2158-4c41-b648-ba77a0ea7074",
				"a95c027c-07fb-4175-b726-4d5cfd55a7cf",
				"026890f5-8623-4d31-b295-b2820a81b85a",
				"d697d6b6-d7e6-47e6-a268-42e917b614e1",
			}, extractIds(res.Objects))
		})
		t.Run("classMT_T1T2_1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_1.Class, tenantNames[0])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"b1d19f8a-2158-4c41-b648-ba77a0ea7074",
				"a95c027c-07fb-4175-b726-4d5cfd55a7cf",
			}, extractIds(res.Objects))
		})
		t.Run("classMT_T1T2_2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_2.Class, tenantNames[0])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"026890f5-8623-4d31-b295-b2820a81b85a",
			}, extractIds(res.Objects))
		})

		t.Run("classMT_T1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_3.Class, tenantNames[0])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"d697d6b6-d7e6-47e6-a268-42e917b614e1",
			}, extractIds(res.Objects))
		})

		t.Run("classMT_T2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_4.Class, tenantNames[0])

			require.NotNil(t, err)
			expErr := &objects.ObjectsListUnprocessableEntity{}
			require.ErrorAs(t, err, &expErr)
			assert.Contains(t, err.(*objects.ObjectsListUnprocessableEntity).Payload.Error[0].Message, tenantNames[0])
			require.Nil(t, res)
		})
	})

	t.Run("list objects for tenant 2", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, "", tenantNames[1])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(4), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7baead88-a42b-4876-a185-e0ccc61c58ca",
				"7fa1fd17-a883-465a-ae22-44f103250b27",
				"fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
				"b33d8f4c-30f9-426d-94a5-fa256f3fb5e7",
			}, extractIds(res.Objects))
		})

		t.Run("classMT_T1T2_1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_1.Class, tenantNames[1])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7baead88-a42b-4876-a185-e0ccc61c58ca",
			}, extractIds(res.Objects))
		})

		t.Run("classMT_T1T2_2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_2.Class, tenantNames[1])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"7fa1fd17-a883-465a-ae22-44f103250b27",
				"fd4ce87a-8034-4e27-8d47-539fa9dde1f3",
			}, extractIds(res.Objects))
		})

		t.Run("classMT_T1", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_3.Class, tenantNames[1])

			require.NotNil(t, err)
			expErr := &objects.ObjectsListUnprocessableEntity{}
			require.ErrorAs(t, err, &expErr)
			assert.Contains(t, err.(*objects.ObjectsListUnprocessableEntity).Payload.Error[0].Message, tenantNames[1])
			require.Nil(t, res)
		})

		t.Run("classMT_T2", func(t *testing.T) {
			res, err := helper.TenantListObjects(t, classMT_4.Class, tenantNames[1])

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"b33d8f4c-30f9-426d-94a5-fa256f3fb5e7",
			}, extractIds(res.Objects))
		})
	})

	t.Run("list objects no tenant", func(t *testing.T) {
		t.Run("no class", func(t *testing.T) {
			res, err := helper.ListObjects(t, "")

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(3), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"6f019424-bacf-4539-b1be-fc1d3eccb50a",
				"8d02b16c-478c-4cae-9384-3b686bae0f4e",
				"865a820a-c325-4d10-8d8c-4b991bc43778",
			}, extractIds(res.Objects))
		})

		t.Run("classNonMT_1", func(t *testing.T) {
			res, err := helper.ListObjects(t, classNonMT_1.Class)

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(2), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"6f019424-bacf-4539-b1be-fc1d3eccb50a",
				"8d02b16c-478c-4cae-9384-3b686bae0f4e",
			}, extractIds(res.Objects))
		})

		t.Run("classNonMT_2", func(t *testing.T) {
			res, err := helper.ListObjects(t, classNonMT_2.Class)

			require.Nil(t, err)
			require.NotNil(t, res)
			require.Equal(t, int64(1), res.TotalResults)
			assert.ElementsMatch(t, []string{
				"865a820a-c325-4d10-8d8c-4b991bc43778",
			}, extractIds(res.Objects))
		})
	})
}
