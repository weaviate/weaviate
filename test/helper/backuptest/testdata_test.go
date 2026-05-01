//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backuptest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestTestDataGenerator_GenerateClass(t *testing.T) {
	t.Run("default class", func(t *testing.T) {
		gen := NewTestDataGenerator(nil)
		class := gen.GenerateClass()

		assert.Equal(t, "BackupTestClass", class.Class)
		assert.Nil(t, class.MultiTenancyConfig)
		assert.NotEmpty(t, class.Properties)

		// Verify required properties exist
		propNames := make(map[string]bool)
		for _, prop := range class.Properties {
			propNames[prop.Name] = true
		}
		assert.True(t, propNames["title"])
		assert.True(t, propNames["content"])
		assert.True(t, propNames["count"])
		assert.True(t, propNames["score"])
		assert.True(t, propNames["active"])
		assert.True(t, propNames["tags"])
	})

	t.Run("custom class name", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "CustomClass",
		})
		class := gen.GenerateClass()

		assert.Equal(t, "CustomClass", class.Class)
	})

	t.Run("multi-tenant class", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName:   "MultiTenantClass",
			MultiTenant: true,
		})
		class := gen.GenerateClass()

		require.NotNil(t, class.MultiTenancyConfig)
		assert.True(t, class.MultiTenancyConfig.Enabled)
	})

	t.Run("with vectorizer", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName:     "VectorizerClass",
			UseVectorizer: "text2vec-contextionary",
		})
		class := gen.GenerateClass()

		assert.Equal(t, "text2vec-contextionary", class.Vectorizer)
		require.NotNil(t, class.ModuleConfig)
		_, hasConfig := class.ModuleConfig.(map[string]interface{})["text2vec-contextionary"]
		assert.True(t, hasConfig)
	})
}

func TestTestDataGenerator_GenerateTenants(t *testing.T) {
	t.Run("non-multi-tenant returns nil", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant: false,
		})
		tenants := gen.GenerateTenants()
		assert.Nil(t, tenants)
	})

	t.Run("multi-tenant returns expected count", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant: true,
			NumTenants:  5,
		})
		tenants := gen.GenerateTenants()

		require.Len(t, tenants, 5)
		assert.Equal(t, "tenant-0", tenants[0])
		assert.Equal(t, "tenant-4", tenants[4])
	})
}

func TestTestDataGenerator_GenerateTenantModels(t *testing.T) {
	t.Run("creates tenant models with correct properties", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant: true,
			NumTenants:  3,
		})
		tenants := gen.GenerateTenantModels()

		require.Len(t, tenants, 3)
		for _, tenant := range tenants {
			assert.NotEmpty(t, tenant.Name)
			assert.Equal(t, models.TenantActivityStatusHOT, tenant.ActivityStatus)
		}
	})

	t.Run("non-multi-tenant returns nil", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant: false,
		})
		tenants := gen.GenerateTenantModels()
		assert.Nil(t, tenants)
	})
}

func TestTestDataGenerator_GenerateObject(t *testing.T) {
	t.Run("generates valid object", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "TestClass",
			Seed:      12345, // Fixed seed for reproducibility
		})
		obj := gen.GenerateObject("")

		assert.Equal(t, "TestClass", obj.Class)
		assert.NotEmpty(t, obj.ID)
		assert.Empty(t, obj.Tenant)

		props := obj.Properties.(map[string]interface{})
		assert.NotEmpty(t, props["title"])
		assert.NotEmpty(t, props["content"])
		assert.NotNil(t, props["count"])
		assert.NotNil(t, props["score"])
		assert.NotNil(t, props["active"])
		assert.NotNil(t, props["tags"])
	})

	t.Run("generates object with tenant", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "TestClass",
		})
		obj := gen.GenerateObject("tenant-1")

		assert.Equal(t, "tenant-1", obj.Tenant)
	})

	t.Run("generates unique IDs", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "TestClass",
		})

		ids := make(map[string]bool)
		for i := 0; i < 100; i++ {
			obj := gen.GenerateObject("")
			idStr := obj.ID.String()
			assert.False(t, ids[idStr], "duplicate ID generated: %s", idStr)
			ids[idStr] = true
		}
	})
}

func TestTestDataGenerator_GenerateObjects(t *testing.T) {
	t.Run("generates correct count", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "TestClass",
		})
		objects := gen.GenerateObjects("", 25)

		assert.Len(t, objects, 25)
	})

	t.Run("all objects have tenant when specified", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName: "TestClass",
		})
		objects := gen.GenerateObjects("my-tenant", 10)

		for _, obj := range objects {
			assert.Equal(t, "my-tenant", obj.Tenant)
		}
	})
}

func TestTestDataGenerator_GenerateAllObjects(t *testing.T) {
	t.Run("single tenant mode", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName:        "TestClass",
			MultiTenant:      false,
			ObjectsPerTenant: 15,
		})
		objects := gen.GenerateAllObjects()

		assert.Len(t, objects, 15)
		for _, obj := range objects {
			assert.Empty(t, obj.Tenant)
		}
	})

	t.Run("multi-tenant mode", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			ClassName:        "TestClass",
			MultiTenant:      true,
			NumTenants:       3,
			ObjectsPerTenant: 10,
		})
		objects := gen.GenerateAllObjects()

		assert.Len(t, objects, 30) // 3 tenants * 10 objects

		// Verify distribution across tenants
		byTenant := gen.ObjectsByTenant(objects)
		assert.Len(t, byTenant, 3)
		for tenant, objs := range byTenant {
			assert.Len(t, objs, 10)
			for _, obj := range objs {
				assert.Equal(t, tenant, obj.Tenant)
			}
		}
	})
}

func TestTestDataGenerator_ObjectsByTenant(t *testing.T) {
	gen := NewTestDataGenerator(&TestDataConfig{
		ClassName: "TestClass",
	})

	objects := []*models.Object{
		{Tenant: "tenant-a"},
		{Tenant: "tenant-a"},
		{Tenant: "tenant-b"},
		{Tenant: ""},
	}

	grouped := gen.ObjectsByTenant(objects)

	assert.Len(t, grouped["tenant-a"], 2)
	assert.Len(t, grouped["tenant-b"], 1)
	assert.Len(t, grouped[""], 1)
}

func TestTestDataGenerator_TotalObjectCount(t *testing.T) {
	t.Run("single tenant", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant:      false,
			ObjectsPerTenant: 50,
		})
		assert.Equal(t, 50, gen.TotalObjectCount())
	})

	t.Run("multi-tenant", func(t *testing.T) {
		gen := NewTestDataGenerator(&TestDataConfig{
			MultiTenant:      true,
			NumTenants:       5,
			ObjectsPerTenant: 20,
		})
		assert.Equal(t, 100, gen.TotalObjectCount())
	})
}

func TestTestDataGenerator_FluentAPI(t *testing.T) {
	gen := NewTestDataGenerator(nil).
		WithClassName("FluentClass").
		WithMultiTenant(true).
		WithNumTenants(3).
		WithObjectsPerTenant(5).
		WithVectorizer("text2vec-contextionary").
		WithSeed(42)

	class := gen.GenerateClass()
	assert.Equal(t, "FluentClass", class.Class)
	assert.True(t, class.MultiTenancyConfig.Enabled)
	assert.Equal(t, "text2vec-contextionary", class.Vectorizer)

	tenants := gen.GenerateTenants()
	assert.Len(t, tenants, 3)

	objects := gen.GenerateAllObjects()
	assert.Len(t, objects, 15)
}

func TestTestDataGenerator_Reproducibility(t *testing.T) {
	// Same seed should produce same results
	gen1 := NewTestDataGenerator(&TestDataConfig{
		ClassName: "TestClass",
		Seed:      12345,
	})
	gen2 := NewTestDataGenerator(&TestDataConfig{
		ClassName: "TestClass",
		Seed:      12345,
	})

	// Generate the same number of objects
	for i := 0; i < 10; i++ {
		obj1 := gen1.GenerateObject("")
		obj2 := gen2.GenerateObject("")

		// Properties should be identical (IDs will differ as they use uuid.New())
		props1 := obj1.Properties.(map[string]interface{})
		props2 := obj2.Properties.(map[string]interface{})

		assert.Equal(t, props1["title"], props2["title"])
		assert.Equal(t, props1["content"], props2["content"])
		assert.Equal(t, props1["count"], props2["count"])
		assert.Equal(t, props1["score"], props2["score"])
		assert.Equal(t, props1["active"], props2["active"])
	}
}

func TestFixedTestData(t *testing.T) {
	t.Run("creates known objects", func(t *testing.T) {
		data := NewFixedTestData("FixedClass")

		assert.Equal(t, "FixedClass", data.ClassName)
		require.Len(t, data.Objects, 3)

		// Verify IDs are predictable
		assert.Equal(t, "00000000-0000-0000-0000-000000000001", data.Objects[0].ID.String())
		assert.Equal(t, "00000000-0000-0000-0000-000000000002", data.Objects[1].ID.String())
		assert.Equal(t, "00000000-0000-0000-0000-000000000003", data.Objects[2].ID.String())

		// Verify class name is set on objects
		for _, obj := range data.Objects {
			assert.Equal(t, "FixedClass", obj.Class)
		}
	})

	t.Run("IDs returns all object IDs", func(t *testing.T) {
		data := NewFixedTestData("TestClass")
		ids := data.IDs()

		require.Len(t, ids, 3)
		assert.Equal(t, "00000000-0000-0000-0000-000000000001", ids[0].String())
		assert.Equal(t, "00000000-0000-0000-0000-000000000002", ids[1].String())
		assert.Equal(t, "00000000-0000-0000-0000-000000000003", ids[2].String())
	})
}

func TestTestDataGenerator_ClassPropertyTypes(t *testing.T) {
	gen := NewTestDataGenerator(nil)
	class := gen.GenerateClass()

	propTypes := make(map[string]string)
	for _, prop := range class.Properties {
		propTypes[prop.Name] = prop.DataType[0]
	}

	// Verify correct data types
	assert.Equal(t, string(schema.DataTypeText), propTypes["title"])
	assert.Equal(t, string(schema.DataTypeText), propTypes["content"])
	assert.Equal(t, string(schema.DataTypeInt), propTypes["count"])
	assert.Equal(t, string(schema.DataTypeNumber), propTypes["score"])
	assert.Equal(t, string(schema.DataTypeBoolean), propTypes["active"])
	assert.Equal(t, string(schema.DataTypeTextArray), propTypes["tags"])
}
