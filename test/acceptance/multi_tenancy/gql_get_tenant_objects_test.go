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
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestGQLGetTenantObjects(t *testing.T) {
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
			{
				Name:     "text",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		Vectorizer: "text2vec-contextionary",
	}
	tenant := "Tenant1"
	otherTenant := "otherTenant"
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenant,
				"text": "meat",
			},
			Tenant: tenant,
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenant,
				"text": "bananas",
			},
			Tenant: tenant,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenant,
				"text": "kiwi",
			},
			Tenant: tenant,
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: testClass.Class,
			Properties: map[string]interface{}{
				"name": tenant,
				"text": "kiwi",
			},
			Tenant: otherTenant,
		},
	}

	// add more objects for other tenant, won't show up in search

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	helper.CreateClass(t, &testClass)
	helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: tenant}, {Name: otherTenant}})
	helper.CreateObjectsBatch(t, tenantObjects)

	t.Run("Test tenant objects", func(t *testing.T) {
		for _, obj := range tenantObjects {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
			require.Nil(t, err)
			assert.Equal(t, obj.ID, resp.ID)
			assert.Equal(t, obj.Class, resp.Class)
			assert.Equal(t, obj.Properties, resp.Properties)
		}
	})

	t.Run("GQL Get tenant objects", func(t *testing.T) {
		expectedIDs := map[strfmt.UUID]bool{}
		for _, obj := range tenantObjects {
			expectedIDs[obj.ID] = false
		}

		query := fmt.Sprintf(`{Get{%s(tenant:%q){_additional{id}}}}`, testClass.Class, tenant)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		for _, obj := range result.Get("Get", testClass.Class).AsSlice() {
			id := obj.(map[string]any)["_additional"].(map[string]any)["id"].(string)
			if _, ok := expectedIDs[strfmt.UUID(id)]; ok {
				expectedIDs[strfmt.UUID(id)] = true
			} else {
				t.Fatalf("found unexpected id %q", id)
			}
		}

		for id, found := range expectedIDs {
			if !found {
				t.Fatalf("expected to find id %q, but didn't", id)
			}
		}
	})

	t.Run("GQL near objects", func(t *testing.T) {
		expectedIDs := map[strfmt.UUID]bool{}
		for _, obj := range tenantObjects {
			expectedIDs[obj.ID] = false
		}

		query := fmt.Sprintf(`{Get{%s(nearObject:{id: %q}, tenant:%q){_additional{id}}}}`, testClass.Class, tenantObjects[0].ID, tenant)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		res := result.Get("Get", testClass.Class)
		require.NotNil(t, res) // objects have no content, so no result
	})

	t.Run("GQL near text", func(t *testing.T) {
		expectedIDs := map[strfmt.UUID]bool{}
		for _, obj := range tenantObjects {
			expectedIDs[obj.ID] = false
		}

		query := fmt.Sprintf(`{Get{%s(nearText:{concepts: "apple", moveTo: {concepts: ["fruit"], force: 0.1}, moveAwayFrom: {objects: [{id:"0927a1e0-398e-4e76-91fb-04a7a8f0405c"}], force: 0.1}}, tenant:%q){_additional{id}}}}`, testClass.Class, tenant)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		res := result.Get("Get", testClass.Class)
		require.NotNil(t, res)
		require.Len(t, res.Result, 3) // don't find object from other tenants
	})

	t.Run("GQL bm25", func(t *testing.T) {
		expectedIDs := map[strfmt.UUID]bool{}
		for _, obj := range tenantObjects {
			expectedIDs[obj.ID] = false
		}

		query := fmt.Sprintf(`{Get{%s(bm25:{query: "kiwi"}, tenant:%q){_additional{id}}}}`, testClass.Class, tenant)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		res := result.Get("Get", testClass.Class)
		require.NotNil(t, res)
		require.Len(t, res.Result, 1) // don't find object from other tenants
	})

	t.Run("GQL hybrid", func(t *testing.T) {
		expectedIDs := map[strfmt.UUID]bool{}
		for _, obj := range tenantObjects {
			expectedIDs[obj.ID] = false
		}

		query := fmt.Sprintf(`{Get{%s(hybrid:{query: "kiwi", alpha: 0.1}, tenant:%q, autocut:1){text _additional{id}}}}`, testClass.Class, tenant)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		res := result.Get("Get", testClass.Class)
		require.NotNil(t, res)
		require.Len(t, res.Result, 1) // find only relevant results from tenant
	})
}

func TestGQLGetTenantObjects_MissingTenant(t *testing.T) {
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

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	helper.CreateClass(t, &testClass)
	helper.CreateTenants(t, testClass.Class, []*models.Tenant{{Name: tenantName}})
	helper.CreateObjectsBatch(t, tenantObjects)

	for _, obj := range tenantObjects {
		resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
		require.Nil(t, err)
		assert.Equal(t, obj.ID, resp.ID)
		assert.Equal(t, obj.Class, resp.Class)
		assert.Equal(t, obj.Properties, resp.Properties)
	}

	query := fmt.Sprintf(`{Get{%s{_additional{id}}}}`, testClass.Class)
	result, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
	require.Nil(t, err)
	require.Len(t, result.Errors, 1)
	assert.Nil(t, result.Data["Get"].(map[string]interface{})[testClass.Class])
	msg := fmt.Sprintf(`explorer: list class: search: object search at index %s: `,
		strings.ToLower(testClass.Class)) +
		fmt.Sprintf(`class %s has multi-tenancy enabled, but request was without tenant`, testClass.Class)
	assert.Equal(t, result.Errors[0].Message, msg)
}
