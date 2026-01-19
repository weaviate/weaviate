//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Helper function to create test collection for deletion tests
func createDeleteTestCollection(t *testing.T, apiKey string) *models.Class {
	t.Helper()

	class := &models.Class{
		Class: "DeleteTestArticle",
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
			{
				Name:     "status",
				DataType: []string{"text"},
			},
			{
				Name:     "year",
				DataType: []string{"int"},
			},
			{
				Name:     "publishDate",
				DataType: []string{"date"},
			},
		},
	}

	helper.CreateClassAuth(t, class, apiKey)
	return class
}

// Helper function to create multi-tenant collection for deletion tests
func createDeleteTestMultiTenantCollection(t *testing.T, apiKey string) *models.Class {
	t.Helper()

	class := &models.Class{
		Class: "DeleteTestTenantData",
		Properties: []*models.Property{
			{
				Name:     "data",
				DataType: []string{"text"},
			},
			{
				Name:     "category",
				DataType: []string{"text"},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	helper.CreateClassAuth(t, class, apiKey)

	// Create tenants
	helper.CreateTenantsAuth(t, class.Class, []*models.Tenant{
		{Name: "tenant-a"},
		{Name: "tenant-b"},
	}, apiKey)

	return class
}

// Helper function to insert test objects
func insertDeleteTestObjects(t *testing.T, className string, apiKey string) []strfmt.UUID {
	t.Helper()

	objects := []map[string]interface{}{
		{"title": "Article One", "status": "published", "year": 2020, "publishDate": "2020-01-15T10:00:00Z"},
		{"title": "Article Two", "status": "draft", "year": 2021, "publishDate": "2021-03-20T14:30:00Z"},
		{"title": "Article Three", "status": "published", "year": 2022, "publishDate": "2022-06-10T09:15:00Z"},
		{"title": "Article Four", "status": "archived", "year": 2019, "publishDate": "2019-11-05T16:45:00Z"},
		{"title": "Article Five", "status": "published", "year": 2023, "publishDate": "2023-08-25T11:20:00Z"},
	}

	var ids []strfmt.UUID
	for _, props := range objects {
		obj := &models.Object{
			Class:      className,
			Properties: props,
		}
		resp, err := helper.CreateObjectWithResponseAuth(t, obj, apiKey)
		require.Nil(t, err)
		ids = append(ids, resp.ID)
	}

	return ids
}

// Helper to create object with tenant and auth
func createObjectWithTenantAuth(t *testing.T, className string, props map[string]interface{}, apiKey string, tenant string) strfmt.UUID {
	t.Helper()
	obj := &models.Object{
		Class:      className,
		Properties: props,
		Tenant:     tenant,
	}
	resp, err := helper.CreateObjectWithResponseAuth(t, obj, apiKey)
	require.Nil(t, err)
	return resp.ID
}

// Helper to list tenant objects with auth
func tenantListObjectsAuth(t *testing.T, class string, tenant string, apiKey string) (*models.ObjectsListResponse, error) {
	t.Helper()
	// Need to manually implement since helper doesn't have auth version
	req := objects.NewObjectsListParams().WithTenant(&tenant)
	if class != "" {
		req.WithClass(&class)
	}
	resp, err := helper.Client(t).Objects.ObjectsList(req, helper.CreateAuth(apiKey))
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// Test 1: Dry run delete single object (default behavior)
func TestDeleteObjects_DryRunSingleObject(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ids := insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete with filter matching single object (dry run by default)
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "draft",
		},
		// dry_run not specified, should default to true
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DryRun, "should be dry run by default")
	assert.Equal(t, 1, result.Matches, "should match 1 object")
	assert.Equal(t, 0, result.Deleted, "should not delete in dry run")

	// Verify object still exists
	obj, err := helper.GetObjectAuth(t, cls.Class, ids[1], apiKey)
	require.Nil(t, err)
	assert.NotNil(t, obj)
	assert.Equal(t, "draft", obj.Properties.(map[string]interface{})["status"])
}

// Test 2: Actual delete single object with dry_run=false
func TestDeleteObjects_ActualDeleteSingleObject(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ids := insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete with dry_run explicitly set to false
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "draft",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.False(t, result.DryRun, "should not be dry run")
	assert.Equal(t, 1, result.Matches, "should match 1 object")
	assert.Equal(t, 1, result.Deleted, "should delete 1 object")

	// Verify object was deleted
	err = helper.AssertGetObjectFailsEventually(t, cls.Class, ids[1])
	assert.NotNil(t, err, "object should be deleted")

	// Verify other objects still exist
	obj, err := helper.GetObjectAuth(t, cls.Class, ids[0], apiKey)
	require.Nil(t, err)
	assert.NotNil(t, obj)
}

// Test 3: Delete multiple objects with filter
func TestDeleteObjects_DeleteMultipleObjects(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete all objects with status="published"
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "published",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.False(t, result.DryRun)
	assert.Equal(t, 3, result.Matches, "should match 3 published objects")
	assert.Equal(t, 3, result.Deleted, "should delete 3 objects")

	// Verify remaining objects
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(2), list.TotalResults, "should have 2 objects remaining")
}

// Test 4: Delete with numeric filter
func TestDeleteObjects_WithNumericFilter(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete objects with year < 2021
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":     []string{"year"},
			"operator": "LessThan",
			"valueInt": 2021,
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, result.Matches, "should match 2 objects (2019, 2020)")
	assert.Equal(t, 2, result.Deleted, "should delete 2 objects")

	// Verify remaining objects
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(3), list.TotalResults, "should have 3 objects remaining")
}

// Test 5: Delete with date filter
func TestDeleteObjects_WithDateFilter(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete objects published before 2021
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"publishDate"},
			"operator":  "LessThan",
			"valueDate": "2021-01-01T00:00:00Z",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, result.Matches, "should match 2 objects from 2019 and 2020")
	assert.Equal(t, 2, result.Deleted, "should delete 2 objects")
}

// Test 6: Delete with complex AND filter
func TestDeleteObjects_WithComplexAndFilter(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete objects with status="published" AND year >= 2022
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"operator": "And",
			"operands": []map[string]any{
				{
					"path":      []string{"status"},
					"operator":  "Equal",
					"valueText": "published",
				},
				{
					"path":     []string{"year"},
					"operator": "GreaterThanEqual",
					"valueInt": 2022,
				},
			},
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, result.Matches, "should match 2 objects (2022, 2023)")
	assert.Equal(t, 2, result.Deleted, "should delete 2 objects")

	// Verify remaining objects
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(3), list.TotalResults, "should have 3 objects remaining")
}

// Test 7: Delete with OR filter
func TestDeleteObjects_WithOrFilter(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete objects with status="draft" OR status="archived"
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"operator": "Or",
			"operands": []map[string]any{
				{
					"path":      []string{"status"},
					"operator":  "Equal",
					"valueText": "draft",
				},
				{
					"path":      []string{"status"},
					"operator":  "Equal",
					"valueText": "archived",
				},
			},
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, result.Matches, "should match 2 objects (draft + archived)")
	assert.Equal(t, 2, result.Deleted, "should delete 2 objects")

	// Verify only published objects remain
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(3), list.TotalResults, "should have 3 published objects remaining")
}

// Test 8: Delete all objects (no filter)
func TestDeleteObjects_DeleteAllObjects(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete all objects (no where filter)
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		// No Where filter - should delete all
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 5, result.Matches, "should match all 5 objects")
	assert.Equal(t, 5, result.Deleted, "should delete all 5 objects")

	// Verify no objects remain
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(0), list.TotalResults, "should have no objects remaining")
}

// Test 9: Delete with tenant
func TestDeleteObjects_WithTenant(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestMultiTenantCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	// Insert objects for tenant-a
	tenantA := "tenant-a"
	createObjectWithTenantAuth(t, cls.Class, map[string]interface{}{
		"data":     "tenant-a data 1",
		"category": "active",
	}, apiKey, tenantA)
	createObjectWithTenantAuth(t, cls.Class, map[string]interface{}{
		"data":     "tenant-a data 2",
		"category": "inactive",
	}, apiKey, tenantA)

	// Insert objects for tenant-b
	tenantB := "tenant-b"
	createObjectWithTenantAuth(t, cls.Class, map[string]interface{}{
		"data":     "tenant-b data 1",
		"category": "active",
	}, apiKey, tenantB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete inactive objects from tenant-a only
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		TenantName:     tenantA,
		Where: map[string]any{
			"path":      []string{"category"},
			"operator":  "Equal",
			"valueText": "inactive",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Matches, "should match 1 inactive object in tenant-a")
	assert.Equal(t, 1, result.Deleted, "should delete 1 object")

	// Verify tenant-a has 1 object remaining
	listA, err := tenantListObjectsAuth(t, cls.Class, tenantA, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(1), listA.TotalResults, "tenant-a should have 1 object")

	// Verify tenant-b still has all objects
	listB, err := tenantListObjectsAuth(t, cls.Class, tenantB, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(1), listB.TotalResults, "tenant-b should still have 1 object")
}

// Test 10: Dry run with tenant
func TestDeleteObjects_DryRunWithTenant(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestMultiTenantCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	// Insert objects for tenant-a
	tenantA := "tenant-a"
	createObjectWithTenantAuth(t, cls.Class, map[string]interface{}{
		"data":     "tenant-a data 1",
		"category": "active",
	}, apiKey, tenantA)
	createObjectWithTenantAuth(t, cls.Class, map[string]interface{}{
		"data":     "tenant-a data 2",
		"category": "active",
	}, apiKey, tenantA)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dry run delete all from tenant-a
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		TenantName:     tenantA,
		// No where filter, no dry_run specified (defaults to true)
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DryRun)
	assert.Equal(t, 2, result.Matches, "should match 2 objects")
	assert.Equal(t, 0, result.Deleted, "should not delete in dry run")

	// Verify objects still exist
	list, err := tenantListObjectsAuth(t, cls.Class, tenantA, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(2), list.TotalResults, "should still have 2 objects")
}

// Test 11: Delete with no matching objects
func TestDeleteObjects_NoMatches(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to delete objects with non-existent status
	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "nonexistent",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.Matches, "should match 0 objects")
	assert.Equal(t, 0, result.Deleted, "should delete 0 objects")

	// Verify all objects still exist
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(5), list.TotalResults, "should still have all 5 objects")
}

// Test 12: Failure - Invalid collection name
func TestDeleteObjects_Fail_InvalidCollection(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: "NonExistentCollection",
		DryRun:         &dryRun,
	}, &result)

	// Should fail due to non-existent collection
	assert.NotNil(t, err)
}

// Test 13: Failure - Missing required collection_name
func TestDeleteObjects_Fail_MissingCollectionName(t *testing.T) {
	helper.SetupClient("localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		DryRun: &dryRun,
		// Missing CollectionName
	}, &result)

	// Should fail due to missing collection name
	assert.NotNil(t, err)
}

// Test 14: Failure - Invalid tenant name
func TestDeleteObjects_Fail_InvalidTenant(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestMultiTenantCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		TenantName:     "nonexistent-tenant",
		DryRun:         &dryRun,
	}, &result)

	// Should fail due to non-existent tenant
	assert.NotNil(t, err)
}

// Test 15: Compare dry run vs actual deletion results
func TestDeleteObjects_DryRunVsActualComparison(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filter := map[string]any{
		"path":      []string{"status"},
		"operator":  "Equal",
		"valueText": "published",
	}

	// First do dry run
	var dryRunResult *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where:          filter,
		// dry_run defaults to true
	}, &dryRunResult)

	require.Nil(t, err)
	require.NotNil(t, dryRunResult)
	assert.True(t, dryRunResult.DryRun)
	assert.Equal(t, 3, dryRunResult.Matches)
	assert.Equal(t, 0, dryRunResult.Deleted)

	// Verify objects still exist after dry run
	list1, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(5), list1.TotalResults)

	// Now do actual deletion with same filter
	dryRun := false
	var actualResult *create.DeleteObjectsResp
	err = helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where:          filter,
		DryRun:         &dryRun,
	}, &actualResult)

	require.Nil(t, err)
	require.NotNil(t, actualResult)
	assert.False(t, actualResult.DryRun)
	assert.Equal(t, 3, actualResult.Matches)
	assert.Equal(t, 3, actualResult.Deleted)

	// Verify matches from dry run equals deleted from actual
	assert.Equal(t, dryRunResult.Matches, actualResult.Deleted)

	// Verify objects were actually deleted
	list2, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(2), list2.TotalResults)
}

// Test 16: Delete from empty collection
func TestDeleteObjects_EmptyCollection(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	// Don't insert any objects

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dryRun := false
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		DryRun:         &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.Matches, "should match 0 objects in empty collection")
	assert.Equal(t, 0, result.Deleted, "should delete 0 objects")
}

// Test 17: Verify dry_run=true explicitly
func TestDeleteObjects_ExplicitDryRunTrue(t *testing.T) {
	helper.SetupClient("localhost:8080")
	apiKey := "admin-key"

	cls := createDeleteTestCollection(t, apiKey)
	defer helper.DeleteClassAuth(t, cls.Class, apiKey)

	insertDeleteTestObjects(t, cls.Class, apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Explicitly set dry_run to true
	dryRun := true
	var result *create.DeleteObjectsResp
	err := helper.CallToolOnce(ctx, t, "weaviate-objects-delete", &create.DeleteObjectsArgs{
		CollectionName: cls.Class,
		Where: map[string]any{
			"path":      []string{"status"},
			"operator":  "Equal",
			"valueText": "published",
		},
		DryRun: &dryRun,
	}, &result)

	require.Nil(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DryRun)
	assert.Equal(t, 3, result.Matches)
	assert.Equal(t, 0, result.Deleted)

	// Verify objects still exist
	list, err := helper.ListObjectsAuth(t, cls.Class, apiKey)
	require.Nil(t, err)
	assert.Equal(t, int64(5), list.TotalResults)
}
