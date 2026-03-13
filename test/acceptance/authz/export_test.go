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

package authz

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestExportRBAC(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacRoots(adminUser).WithDbUsers().
		WithBackendS3("bucket", "eu-west-1").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	backend := "s3"
	className := "ExportRBACTest"

	t.Run("authorized user can create, check status, and cancel export", func(t *testing.T) {
		exportID := "export-authorized"
		roleName := "export-manage-role"

		// Create a class as admin
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

		// Create role with manage_backups permission (covers export)
		helper.DeleteRole(t, adminKey, roleName)
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(roleName),
			Permissions: []*models.Permission{
				helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(className).Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		// Create export as custom user — should succeed
		createResp, err := createExportWithAuth(t, backend, exportID, []string{className}, helper.CreateAuth(customKey))
		require.NoError(t, err)
		require.NotNil(t, createResp.Payload)
		require.Equal(t, "STARTED", createResp.Payload.Status)

		// Status as custom user — should succeed
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			statusResp, err := exportStatusWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
			require.NoError(c, err)
			require.NotNil(c, statusResp.Payload)
			status := statusResp.Payload.Status
			require.Truef(c, status == "SUCCESS" || status == "TRANSFERRING" || status == "STARTED",
				"unexpected status: %s (error: %s)", status, statusResp.Payload.Error)
		}, 60*time.Second, 500*time.Millisecond)

		// Cancel — may get 409 if already finished, both are acceptable
		_, cancelErr := cancelExportWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
		if cancelErr != nil {
			require.Contains(t, cancelErr.Error(), "409",
				"expected either success or 409, got: %v", cancelErr)
		}
	})

	t.Run("unauthorized user is forbidden from creating export", func(t *testing.T) {
		exportID := "export-forbidden-create"

		// Create a class as admin
		helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

		// Custom user has no roles — create export should get 403
		_, err := createExportWithAuth(t, backend, exportID, []string{className}, helper.CreateAuth(customKey))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})

	t.Run("unauthorized user is forbidden from getting export status", func(t *testing.T) {
		exportID := "export-forbidden-status"

		// Create a class and export as admin
		helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

		_, err := createExportWithAuth(t, backend, exportID, []string{className}, helper.CreateAuth(adminKey))
		require.NoError(t, err)

		// Wait for export to finish
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := exportStatusWithAuth(t, backend, exportID, helper.CreateAuth(adminKey))
			require.NoError(c, err)
			require.Equal(c, "SUCCESS", resp.Payload.Status)
		}, 60*time.Second, 500*time.Millisecond)

		// Status as unauthorized user — should get 403
		_, err = exportStatusWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})

	t.Run("unauthorized user is forbidden from canceling export", func(t *testing.T) {
		exportID := "export-forbidden-cancel"

		// Create a class and start export as admin
		helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

		_, err := createExportWithAuth(t, backend, exportID, []string{className}, helper.CreateAuth(adminKey))
		require.NoError(t, err)

		// Cancel as unauthorized user — should get 403
		_, err = cancelExportWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})

	t.Run("manage_backups grants status and cancel access", func(t *testing.T) {
		exportID := "export-manage-status-cancel"
		roleName := "export-manage-role-2"

		// Create a class and export as admin
		helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, &models.Class{
			Class:      className,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

		_, err := createExportWithAuth(t, backend, exportID, []string{className}, helper.CreateAuth(adminKey))
		require.NoError(t, err)

		// Wait for export to finish
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := exportStatusWithAuth(t, backend, exportID, helper.CreateAuth(adminKey))
			require.NoError(c, err)
			require.Equal(c, "SUCCESS", resp.Payload.Status)
		}, 60*time.Second, 500*time.Millisecond)

		// Give custom user manage_backups
		helper.DeleteRole(t, adminKey, roleName)
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(roleName),
			Permissions: []*models.Permission{
				helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(className).Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		// Status should work with manage_backups
		statusResp, err := exportStatusWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
		require.NoError(t, err)
		require.Equal(t, "SUCCESS", statusResp.Payload.Status)

		// Cancel on finished export -> 409 (not 403), confirming auth passed
		_, err = cancelExportWithAuth(t, backend, exportID, helper.CreateAuth(customKey))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "409")
	})

	t.Run("cross-collection authorization", func(t *testing.T) {
		roleName := "export-class-a-only"
		classA := "ExportClassA"
		classB := "ExportClassB"

		// Create two classes as admin
		helper.DeleteClassWithAuthz(t, classA, helper.CreateAuth(adminKey))
		helper.DeleteClassWithAuthz(t, classB, helper.CreateAuth(adminKey))
		helper.CreateClassAuth(t, &models.Class{
			Class:      classA,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		helper.CreateClassAuth(t, &models.Class{
			Class:      classB,
			Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		}, adminKey)
		defer helper.DeleteClassWithAuthz(t, classA, helper.CreateAuth(adminKey))
		defer helper.DeleteClassWithAuthz(t, classB, helper.CreateAuth(adminKey))

		// Give custom user manage_backups only for classA
		helper.DeleteRole(t, adminKey, roleName)
		helper.CreateRole(t, adminKey, &models.Role{
			Name: String(roleName),
			Permissions: []*models.Permission{
				helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(classA).Permission(),
			},
		})
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		// Export both classes — should be forbidden because user lacks permission on classB
		_, err := createExportWithAuth(t, backend, "export-cross-both", []string{classA, classB}, helper.CreateAuth(customKey))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")

		// Export only classA — should succeed
		createResp, err := createExportWithAuth(t, backend, "export-cross-a-only", []string{classA}, helper.CreateAuth(customKey))
		require.NoError(t, err)
		require.NotNil(t, createResp.Payload)
	})
}

// createExportWithAuth creates an export with authentication.
func createExportWithAuth(t *testing.T, backend, exportID string, include []string, auth runtime.ClientAuthInfoWriter) (*export.ExportCreateOK, error) {
	t.Helper()
	fileFormat := models.ExportCreateRequestFileFormatParquet
	params := export.NewExportCreateParams().
		WithBackend(backend).
		WithBody(&models.ExportCreateRequest{
			ID:         &exportID,
			Include:    include,
			FileFormat: &fileFormat,
		})
	return helper.Client(t).Export.ExportCreate(params, auth)
}

// exportStatusWithAuth gets export status with authentication.
func exportStatusWithAuth(t *testing.T, backend, exportID string, auth runtime.ClientAuthInfoWriter) (*export.ExportStatusOK, error) {
	t.Helper()
	params := export.NewExportStatusParams().
		WithBackend(backend).
		WithID(exportID)
	return helper.Client(t).Export.ExportStatus(params, auth)
}

// cancelExportWithAuth cancels an export with authentication.
func cancelExportWithAuth(t *testing.T, backend, exportID string, auth runtime.ClientAuthInfoWriter) (*export.ExportCancelNoContent, error) {
	t.Helper()
	params := export.NewExportCancelParams().
		WithBackend(backend).
		WithID(exportID)
	return helper.Client(t).Export.ExportCancel(params, auth)
}
