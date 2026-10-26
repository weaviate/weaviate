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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthzReindexIndexEndpointsDeny pins the RBAC deny contract for the
// per-property index endpoints: mutations require UPDATE (always 403, never a
// leaking 404/422); GET requires READ. Also covers read-only and
// wrong-collection grants not authorizing the target.
func TestAuthzReindexIndexEndpointsDeny(t *testing.T) {
	adminAuth := helper.CreateAuth(sharedRootKey)
	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpShared(t)
	defer down()

	targetClass := "AuthzReindexTarget"
	otherClass := "AuthzReindexOther"
	propName := "title"
	indexType := "filterable"

	ptrBool := func(b bool) *bool { return &b }
	makeClass := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{
				{
					Name:            propName,
					DataType:        schema.DataTypeText.PropString(),
					IndexFilterable: ptrBool(true),
					IndexSearchable: ptrBool(true),
				},
			},
		}
	}

	deleteObjectClass(t, targetClass, adminAuth)
	deleteObjectClass(t, otherClass, adminAuth)
	helper.CreateClassAuth(t, makeClass(targetClass), sharedRootKey)
	defer deleteObjectClass(t, targetClass, adminAuth)
	helper.CreateClassAuth(t, makeClass(otherClass), sharedRootKey)
	defer deleteObjectClass(t, otherClass, adminAuth)

	// Each entry calls one mutation endpoint and reports whether it came back
	// 403 Forbidden. The Forbidden type is per-operation, so errors.As lives
	// inside each closure.
	type endpoint struct {
		name string
		call func(key string) (err error, forbidden bool)
	}
	mutations := []endpoint{
		{
			name: "PUT .../index/{indexType} (upsert)",
			call: func(key string) (error, bool) {
				params := clschema.NewSchemaObjectsIndexUpsertParams().
					WithClassName(targetClass).WithPropertyName(propName).
					WithIndexName(indexType).WithBody(&models.IndexUpsertRequest{})
				_, _, err := helper.Client(t).Schema.SchemaObjectsIndexUpsert(params, helper.CreateAuth(key))
				var f *clschema.SchemaObjectsIndexUpsertForbidden
				return err, errors.As(err, &f)
			},
		},
		{
			name: "POST .../index/{indexType}/rebuild",
			call: func(key string) (error, bool) {
				params := clschema.NewSchemaObjectsIndexRebuildParams().
					WithClassName(targetClass).WithPropertyName(propName).WithIndexName(indexType)
				_, err := helper.Client(t).Schema.SchemaObjectsIndexRebuild(params, helper.CreateAuth(key))
				var f *clschema.SchemaObjectsIndexRebuildForbidden
				return err, errors.As(err, &f)
			},
		},
		{
			name: "POST .../index/{indexType}/cancel",
			call: func(key string) (error, bool) {
				params := clschema.NewSchemaObjectsIndexCancelParams().
					WithClassName(targetClass).WithPropertyName(propName).WithIndexName(indexType)
				_, err := helper.Client(t).Schema.SchemaObjectsIndexCancel(params, helper.CreateAuth(key))
				var f *clschema.SchemaObjectsIndexCancelForbidden
				return err, errors.As(err, &f)
			},
		},
		{
			name: "DELETE .../index/{indexType}",
			call: func(key string) (error, bool) {
				params := clschema.NewSchemaObjectsPropertiesDeleteParams().
					WithClassName(targetClass).WithPropertyName(propName).WithIndexName(indexType)
				_, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(params, helper.CreateAuth(key))
				var f *clschema.SchemaObjectsPropertiesDeleteForbidden
				return err, errors.As(err, &f)
			},
		},
	}

	getIndexes := func(class, key string) (error, bool) {
		params := clschema.NewSchemaObjectsIndexesGetParams().WithClassName(class)
		_, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(params, helper.CreateAuth(key))
		var f *clschema.SchemaObjectsIndexesGetForbidden
		return err, errors.As(err, &f)
	}

	t.Run("permissionless principal is denied on every endpoint", func(t *testing.T) {
		for _, ep := range mutations {
			err, forbidden := ep.call(customKey)
			require.Error(t, err, "%s: a permissionless principal must be denied", ep.name)
			require.True(t, forbidden, "%s: expected 403 Forbidden, got %v", ep.name, err)
		}
		err, forbidden := getIndexes(targetClass, customKey)
		require.Error(t, err, "GET .../indexes: a permissionless principal must be denied")
		require.True(t, forbidden, "GET .../indexes: expected 403 Forbidden, got %v", err)
	})

	t.Run("read_collections is insufficient for mutations but authorizes GET", func(t *testing.T) {
		roleName := "reindexReadOnly"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(targetClass).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		for _, ep := range mutations {
			err, forbidden := ep.call(customKey)
			require.Error(t, err, "%s: read_collections must not authorize a mutation", ep.name)
			require.True(t, forbidden, "%s: expected 403 Forbidden with a read-only role, got %v", ep.name, err)
		}

		// Positive control: read_collections DOES authorize GET, proving the
		// 403s above are a real deny, not a blanket reject.
		err, forbidden := getIndexes(targetClass, customKey)
		require.False(t, forbidden, "GET .../indexes must not be forbidden with read_collections")
		require.NoError(t, err, "GET .../indexes must succeed with read_collections, got %v", err)
	})

	t.Run("permissions scoped to another collection do not authorize the target", func(t *testing.T) {
		roleName := "reindexOtherCollection"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(otherClass).Permission(),
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(otherClass).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		// The principal is fully authorized for otherClass (read + update
		// collections) but every call below targets targetClass.
		for _, ep := range mutations {
			err, forbidden := ep.call(customKey)
			require.Error(t, err, "%s: a grant scoped to another collection must not authorize the target", ep.name)
			require.True(t, forbidden, "%s: expected 403 Forbidden, got %v", ep.name, err)
		}
		err, forbidden := getIndexes(targetClass, customKey)
		require.Error(t, err, "GET .../indexes: a read grant on another collection must not authorize the target")
		require.True(t, forbidden, "GET .../indexes: expected 403 Forbidden, got %v", err)
	})

	t.Run("update_collections alone is insufficient (update_data required)", func(t *testing.T) {
		roleName := "reindexMetaOnly"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(targetClass).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		// All four index write verbs demand Collections (data + metadata);
		// metadata alone must still 403.
		for _, ep := range mutations {
			err, forbidden := ep.call(customKey)
			require.Error(t, err, "%s: update_collections without update_data must not authorize a mutation", ep.name)
			require.True(t, forbidden, "%s: expected 403 without update_data, got %v", ep.name, err)
		}
	})

	// LAST subtest: the authorized mutations execute and mutate the target
	// (DELETE drops the index), so nothing after may depend on class state.
	t.Run("update_collections + update_data authorizes every mutation", func(t *testing.T) {
		roleName := "reindexDataAndMeta"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(targetClass).Permission(),
				helper.NewDataPermission().WithAction(authorization.UpdateData).WithCollection(targetClass).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		// Not 403: a downstream 202/400/409 is fine — only authz is under test.
		for _, ep := range mutations {
			_, forbidden := ep.call(customKey)
			require.False(t, forbidden,
				"%s: update_collections + update_data must authorize the mutation (not 403)", ep.name)
		}
	})
}
