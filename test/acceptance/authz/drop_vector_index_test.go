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
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthzDeleteClassVectorIndex pins the endpoint's authorization scope:
// dropping a named vector index irreversibly rewrites every object in the
// collection, so it demands Collections (data+metadata) like
// DeleteClassPropertyIndex — update_collections (metadata-only) alone must
// be denied. Regression pin for the CollectionsMetadata→Collections
// tightening.
func TestAuthzDeleteClassVectorIndex(t *testing.T) {
	adminAuth := helper.CreateAuth(sharedRootKey)

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpShared(t)
	defer down()

	className := "AuthzDeleteVectorIndex"
	deleteObjectClass(t, className, adminAuth)

	noneVectorizer := map[string]any{"none": map[string]any{}}
	c := &models.Class{
		Class: className,
		// Two named vectors: dropping the only vector of a class is rejected
		// outright, which would mask the authz verdict under test.
		VectorConfig: map[string]models.VectorConfig{
			"toDrop":  {Vectorizer: noneVectorizer, VectorIndexType: "hnsw"},
			"sibling": {Vectorizer: noneVectorizer, VectorIndexType: "hnsw"},
		},
	}
	helper.CreateClassAuth(t, c, sharedRootKey)
	defer deleteObjectClass(t, className, adminAuth)

	dropVectorIndex := func(targetVector, key string) error {
		params := clschema.NewSchemaObjectsVectorsDeleteParams().
			WithClassName(className).
			WithVectorIndexName(targetVector)
		_, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, helper.CreateAuth(key))
		return err
	}

	t.Run("fail without any permission", func(t *testing.T) {
		err := dropVectorIndex("toDrop", customKey)
		require.Error(t, err)
		var forbidden *clschema.SchemaObjectsVectorsDeleteForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("fail with only update_collections permission", func(t *testing.T) {
		roleName := "updateCollectionsOnly"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		err := dropVectorIndex("toDrop", customKey)
		require.Error(t, err, "metadata-only update must not authorize a data-rewriting drop")
		var forbidden *clschema.SchemaObjectsVectorsDeleteForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("succeed with update_collections + update_data permission", func(t *testing.T) {
		roleName := "updateCollectionsAndData"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
				helper.NewDataPermission().WithAction(authorization.UpdateData).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, role)
		defer helper.DeleteRole(t, sharedRootKey, roleName)
		helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

		require.NoError(t, dropVectorIndex("toDrop", customKey))
	})
}

// TestAuthzUpdateClassVectorConfigRemoval pins the generic-update side of the
// same hardened surface: PUT /v1/schema/{class} with a VectorConfig entry
// missing reaches the identical removal checkpoint the drop endpoint guards,
// so it demands the drop endpoint's scope (Collections, not metadata-only).
// An update that keeps every entry must NOT be escalated.
func TestAuthzUpdateClassVectorConfigRemoval(t *testing.T) {
	adminAuth := helper.CreateAuth(sharedRootKey)

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpShared(t)
	defer down()

	className := "AuthzUpdateVectorRemoval"
	deleteObjectClass(t, className, adminAuth)

	noneVectorizer := map[string]any{"none": map[string]any{}}
	c := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"toRemove": {Vectorizer: noneVectorizer, VectorIndexType: "hnsw"},
			"sibling":  {Vectorizer: noneVectorizer, VectorIndexType: "hnsw"},
		},
	}
	helper.CreateClassAuth(t, c, sharedRootKey)
	defer deleteObjectClass(t, className, adminAuth)

	roleName := "updateCollectionsOnlyPut"
	role := &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
		},
	}
	helper.CreateRole(t, sharedRootKey, role)
	defer helper.DeleteRole(t, sharedRootKey, roleName)
	helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
	defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

	t.Run("metadata-only may update without removing entries", func(t *testing.T) {
		full := helper.GetClassAuth(t, className, sharedRootKey)
		full.Description = "still all vectors present"
		_, err := helper.UpdateClassAuthWithReturn(t, className, full, customKey)
		require.NoError(t, err, "no escalation may fire when every VectorConfig entry is kept")
	})

	t.Run("metadata-only is denied a vector-entry removal", func(t *testing.T) {
		stripped := helper.GetClassAuth(t, className, sharedRootKey)
		delete(stripped.VectorConfig, "toRemove")
		_, err := helper.UpdateClassAuthWithReturn(t, className, stripped, customKey)
		require.Error(t, err, "metadata-only update must not authorize a vector-entry removal")
		var forbidden *clschema.SchemaObjectsUpdateForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("collections + data scope passes the authz checkpoint", func(t *testing.T) {
		dataRoleName := "updateCollectionsAndDataPut"
		dataRole := &models.Role{
			Name: &dataRoleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
				helper.NewDataPermission().WithAction(authorization.UpdateData).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, sharedRootKey, dataRole)
		defer helper.DeleteRole(t, sharedRootKey, dataRoleName)
		helper.AssignRoleToUser(t, sharedRootKey, dataRoleName, customUser)
		defer helper.RevokeRoleFromUser(t, sharedRootKey, dataRoleName, customUser)

		stripped := helper.GetClassAuth(t, className, sharedRootKey)
		delete(stripped.VectorConfig, "toRemove")
		_, err := helper.UpdateClassAuthWithReturn(t, className, stripped, customKey)
		// The request must clear the authz checkpoint and proceed to
		// validation: removing a LIVE entry is then rejected by the parser's
		// immutability check as 422 — never 403. (A successful removal needs
		// a completed drop, which the drop_vector_index suite covers; a
		// stable "none" entry cannot be staged here because an empty class
		// finalizes immediately.)
		require.Error(t, err)
		var unprocessable *clschema.SchemaObjectsUpdateUnprocessableEntity
		require.True(t, errors.As(err, &unprocessable),
			"expected the parser's 422, got %v (a 403 would mean the authz checkpoint misfired)", err)
	})
}
