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
