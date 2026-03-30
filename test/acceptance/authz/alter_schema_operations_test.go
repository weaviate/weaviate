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

func TestAuthzDeleteClassPropertyIndex(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUp(t,
		map[string]string{adminUser: adminKey},
		map[string]string{customUser: customKey},
		nil,
	)
	defer down()

	className := "AuthzDeletePropertyIndex"
	propName := "title"
	deleteObjectClass(t, className, adminAuth)

	ptrBool := func(b bool) *bool { return &b }
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            propName,
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: ptrBool(true),
				IndexSearchable: ptrBool(true),
			},
		},
	}
	helper.CreateClassAuth(t, c, adminKey)
	defer deleteObjectClass(t, className, adminAuth)

	deletePropertyIndex := func(propertyName, indexName, key string) error {
		params := clschema.NewSchemaObjectsPropertiesDeleteParams().
			WithClassName(className).
			WithPropertyName(propertyName).
			WithIndexName(indexName)
		_, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(params, helper.CreateAuth(key))
		return err
	}

	t.Run("fail to delete property index without any permission", func(t *testing.T) {
		err := deletePropertyIndex(propName, "filterable", customKey)
		require.NotNil(t, err)
		var forbidden *clschema.SchemaObjectsPropertiesDeleteForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("fail to delete property index with only read_collections permission", func(t *testing.T) {
		roleName := "readCollectionsOnly"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, adminKey, role)
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		err := deletePropertyIndex(propName, "filterable", customKey)
		require.NotNil(t, err)
		var forbidden *clschema.SchemaObjectsPropertiesDeleteForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	t.Run("succeed to delete filterable index with update_collections permission", func(t *testing.T) {
		roleName := "updateCollections"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, adminKey, role)
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		err := deletePropertyIndex(propName, "filterable", customKey)
		require.Nil(t, err)
	})

	t.Run("succeed to delete searchable index with update_collections permission", func(t *testing.T) {
		roleName := "updateCollections"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, adminKey, role)
		defer helper.DeleteRole(t, adminKey, roleName)
		helper.AssignRoleToUser(t, adminKey, roleName, customUser)
		defer helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)

		err := deletePropertyIndex(propName, "searchable", customKey)
		require.Nil(t, err)
	})
}
