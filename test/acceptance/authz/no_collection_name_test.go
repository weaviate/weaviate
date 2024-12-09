package test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Test create + delete - update does not seem to work without classname and we should not fix it
func TestWithoutCollectionName(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	readDataAction := authorization.ReadData
	deleteDataAction := authorization.DeleteData
	readCollectionsAction := authorization.ReadCollections
	testRoleName := t.Name() + "role"
	all := "*"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	// add classes with object
	className := t.Name() + "class"

	deleteObjectClass(t, className, adminAuth)
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "prop",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	helper.CreateClassAuth(t, c, adminKey)

	obj := &models.Object{
		ID:    UUID2,
		Class: className,
		Properties: map[string]interface{}{
			"prop": "test",
		},
	}
	err := helper.CreateObjectAuth(t, obj, adminKey)
	require.NoError(t, err)

	getPermissionsClass := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &className},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &className},
		},
	}
	t.Run("Test get object - fail", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: getPermissionsClass}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := getObject(t, UUID2, customKey)
		require.Error(t, err)
		require.Nil(t, res)
	})

	getPermissionsAll := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
	}
	t.Run("Test get object - succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: getPermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := getObject(t, UUID2, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	deletePermissionsClass := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &className},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &className},
		},
	}
	t.Run("delete object without collection name fail", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: deletePermissionsClass}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		obj2 := &models.Object{
			ID: UUID2,
			Properties: map[string]interface{}{
				"prop": "updated",
			},
		}
		res, err := updateObject(t, UUID2, obj2, customKey)
		require.Error(t, err)
		require.Nil(t, res)
	})

	deletePermissionsAll := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:      &readCollectionsAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
	}
	t.Run("delete object without collection name succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: deletePermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := deleteObject(t, UUID2, customKey)
		require.Error(t, err)
		require.Nil(t, res)
	})
}
