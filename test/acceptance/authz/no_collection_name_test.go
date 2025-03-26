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

package authz

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/client/objects"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"

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
	readTenantAction := authorization.ReadTenants
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
			Action:  &readTenantAction,
			Tenants: &models.PermissionTenants{Collection: &className},
		},
	}
	t.Run("Test get object - fail", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: getPermissionsClass}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := getObjectDeprecated(t, UUID2, customKey)
		require.Error(t, err)
		var unauthorized *objects.ObjectsGetForbidden
		require.True(t, errors.As(err, &unauthorized))

		require.Nil(t, res)
	})

	getPermissionsAll := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:  &readTenantAction,
			Tenants: &models.PermissionTenants{Collection: &all},
		},
	}
	t.Run("Test get object - succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: getPermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := getObjectDeprecated(t, UUID2, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	deletePermissionsClass := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &className},
		},
		{
			Action:  &readTenantAction,
			Tenants: &models.PermissionTenants{Collection: &className},
		},
	}
	t.Run("delete object without collection name fail", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: deletePermissionsClass}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := deleteObjectDeprecated(t, UUID2, customKey)
		require.Error(t, err)
		var unauthorized *objects.ObjectsDeleteForbidden
		require.True(t, errors.As(err, &unauthorized))

		require.Nil(t, res)
	})

	deletePermissionsAll := []*models.Permission{
		{
			Action: &deleteDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:  &readTenantAction,
			Tenants: &models.PermissionTenants{Collection: &all},
		},
	}
	t.Run("delete object without collection name succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: deletePermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		res, err := deleteObjectDeprecated(t, UUID2, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})
}

func TestRefsWithoutCollectionNames(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := t.Name() + "role"

	readDataAction := authorization.ReadData
	updateDataAction := authorization.UpdateData
	readCollectionAction := authorization.ReadCollections
	all := "*"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	articlesCls := articles.ArticlesClass()
	paragraphsCls := articles.ParagraphsClass()

	helper.DeleteClassWithAuthz(t, paragraphsCls.Class, adminAuth)
	helper.DeleteClassWithAuthz(t, articlesCls.Class, adminAuth)
	helper.CreateClassAuth(t, paragraphsCls, adminKey)
	helper.CreateClassAuth(t, articlesCls, adminKey)
	defer helper.DeleteClassWithAuthz(t, paragraphsCls.Class, adminAuth)
	defer helper.DeleteClassWithAuthz(t, articlesCls.Class, adminAuth)

	objs := []*models.Object{articles.NewParagraph().WithID(UUID1).Object(), articles.NewParagraph().WithID(UUID2).Object()}
	objs = append(objs, articles.NewArticle().WithTitle("Article 1").WithID(UUID3).Object())
	objs = append(objs, articles.NewArticle().WithTitle("Article 2").WithID(UUID4).Object())
	helper.CreateObjectsBatchAuth(t, objs, adminKey)

	addrefPermissionsClass := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &paragraphsCls.Class},
		},
	}
	t.Run("Test add ref only class permissions - fail", func(t *testing.T) {
		role := &models.Role{Name: &testRoleName, Permissions: addrefPermissionsClass}
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := addRef(t, UUID3, "hasParagraphs", ref, customKey)
		require.Error(t, err)
		var unauthorized *objects.ObjectsReferencesCreateForbidden
		require.True(t, errors.As(err, &unauthorized))

		require.Nil(t, res)
	})

	addrefPermissionsAll := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
	}
	t.Run("Test add ref all permissions - succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: addrefPermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := addRef(t, UUID3, "hasParagraphs", ref, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	for _, permissions := range generateMissingLists(addrefPermissionsAll) {
		t.Run("Test add ref - missing permissions", func(t *testing.T) {
			deleteRole := &models.Role{Name: &testRoleName, Permissions: permissions}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

			ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
			res, err := addRef(t, UUID3, "hasParagraphs", ref, customKey)
			require.Error(t, err)
			var unauthorized *objects.ObjectsReferencesCreateForbidden
			require.True(t, errors.As(err, &unauthorized))

			require.Nil(t, res)
		})
	}

	updaterefPermissionsClass := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &paragraphsCls.Class},
		},
	}
	t.Run("Test add ref only class permissions - fail", func(t *testing.T) {
		role := &models.Role{Name: &testRoleName, Permissions: updaterefPermissionsClass}
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := updateRef(t, UUID3, "hasParagraphs", ref, customKey)
		var unauthorized *objects.ObjectsReferencesUpdateForbidden
		require.True(t, errors.As(err, &unauthorized))

		require.Error(t, err)
		require.Nil(t, res)
	})

	updaterefPermissionsAll := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
	}
	t.Run("Test update ref all permissions - succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: updaterefPermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := updateRef(t, UUID3, "hasParagraphs", ref, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	t.Run("Test update ref - missing permissions", func(t *testing.T) {
		for _, permissions := range generateMissingLists(updaterefPermissionsAll) {
			deleteRole := &models.Role{Name: &testRoleName, Permissions: permissions}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

			ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
			res, err := updateRef(t, UUID3, "hasParagraphs", ref, customKey)
			require.Error(t, err)
			var unauthorized *objects.ObjectsReferencesUpdateForbidden
			require.True(t, errors.As(err, &unauthorized))

			require.Nil(t, res)
		}
	})

	deleterefPermissionsClass := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &articlesCls.Class},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &paragraphsCls.Class},
		},
	}
	t.Run("Test delete ref only class permissions - fail", func(t *testing.T) {
		role := &models.Role{Name: &testRoleName, Permissions: deleterefPermissionsClass}
		helper.DeleteRole(t, adminKey, *role.Name)
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := deleteRef(t, UUID3, "hasParagraphs", ref, customKey)
		require.Error(t, err)
		var unauthorized *objects.ObjectsReferencesDeleteForbidden
		require.True(t, errors.As(err, &unauthorized))

		require.Nil(t, res)
	})

	deleterefPermissionsAll := []*models.Permission{
		{
			Action: &readDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action: &updateDataAction,
			Data:   &models.PermissionData{Collection: &all},
		},
		{
			Action:      &readCollectionAction,
			Collections: &models.PermissionCollections{Collection: &all},
		},
	}
	t.Run("Test delete ref all permissions - succeed", func(t *testing.T) {
		deleteRole := &models.Role{Name: &testRoleName, Permissions: deleterefPermissionsAll}
		helper.DeleteRole(t, adminKey, *deleteRole.Name)
		helper.CreateRole(t, adminKey, deleteRole)
		helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

		ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
		res, err := deleteRef(t, UUID3, "hasParagraphs", ref, customKey)
		require.NoError(t, err)
		require.NotNil(t, res)
	})

	t.Run("Test delete ref - missing permissions", func(t *testing.T) {
		for _, permissions := range generateMissingLists(deleterefPermissionsAll) {
			deleteRole := &models.Role{Name: &testRoleName, Permissions: permissions}
			helper.DeleteRole(t, adminKey, *deleteRole.Name)
			helper.CreateRole(t, adminKey, deleteRole)
			helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)

			ref := &models.SingleRef{Beacon: strfmt.URI(fmt.Sprintf(beaconStart+"%s", UUID1.String()))}
			res, err := deleteRef(t, UUID3, "hasParagraphs", ref, customKey)
			require.Error(t, err)
			var unauthorized *objects.ObjectsReferencesDeleteForbidden
			require.True(t, errors.As(err, &unauthorized))

			require.Nil(t, res)
		}
	})
}
