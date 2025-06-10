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

package helper

import (
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func CreateRole(t *testing.T, key string, role *models.Role) {
	t.Helper()
	resp, err := Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func GetRoles(t *testing.T, key string) []*models.Role {
	resp, err := Client(t).Authz.GetRoles(authz.NewGetRolesParams(), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func GetRolesForUser(t *testing.T, user, key string, includeRoles bool) []*models.Role {
	t.Helper()
	userType := models.UserTypeInputDb
	resp, err := Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(user).WithUserType(string(userType)).WithIncludeFullRoles(&includeRoles), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func GetRolesForUserOIDC(t *testing.T, user, key string) []*models.Role {
	t.Helper()
	truep := true
	userType := models.UserTypeInputOidc
	resp, err := Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(user).WithUserType(string(userType)).WithIncludeFullRoles(&truep), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func GetUserForRoles(t *testing.T, roleName, key string) []string {
	t.Helper()
	resp, err := Client(t).Authz.GetUsersForRole(authz.NewGetUsersForRoleParams().WithID(roleName), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	userIds := make([]string, 0, len(resp.Payload))
	for _, user := range resp.Payload {
		if *user.UserType == models.UserTypeOutputOidc {
			continue
		}
		userIds = append(userIds, user.UserID)
	}
	return userIds
}

func GetUserForRolesBoth(t *testing.T, roleName, key string) []*authz.GetUsersForRoleOKBodyItems0 {
	t.Helper()
	resp, err := Client(t).Authz.GetUsersForRole(authz.NewGetUsersForRoleParams().WithID(roleName), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func GetInfoForOwnUser(t *testing.T, key string) *models.UserOwnInfo {
	t.Helper()
	resp, err := Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func DeleteUser(t *testing.T, userId, key string) {
	t.Helper()
	resp, err := Client(t).Users.DeleteUser(users.NewDeleteUserParams().WithUserID(userId), CreateAuth(key))
	if err != nil {
		var parsed *users.DeleteUserNotFound
		require.True(t, errors.As(err, &parsed))
	} else {
		AssertRequestOk(t, resp, err, nil)
		require.Nil(t, err)
	}
}

func GetUser(t *testing.T, userId, key string) *models.DBUserInfo {
	t.Helper()
	resp, err := Client(t).Users.GetUserInfo(users.NewGetUserInfoParams().WithUserID(userId), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func GetUserWithLastUsedTime(t *testing.T, userId, key string, lastUsedTime bool) *models.DBUserInfo {
	t.Helper()
	resp, err := Client(t).Users.GetUserInfo(users.NewGetUserInfoParams().WithUserID(userId).WithIncludeLastUsedTime(&lastUsedTime), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func CreateUser(t *testing.T, userId, key string) string {
	t.Helper()
	resp, err := Client(t).Users.CreateUser(users.NewCreateUserParams().WithUserID(userId), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Apikey)
	return *resp.Payload.Apikey
}

func CreateUserWithApiKey(t *testing.T, userId, key string, createdAt *time.Time) string {
	t.Helper()
	tp := true
	if createdAt == nil {
		createdAt = &time.Time{}
	}

	resp, err := Client(t).Users.CreateUser(users.NewCreateUserParams().WithUserID(userId).WithBody(users.CreateUserBody{Import: &tp, CreateTime: strfmt.DateTime(*createdAt)}), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Apikey)
	return *resp.Payload.Apikey
}

func RotateKey(t *testing.T, userId, key string) string {
	t.Helper()
	resp, err := Client(t).Users.RotateUserAPIKey(users.NewRotateUserAPIKeyParams().WithUserID(userId), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Apikey)
	return *resp.Payload.Apikey
}

func DeactivateUser(t *testing.T, key, userId string, revokeKey bool) {
	t.Helper()
	resp, err := Client(t).Users.DeactivateUser(users.NewDeactivateUserParams().WithUserID(userId).WithBody(users.DeactivateUserBody{RevokeKey: &revokeKey}), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.NoError(t, err)
}

func ActivateUser(t *testing.T, key, userId string) {
	t.Helper()
	resp, err := Client(t).Users.ActivateUser(users.NewActivateUserParams().WithUserID(userId), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.NoError(t, err)
}

func ListAllUsers(t *testing.T, key string) []*models.DBUserInfo {
	t.Helper()
	resp, err := Client(t).Users.ListAllUsers(users.NewListAllUsersParams(), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func ListAllUsersWithIncludeTime(t *testing.T, key string, includeLastUsedTime bool) []*models.DBUserInfo {
	t.Helper()
	resp, err := Client(t).Users.ListAllUsers(users.NewListAllUsersParams().WithIncludeLastUsedTime(&includeLastUsedTime), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	return resp.Payload
}

func DeleteRole(t *testing.T, key, role string) {
	t.Helper()
	resp, err := Client(t).Authz.DeleteRole(authz.NewDeleteRoleParams().WithID(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func GetRoleByName(t *testing.T, key, role string) *models.Role {
	t.Helper()
	resp, err := Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

func AssignRoleToUser(t *testing.T, key, role, user string) {
	t.Helper()
	userType := models.UserTypeInputDb
	resp, err := Client(t).Authz.AssignRoleToUser(
		authz.NewAssignRoleToUserParams().WithID(user).WithBody(authz.AssignRoleToUserBody{Roles: []string{role}, UserType: userType}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func AssignRoleToUserOIDC(t *testing.T, key, role, user string) {
	t.Helper()
	userType := models.UserTypeInputOidc
	resp, err := Client(t).Authz.AssignRoleToUser(
		authz.NewAssignRoleToUserParams().WithID(user).WithBody(authz.AssignRoleToUserBody{Roles: []string{role}, UserType: userType}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func RevokeRoleFromUser(t *testing.T, key, role, user string) {
	userType := models.UserTypeInputDb

	resp, err := Client(t).Authz.RevokeRoleFromUser(
		authz.NewRevokeRoleFromUserParams().WithID(user).WithBody(authz.RevokeRoleFromUserBody{Roles: []string{role}, UserType: userType}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func AssignRoleToGroup(t *testing.T, key, role, group string) {
	resp, err := Client(t).Authz.AssignRoleToGroup(
		authz.NewAssignRoleToGroupParams().WithID(group).WithBody(authz.AssignRoleToGroupBody{Roles: []string{role}}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func RevokeRoleFromGroup(t *testing.T, key, role, group string) {
	resp, err := Client(t).Authz.RevokeRoleFromGroup(
		authz.NewRevokeRoleFromGroupParams().WithID(group).WithBody(authz.RevokeRoleFromGroupBody{Roles: []string{role}}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func AddPermissions(t *testing.T, key, role string, permissions ...*models.Permission) {
	resp, err := Client(t).Authz.AddPermissions(
		authz.NewAddPermissionsParams().WithID(role).WithBody(authz.AddPermissionsBody{
			Permissions: permissions,
		}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func RemovePermissions(t *testing.T, key, role string, permissions ...*models.Permission) {
	resp, err := Client(t).Authz.RemovePermissions(
		authz.NewRemovePermissionsParams().WithID(role).WithBody(authz.RemovePermissionsBody{
			Permissions: permissions,
		}),
		CreateAuth(key),
	)
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

type BackupPermission models.Permission

func NewBackupPermission() *BackupPermission {
	return &BackupPermission{}
}

func (p *BackupPermission) WithAction(action string) *BackupPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *BackupPermission) WithCollection(collection string) *BackupPermission {
	if p.Backups == nil {
		p.Backups = &models.PermissionBackups{}
	}
	p.Backups.Collection = authorization.String(collection)
	return p
}

func (p *BackupPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}

type CollectionsPermission models.Permission

func NewCollectionsPermission() *CollectionsPermission {
	return &CollectionsPermission{}
}

func (p *CollectionsPermission) WithAction(action string) *CollectionsPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *CollectionsPermission) WithCollection(collection string) *CollectionsPermission {
	if p.Collections == nil {
		p.Collections = &models.PermissionCollections{}
	}
	p.Collections.Collection = authorization.String(collection)
	return p
}

func (p *CollectionsPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}

type TenantsPermission models.Permission

func NewTenantsPermission() *TenantsPermission {
	return &TenantsPermission{}
}

func (p *TenantsPermission) WithAction(action string) *TenantsPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *TenantsPermission) WithCollection(collection string) *TenantsPermission {
	if p.Tenants == nil {
		p.Tenants = &models.PermissionTenants{}
	}
	p.Tenants.Collection = authorization.String(collection)
	return p
}

func (p *TenantsPermission) WithTenant(tenant string) *TenantsPermission {
	if p.Tenants == nil {
		p.Tenants = &models.PermissionTenants{}
	}
	p.Tenants.Tenant = authorization.String(tenant)
	return p
}

func (p *TenantsPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}

type DataPermission models.Permission

func NewDataPermission() *DataPermission {
	return &DataPermission{}
}

func (p *DataPermission) WithAction(action string) *DataPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *DataPermission) WithCollection(collection string) *DataPermission {
	if p.Data == nil {
		p.Data = &models.PermissionData{}
	}
	p.Data.Collection = authorization.String(collection)
	return p
}

func (p *DataPermission) WithTenant(tenant string) *DataPermission {
	if p.Data == nil {
		p.Data = &models.PermissionData{}
	}
	p.Data.Tenant = authorization.String(tenant)
	return p
}

func (p *DataPermission) WithObject(object string) *DataPermission {
	if p.Data == nil {
		p.Data = &models.PermissionData{}
	}
	p.Data.Object = authorization.String(object)
	return p
}

func (p *DataPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}

type NodesPermission models.Permission

func NewNodesPermission() *NodesPermission {
	return &NodesPermission{}
}

func (p *NodesPermission) WithAction(action string) *NodesPermission {
	p.Action = authorization.String(action)
	return p
}

func (p *NodesPermission) WithVerbosity(verbosity string) *NodesPermission {
	if p.Nodes == nil {
		p.Nodes = &models.PermissionNodes{}
	}
	p.Nodes.Verbosity = authorization.String(verbosity)
	return p
}

func (p *NodesPermission) WithCollection(collection string) *NodesPermission {
	if p.Nodes == nil {
		p.Nodes = &models.PermissionNodes{}
	}
	p.Nodes.Collection = authorization.String(collection)
	return p
}

func (p *NodesPermission) Permission() *models.Permission {
	perm := models.Permission(*p)
	return &perm
}
