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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
)

func CreateRole(t *testing.T, key string, role *models.Role) {
	t.Helper()
	resp, err := Client(t).Authz.CreateRole(authz.NewCreateRoleParams().WithBody(role), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func GetRoles(t *testing.T, key string) []*models.Role {
	t.Helper()
	resp, err := Client(t).Authz.GetRoles(authz.NewGetRolesParams(), CreateAuth(key))
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
	resp, err := Client(t).Authz.AssignRole(authz.NewAssignRoleParams().WithID(user).WithBody(authz.AssignRoleBody{Roles: []string{role}}), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}

func RevokeRoleFromUser(t *testing.T, key, role, user string) {
	t.Helper()
	resp, err := Client(t).Authz.RevokeRole(authz.NewRevokeRoleParams().WithID(user).WithBody(authz.RevokeRoleBody{Roles: []string{role}}), CreateAuth(key))
	AssertRequestOk(t, resp, err, nil)
	require.Nil(t, err)
}
