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

package rbac

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

// policiesOf converts permissions the way the REST path does.
func policiesOf(t *testing.T, perms ...*models.Permission) []authorization.Policy {
	t.Helper()
	ps, err := conv.PermissionToPolicies(perms...)
	require.NoError(t, err)
	out := make([]authorization.Policy, len(ps))
	for i, p := range ps {
		out[i] = *p
	}
	return out
}

func backupsPermission(b *models.PermissionBackups) *models.Permission {
	return &models.Permission{Action: authorization.String(authorization.ManageBackups), Backups: b}
}

// applyUpsert applies an upsert command at the latest command version.
func applyUpsert(t *testing.T, m *Manager, roles map[string][]authorization.Policy) {
	t.Helper()
	sub, err := json.Marshal(&cmd.CreateRolesRequest{Roles: roles, Version: cmd.RBACLatestCommandPolicyVersion})
	require.NoError(t, err)
	require.NoError(t, m.UpsertRolesPermissions(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, SubCommand: sub}))
}

// applyRemove applies a remove command at the latest command version.
func applyRemove(t *testing.T, m *Manager, role string, policies ...authorization.Policy) {
	t.Helper()
	ps := make([]*authorization.Policy, len(policies))
	for i := range policies {
		ps[i] = &policies[i]
	}
	sub, err := json.Marshal(&cmd.RemovePermissionsRequest{Role: role, Permissions: ps, Version: cmd.RBACLatestCommandPolicyVersion})
	require.NoError(t, err)
	require.NoError(t, m.RemovePermissions(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_REMOVE_PERMISSIONS, SubCommand: sub}))
}

func rolePolicies(t *testing.T, m *Manager, role string) []authorization.Policy {
	t.Helper()
	roles, err := m.authZ.GetRoles(role)
	require.NoError(t, err)
	var out []authorization.Policy
	for _, p := range roles[role] {
		if p.Resource != conv.InternalPlaceHolder {
			out = append(out, p)
		}
	}
	return out
}

func TestBackupPrincipalMigration(t *testing.T) {
	movies := policiesOf(t, backupsPermission(&models.PermissionBackups{Collection: authorization.String("Movies")}))[0]
	grants := policiesOf(t, backupsPermission(authorization.AllBackupUsers), backupsPermission(authorization.AllBackupRoles))
	usersGrant, rolesGrant := grants[0], grants[1]

	t.Run("an upsert adds exactly its rows", func(t *testing.T) {
		m := newTestManager(t)
		applyUpsert(t, m, map[string][]authorization.Policy{"operator": {movies}})
		assert.Equal(t, []authorization.Policy{movies}, rolePolicies(t, m, "operator"))
	})

	t.Run("a remove keeps an explicit users or roles wildcard", func(t *testing.T) {
		m := newTestManager(t)
		applyUpsert(t, m, map[string][]authorization.Policy{"operator": {movies, usersGrant, rolesGrant}})
		applyRemove(t, m, "operator", movies)
		assert.ElementsMatch(t, grants, rolePolicies(t, m, "operator"))
	})

	// The log has three entries: the role creation, the leader's migration
	// entry, and an admin removing the roles wildcard. The blob holds the rows
	// a v1.40.5 node has after applying that log, with the snapshot version
	// v1.40.5 writes.
	t.Run("a version-1 blob holding what an old node holds after the migration and a later removal equals a replay of the same log", func(t *testing.T) {
		replayed := newTestManager(t)
		applyUpsert(t, replayed, map[string][]authorization.Policy{"operator": {movies}})
		applyUpsert(t, replayed, map[string][]authorization.Policy{"operator": grants, conv.BackupPrincipalsMigratedRole: {}})
		applyRemove(t, replayed, "operator", rolesGrant)

		anchor := conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, authentication.AuthTypeDb)
		operator := conv.PrefixRoleName("operator")
		blob, err := json.Marshal(map[string]any{
			"roles_policies": [][]string{
				{operator, movies.Resource, movies.Verb, movies.Domain},
				{operator, usersGrant.Resource, usersGrant.Verb, usersGrant.Domain},
			},
			"grouping_policies": [][]string{
				{anchor, operator},
				{anchor, conv.PrefixRoleName(conv.BackupPrincipalsMigratedRole)},
			},
			"version": 1,
		})
		require.NoError(t, err)
		restored := newTestManager(t)
		require.NoError(t, restored.Restore(blob))

		want, got := casbinRows(t, replayed.authZ), casbinRows(t, restored.authZ)
		assert.ElementsMatch(t, want.Policy, got.Policy, "p rows")
		assert.ElementsMatch(t, want.GroupingPolicy, got.GroupingPolicy, "g rows")
		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant}, rolePolicies(t, restored, "operator"))
	})
}
