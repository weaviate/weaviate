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
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

var manageBackups = authorization.ManageBackups

// grant gives user a role holding the permissions, stored as the REST path
// stores them.
func grant(t *testing.T, m *Manager, user string, perms ...*models.Permission) {
	t.Helper()
	policies, err := conv.PermissionToPolicies(perms...)
	require.NoError(t, err)
	role := "role-of-" + user
	for _, p := range policies {
		_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName(role), p.Resource, p.Verb, p.Domain)
		require.NoError(t, err)
	}
	_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb), conv.PrefixRoleName(role))
	require.NoError(t, err)
	require.NoError(t, m.casbin.InvalidateCache())
}

func backupsPerm(b *models.PermissionBackups) *models.Permission {
	return &models.Permission{Action: &manageBackups, Backups: b}
}

func TestAuthorizeBackupPrincipals(t *testing.T) {
	tests := []struct {
		name    string
		grant   []*models.Permission
		allowed []string
		denied  []string
	}{
		{
			name:    "a per-ID user grant allows exactly its user",
			grant:   []*models.Permission{backupsPerm(&models.PermissionBackups{User: authorization.String("alice")})},
			allowed: authorization.BackupUsers("alice"),
			denied:  []string{"backups/users/alice2", "backups/users/bob", "backups/users/*", "backups/roles/alice"},
		},
		{
			name:    "an OIDC user grant with colons allows exactly its user",
			grant:   []*models.Permission{backupsPerm(&models.PermissionBackups{User: authorization.String("urn:corp:alice")})},
			allowed: authorization.BackupUsers("urn:corp:alice"),
			denied:  []string{"backups/users/urn:corp:bob", "backups/users/*"},
		},
		{
			name:    "a per-ID role grant allows exactly its role",
			grant:   []*models.Permission{backupsPerm(&models.PermissionBackups{Role: authorization.String("editor")})},
			allowed: authorization.BackupRoles("editor"),
			denied:  []string{"backups/roles/editor2", "backups/roles/*", "backups/users/editor"},
		},
		{
			name:    "the users wildcard covers every user and the users kind check, not roles",
			grant:   []*models.Permission{backupsPerm(authorization.AllBackupUsers)},
			allowed: []string{"backups/users/alice", "backups/users/*"},
			denied:  []string{"backups/roles/editor", "backups/roles/*"},
		},
		{
			name:    "the roles wildcard covers every role, not users",
			grant:   []*models.Permission{backupsPerm(authorization.AllBackupRoles)},
			allowed: []string{"backups/roles/editor", "backups/roles/*"},
			denied:  []string{"backups/users/alice", "backups/users/*"},
		},
		{
			name:    "a collections grant does not reach users or roles",
			grant:   []*models.Permission{backupsPerm(authorization.AllBackups)},
			allowed: authorization.Backups("Movies"),
			denied:  []string{"backups/users/alice", "backups/users/*", "backups/roles/editor", "backups/roles/*"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)
			grant(t, m, "operator", tt.grant...)
			caller := &models.Principal{Username: "operator", UserType: models.UserTypeInputDb}

			for _, r := range tt.allowed {
				assert.NoError(t, m.Authorize(context.Background(), caller, authorization.CREATE, r), r)
			}
			for _, r := range tt.denied {
				err := m.Authorize(context.Background(), caller, authorization.CREATE, r)
				assert.ErrorAs(t, err, new(authzErrors.Forbidden), r)
			}
		})
	}

	t.Run("built-in root and admin hold both wildcards", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		m, err := setupTestManager(t, logger)
		require.NoError(t, err)
		for _, role := range []string{authorization.Root, authorization.Admin} {
			user := "builtin-" + role
			_, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb), conv.PrefixRoleName(role))
			require.NoError(t, err)
			caller := &models.Principal{Username: user, UserType: models.UserTypeInputDb}
			assert.NoError(t, m.Authorize(context.Background(), caller, authorization.CREATE, append(authorization.BackupUsers(), authorization.BackupRoles()...)...), role)
		}
	})

	t.Run("a namespaced caller stays denied despite a wildcard grant", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		m, err := setupNSEnabledTestManager(t, logger, "customer1")
		require.NoError(t, err)
		grant(t, m, "customer1:alice", backupsPerm(authorization.AllBackupUsers), backupsPerm(authorization.AllBackupRoles))
		caller := &models.Principal{Username: "customer1:alice", Namespace: "customer1", UserType: models.UserTypeInputDb}
		for _, r := range []string{"backups/users/*", "backups/users/customer1:bob", "backups/roles/*"} {
			err := m.Authorize(context.Background(), caller, authorization.CREATE, r)
			assert.ErrorAs(t, err, new(authzErrors.Forbidden), r)
		}
	})
}

// snapshotAt encodes rows as an RBAC snapshot recorded at version.
func snapshotAt(t *testing.T, version int, rows ...[]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(snapshot{Policy: rows, Version: version}))
	return buf.Bytes()
}

// rowsOf returns the policy rows role holds, without the role column.
func rowsOf(t *testing.T, m *Manager, role string) []authorization.Policy {
	t.Helper()
	rows, err := m.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(role))
	require.NoError(t, err)
	out := make([]authorization.Policy, 0, len(rows))
	for _, r := range rows {
		out = append(out, authorization.Policy{Resource: r[1], Verb: r[2], Domain: r[3]})
	}
	return out
}

func TestRestoreBackupPrincipalGrants(t *testing.T) {
	restGrants, err := conv.PermissionToPolicies(backupsPerm(authorization.AllBackupUsers), backupsPerm(authorization.AllBackupRoles))
	require.NoError(t, err)
	usersGrant, rolesGrant := *restGrants[0], *restGrants[1]

	allCollections := []string{conv.PrefixRoleName("all-collections"), "backups/collections/.*", conv.CRUD, authorization.BackupsDomain}
	oneCollection := []string{conv.PrefixRoleName("one-collection"), "backups/collections/Movies", conv.CRUD, authorization.BackupsDomain}
	schemaOnly := []string{conv.PrefixRoleName("schema-only"), "schema/collections/Movies/shards/#", authorization.READ, authorization.SchemaDomain}
	collectionsRow := func(row []string) authorization.Policy {
		return authorization.Policy{Resource: row[1], Verb: row[2], Domain: row[3]}
	}

	newManager := func(t *testing.T, dir string) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		m, err := New(filepath.Join(dir, "policy.csv"), rbacconf.Config{Enabled: true},
			config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}}, false, nil, logger)
		require.NoError(t, err)
		return m
	}

	for _, version := range []int{SnapshotVersionV0, SnapshotVersionV1} {
		t.Run("an older snapshot gives collections grants the users and roles grants", func(t *testing.T) {
			dir := t.TempDir()
			m := newManager(t, dir)

			migrated, err := m.RestoreAndReportMigration(snapshotAt(t, version, allCollections, oneCollection, schemaOnly), false)
			require.NoError(t, err)
			assert.True(t, migrated, "version %d", version)

			check := func(t *testing.T, m *Manager) {
				assert.ElementsMatch(t, []authorization.Policy{collectionsRow(allCollections), usersGrant, rolesGrant}, rowsOf(t, m, "all-collections"))
				assert.ElementsMatch(t, []authorization.Policy{collectionsRow(oneCollection), usersGrant, rolesGrant}, rowsOf(t, m, "one-collection"))
				assert.Equal(t, []authorization.Policy{collectionsRow(schemaOnly)}, rowsOf(t, m, "schema-only"))
			}
			t.Run("once the restore returns", func(t *testing.T) { check(t, m) })
			t.Run("after a reload from policy.csv", func(t *testing.T) {
				// Drops in-memory state and reloads the persisted file, as a restart does.
				require.NoError(t, m.casbin.LoadPolicy())
				check(t, m)
			})
		})
	}

	t.Run("a latest-version snapshot is taken as it is", func(t *testing.T) {
		m := newManager(t, t.TempDir())
		migrated, err := m.RestoreAndReportMigration(snapshotAt(t, SnapshotVersionLatest, oneCollection), false)
		require.NoError(t, err)
		assert.False(t, migrated)
		assert.Equal(t, []authorization.Policy{collectionsRow(oneCollection)}, rowsOf(t, m, "one-collection"))
	})

	t.Run("re-running over rows that already hold the grants adds nothing", func(t *testing.T) {
		m := newManager(t, t.TempDir())
		role := oneCollection[0]
		blob := snapshotAt(t, SnapshotVersionV1, oneCollection,
			[]string{role, usersGrant.Resource, usersGrant.Verb, usersGrant.Domain},
			[]string{role, rolesGrant.Resource, rolesGrant.Verb, rolesGrant.Domain})
		for range 2 {
			_, err := m.RestoreAndReportMigration(blob, false)
			require.NoError(t, err)
			assert.Len(t, rowsOf(t, m, "one-collection"), 3)
		}
		require.NoError(t, addBackupPrincipalGrants(m.casbin))
		assert.Len(t, rowsOf(t, m, "one-collection"), 3)
	})

	t.Run("a migrated role is allowed on concrete IDs, and a blind revoke of the REST grants removes the rows", func(t *testing.T) {
		m := newManager(t, t.TempDir())
		_, err := m.RestoreAndReportMigration(snapshotAt(t, SnapshotVersionV1, oneCollection), false)
		require.NoError(t, err)
		_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("operator", authentication.AuthTypeDb), oneCollection[0])
		require.NoError(t, err)
		require.NoError(t, m.casbin.InvalidateCache())
		caller := &models.Principal{Username: "operator", UserType: models.UserTypeInputDb}
		ctx := context.Background()

		require.NoError(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/users/alice", "backups/roles/editor"))

		require.NoError(t, m.RemovePermissions("one-collection", restGrants))
		assert.Equal(t, []authorization.Policy{collectionsRow(oneCollection)}, rowsOf(t, m, "one-collection"))
		assert.ErrorAs(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/users/alice"), new(authzErrors.Forbidden))
		assert.ErrorAs(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/roles/editor"), new(authzErrors.Forbidden))
	})
}
