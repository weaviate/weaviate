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

	t.Run("namespaces enabled, global caller", func(t *testing.T) {
		tests := []struct {
			name    string
			grant   *models.Permission
			allowed []string
			denied  []string
		}{
			{
				name:    "a qualified user grant matches only that qualified user",
				grant:   backupsPerm(&models.PermissionBackups{User: authorization.String("ns1:alice")}),
				allowed: authorization.BackupUsers("ns1:alice"),
				denied:  []string{"backups/users/alice", "backups/users/ns1:bob", "backups/users/ns2:alice", "backups/users/*"},
			},
			{
				name:    "an unqualified user grant does not reach the namespaced user",
				grant:   backupsPerm(&models.PermissionBackups{User: authorization.String("alice")}),
				allowed: authorization.BackupUsers("alice"),
				denied:  []string{"backups/users/ns1:alice"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				m, err := setupNSEnabledTestManager(t, logger, "ns1", "ns2")
				require.NoError(t, err)
				grant(t, m, "operator", tt.grant)
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
	})

	// With namespaces on, admin gets tenantSafeAdminPermissions, which excludes
	// the backups domain. So only root holds the wildcards.
	t.Run("with namespaces, root holds both wildcards and the narrowed admin neither", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		m, err := setupNSEnabledTestManager(t, logger)
		require.NoError(t, err)
		for role, allowed := range map[string]bool{authorization.Root: true, authorization.Admin: false} {
			user := "builtin-" + role
			_, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb), conv.PrefixRoleName(role))
			require.NoError(t, err)
			caller := &models.Principal{Username: user, UserType: models.UserTypeInputDb}
			for _, r := range append(authorization.BackupUsers(), authorization.BackupRoles()...) {
				err := m.Authorize(context.Background(), caller, authorization.CREATE, r)
				if allowed {
					assert.NoError(t, err, role, r)
				} else {
					assert.ErrorAs(t, err, new(authzErrors.Forbidden), role, r)
				}
			}
		}
	})

	t.Run("the built-in metadata reader reads both wildcards and creates neither", func(t *testing.T) {
		const group = "infra-access"
		for _, mode := range metadataReaderModes {
			t.Run(mode.name, func(t *testing.T) {
				m := setupMetadataReaderTestManager(t, mode.namespacesEnabled, group)
				caller := metadataReaderPrincipal("staff", mode.namespacesEnabled, group)
				for _, r := range append(authorization.BackupUsers(), authorization.BackupRoles()...) {
					assert.NoError(t, m.Authorize(context.Background(), caller, authorization.READ, r), r)
					err := m.Authorize(context.Background(), caller, authorization.CREATE, r)
					assert.ErrorAs(t, err, new(authzErrors.Forbidden), r)
				}
			})
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

// snapshotAt encodes the rows as an RBAC snapshot with the given version.
func snapshotAt(t *testing.T, version int, policy, grouping [][]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(snapshot{Policy: policy, GroupingPolicy: grouping, Version: version}))
	return buf.Bytes()
}

// markerRow is the grouping row that records the backup principal migration.
var markerRow = []string{
	conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, authentication.AuthTypeDb),
	conv.PrefixRoleName(conv.BackupPrincipalsMigratedRole),
}

func groupingRows(t *testing.T, m *Manager) [][]string {
	t.Helper()
	rows, err := m.casbin.GetFilteredNamedGroupingPolicy("g", 0)
	require.NoError(t, err)
	return rows
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
	restGrants, err := conv.PermissionToPolicies(backupsPerm(authorization.AllBackupUsers))
	require.NoError(t, err)
	usersGrant := *restGrants[0]

	allCollections := []string{conv.PrefixRoleName("all-collections"), "backups/collections/.*", conv.CRUD, authorization.BackupsDomain}
	oneCollection := []string{conv.PrefixRoleName("one-collection"), "backups/collections/Movies", conv.CRUD, authorization.BackupsDomain}
	schemaOnly := []string{conv.PrefixRoleName("schema-only"), "schema/collections/Movies/shards/#", authorization.READ, authorization.SchemaDomain}
	collectionsRow := func(row []string) authorization.Policy {
		return authorization.Policy{Resource: row[1], Verb: row[2], Domain: row[3]}
	}

	newManager := func(t *testing.T, dir string) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		m, err := New(dir, rbacconf.Config{Enabled: true},
			config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}}, false, nil, logger)
		require.NoError(t, err)
		return m
	}

	// Released binaries write RBAC snapshots with version 1. This snapshot
	// holds what such a node holds after the migration and after an admin
	// removed both wildcards from all-collections and rolesGrant from
	// one-collection.
	t.Run("a version-1 snapshot loads unchanged", func(t *testing.T) {
		m := newManager(t, t.TempDir())
		policy := [][]string{
			allCollections, oneCollection, schemaOnly,
			{oneCollection[0], usersGrant.Resource, usersGrant.Verb, usersGrant.Domain},
		}
		require.NoError(t, m.Restore(snapshotAt(t, 1, policy, [][]string{markerRow}), false))

		assert.Equal(t, []authorization.Policy{collectionsRow(allCollections)}, rowsOf(t, m, "all-collections"))
		assert.ElementsMatch(t, []authorization.Policy{collectionsRow(oneCollection), usersGrant}, rowsOf(t, m, "one-collection"))
		assert.Equal(t, []authorization.Policy{collectionsRow(schemaOnly)}, rowsOf(t, m, "schema-only"))
		assert.Contains(t, groupingRows(t, m), markerRow)
	})

	t.Run("the marker is hidden from GetRoles, ListAllRoles and SnapshotRoleNames", func(t *testing.T) {
		m := newManager(t, t.TempDir())
		require.NoError(t, m.Restore(snapshotAt(t, 1, [][]string{oneCollection}, [][]string{
			{markerRow[0], oneCollection[0]}, markerRow,
		}), false))
		migrated, err := m.BackupPrincipalsMigrated()
		require.NoError(t, err)
		require.True(t, migrated)

		all, err := m.GetRoles()
		require.NoError(t, err)
		assert.NotContains(t, all, conv.BackupPrincipalsMigratedRole)
		named, err := m.GetRoles(conv.BackupPrincipalsMigratedRole)
		require.NoError(t, err)
		assert.Empty(t, named)
		names, err := m.ListAllRoles()
		require.NoError(t, err)
		assert.NotContains(t, names, conv.BackupPrincipalsMigratedRole)

		// A blob of named roles carries the marker row, but SnapshotRoleNames
		// still returns only the named role.
		blob, err := m.Snapshot("one-collection")
		require.NoError(t, err)
		var s snapshot
		require.NoError(t, json.Unmarshal(blob, &s))
		assert.Contains(t, s.GroupingPolicy, markerRow)
		got, err := SnapshotRoleNames(blob)
		require.NoError(t, err)
		assert.Equal(t, []string{"one-collection"}, got)
	})
}

func TestManagerBackupPrincipalMigration(t *testing.T) {
	restGrants, err := conv.PermissionToPolicies(backupsPerm(authorization.AllBackupUsers), backupsPerm(authorization.AllBackupRoles))
	require.NoError(t, err)
	usersGrant, rolesGrant := *restGrants[0], *restGrants[1]
	readBackups := authorization.ReadBackups
	readRest, err := conv.PermissionToPolicies(
		&models.Permission{Action: &readBackups, Backups: authorization.AllBackupUsers},
		&models.Permission{Action: &readBackups, Backups: authorization.AllBackupRoles})
	require.NoError(t, err)

	movies := authorization.Policy{Resource: "backups/collections/Movies", Verb: conv.CRUD, Domain: authorization.BackupsDomain}
	readBooks := authorization.Policy{Resource: "backups/collections/Books", Verb: authorization.READ, Domain: authorization.BackupsDomain}
	schemaOnly := authorization.Policy{Resource: "schema/collections/Movies/shards/#", Verb: authorization.READ, Domain: authorization.SchemaDomain}

	newManager := func(t *testing.T) *Manager {
		t.Helper()
		logger, _ := test.NewNullLogger()
		m, err := New(t.TempDir(), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
		require.NoError(t, err)
		return m
	}

	t.Run("custom roles get the rows they lack with their verb, built-in roles none", func(t *testing.T) {
		m := newManager(t)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
			"operator":     {movies},
			"reader":       {readBooks},
			"half-done":    {movies, usersGrant},
			"schema-only":  {schemaOnly},
			"already-done": {movies, usersGrant, rolesGrant},
		}))
		// After a load, applyPredefinedRoles rewrites built-in rows. Each
		// built-in role is then a wildcard role or, like the metadata reader,
		// already holds the derived rows, so none would gain a row. This row on
		// viewer checks the migration skips built-in roles anyway.
		_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName(authorization.Viewer), readBooks.Resource, readBooks.Verb, readBooks.Domain)
		require.NoError(t, err)

		got, err := m.BackupPrincipalMigration()
		require.NoError(t, err)
		assert.Len(t, got, 4)
		assert.ElementsMatch(t, []authorization.Policy{usersGrant, rolesGrant}, got["operator"])
		assert.ElementsMatch(t, []authorization.Policy{*readRest[0], *readRest[1]}, got["reader"])
		assert.Equal(t, []authorization.Policy{rolesGrant}, got["half-done"])
		assert.Contains(t, got, conv.BackupPrincipalsMigratedRole)
		assert.Empty(t, got[conv.BackupPrincipalsMigratedRole])
	})

	t.Run("an empty store yields only the marker", func(t *testing.T) {
		got, err := newManager(t).BackupPrincipalMigration()
		require.NoError(t, err)
		assert.Equal(t, map[string][]authorization.Policy{conv.BackupPrincipalsMigratedRole: {}}, got)
	})

	t.Run("upserting the result sets the marker and allows concrete IDs; a revoke of the REST grants removes the rows", func(t *testing.T) {
		m := newManager(t)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"operator": {movies}}))
		migrated, err := m.BackupPrincipalsMigrated()
		require.NoError(t, err)
		require.False(t, migrated)

		rows, err := m.BackupPrincipalMigration()
		require.NoError(t, err)
		require.NoError(t, m.UpdateRolesPermissions(rows))
		migrated, err = m.BackupPrincipalsMigrated()
		require.NoError(t, err)
		require.True(t, migrated)

		_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("alice", authentication.AuthTypeDb), conv.PrefixRoleName("operator"))
		require.NoError(t, err)
		require.NoError(t, m.casbin.InvalidateCache())
		caller := &models.Principal{Username: "alice", UserType: models.UserTypeInputDb}
		ctx := context.Background()
		require.NoError(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/users/bob", "backups/roles/editor"))

		require.NoError(t, m.RemovePermissions("operator", restGrants))
		assert.Equal(t, []authorization.Policy{movies}, rowsOf(t, m, "operator"))
		assert.ErrorAs(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/users/bob"), new(authzErrors.Forbidden))
		assert.ErrorAs(t, m.Authorize(ctx, caller, authorization.CREATE, "backups/roles/editor"), new(authzErrors.Forbidden))
	})
}
