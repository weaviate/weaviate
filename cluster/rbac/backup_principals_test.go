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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
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

func applyUpsert(t *testing.T, m *Manager, role string, version int, aware bool, policies ...authorization.Policy) {
	t.Helper()
	sub, err := json.Marshal(&cmd.CreateRolesRequest{
		Roles:   map[string][]authorization.Policy{role: policies},
		Version: version, BackupPrincipalsAware: aware,
	})
	require.NoError(t, err)
	require.NoError(t, m.UpsertRolesPermissions(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, SubCommand: sub}))
}

func applyRemove(t *testing.T, m *Manager, role string, version int, aware bool, policies ...authorization.Policy) {
	t.Helper()
	ps := make([]*authorization.Policy, len(policies))
	for i := range policies {
		ps[i] = &policies[i]
	}
	sub, err := json.Marshal(&cmd.RemovePermissionsRequest{Role: role, Permissions: ps, Version: version, BackupPrincipalsAware: aware})
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

// rbacBlob is an RBAC snapshot holding rows, recorded at version.
func rbacBlob(t *testing.T, version int, rows ...[]string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"roles_policies": rows, "grouping_policies": [][]string{}, "version": version})
	require.NoError(t, err)
	return b
}

func TestBackupPrincipalMigration(t *testing.T) {
	const latest = cmd.RBACLatestCommandPolicyVersion
	allCollections := policiesOf(t, backupsPermission(authorization.AllBackups))[0]
	movies := policiesOf(t, backupsPermission(&models.PermissionBackups{Collection: authorization.String("Movies")}))[0]
	books := policiesOf(t, backupsPermission(&models.PermissionBackups{Collection: authorization.String("Books")}))[0]
	grants := policiesOf(t, backupsPermission(authorization.AllBackupUsers), backupsPermission(authorization.AllBackupRoles))
	readData := policiesOf(t, &models.Permission{Action: authorization.String(authorization.ReadData), Data: authorization.AllData})[0]

	t.Run("replay", func(t *testing.T) {
		t.Run("a marker-less upsert gains the grants for a wildcard or class-scoped collections grant", func(t *testing.T) {
			for _, source := range []authorization.Policy{allCollections, movies} {
				m := newTestManager(t)
				applyUpsert(t, m, "operator", latest, false, source)
				assert.ElementsMatch(t, append([]authorization.Policy{source}, grants...), rolePolicies(t, m, "operator"), source.Resource)
			}
		})

		t.Run("a marker-bearing upsert is untouched", func(t *testing.T) {
			m := newTestManager(t)
			applyUpsert(t, m, "operator", latest, true, movies)
			assert.Equal(t, []authorization.Policy{movies}, rolePolicies(t, m, "operator"))
		})

		t.Run("a role without a backups collections grant is untouched", func(t *testing.T) {
			m := newTestManager(t)
			applyUpsert(t, m, "reader", latest, false, readData)
			assert.Equal(t, []authorization.Policy{readData}, rolePolicies(t, m, "reader"))
		})

		t.Run("re-applying a marker-less upsert adds nothing", func(t *testing.T) {
			m := newTestManager(t)
			applyUpsert(t, m, "operator", latest, false, movies)
			applyUpsert(t, m, "operator", latest, false, movies)
			assert.Len(t, rolePolicies(t, m, "operator"), 3)
		})

		t.Run("a marker-less remove drops the grants only with the last collections grant the role holds", func(t *testing.T) {
			tests := []struct {
				name string
				// setup builds the role that the remove without the marker then acts on.
				setup  func(m *Manager)
				remove []authorization.Policy
				want   []authorization.Policy
			}{
				{
					name:   "removing the last collections grant removes the grants",
					setup:  func(m *Manager) { applyUpsert(t, m, "operator", latest, false, movies, readData) },
					remove: []authorization.Policy{movies},
					want:   []authorization.Policy{readData},
				},
				{
					name:   "removing every collections grant at once removes the grants",
					setup:  func(m *Manager) { applyUpsert(t, m, "operator", latest, false, movies, books, readData) },
					remove: []authorization.Policy{movies, books},
					want:   []authorization.Policy{readData},
				},
				{
					name:   "a remaining collections grant keeps them",
					setup:  func(m *Manager) { applyUpsert(t, m, "operator", latest, false, movies, books) },
					remove: []authorization.Policy{movies},
					want:   append([]authorization.Policy{books}, grants...),
				},
				{
					name: "removing a grant the role never held strips nothing, explicit wildcards included",
					setup: func(m *Manager) {
						applyUpsert(t, m, "operator", latest, true, append([]authorization.Policy{readData}, grants...)...)
					},
					remove: []authorization.Policy{movies},
					want:   append([]authorization.Policy{readData}, grants...),
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					m := newTestManager(t)
					tt.setup(m)
					applyRemove(t, m, "operator", latest, false, tt.remove...)
					assert.ElementsMatch(t, tt.want, rolePolicies(t, m, "operator"))
				})
			}
		})

		t.Run("a marker-less remove of the last collections grant removes the grants at every command version", func(t *testing.T) {
			for version := cmd.RBACCommandPolicyVersionV0; version <= latest; version++ {
				t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
					m := newTestManager(t)
					applyUpsert(t, m, "operator", latest, false, movies, readData)
					applyRemove(t, m, "operator", version, false, movies)
					assert.Equal(t, []authorization.Policy{readData}, rolePolicies(t, m, "operator"))
				})
			}
		})

		t.Run("a marker-bearing remove leaves the grants", func(t *testing.T) {
			m := newTestManager(t)
			applyUpsert(t, m, "operator", latest, false, movies)
			applyRemove(t, m, "operator", latest, true, movies)
			assert.ElementsMatch(t, grants, rolePolicies(t, m, "operator"))
		})
	})

	// The two surfaces migrate by container, not by row: a snapshot recorded
	// before the bump cannot say whether a role holding only collections
	// grants was written by a binary that knew backups/users and backups/roles.
	t.Run("the snapshot and replay surfaces disagree on a collections-only role", func(t *testing.T) {
		restored := newTestManager(t)
		require.NoError(t, restored.Restore(rbacBlob(t, rbac.SnapshotVersionV1,
			[]string{conv.PrefixRoleName("operator"), movies.Resource, movies.Verb, movies.Domain})))
		replayed := newTestManager(t)
		applyUpsert(t, replayed, "operator", latest, true, movies)

		assert.ElementsMatch(t, append([]authorization.Policy{movies}, grants...), rolePolicies(t, restored, "operator"))
		assert.Equal(t, []authorization.Policy{movies}, rolePolicies(t, replayed, "operator"))
	})
}

func TestForcedSnapshotArming(t *testing.T) {
	const latest = cmd.RBACLatestCommandPolicyVersion
	movies := policiesOf(t, backupsPermission(&models.PermissionBackups{Collection: authorization.String("Movies")}))[0]
	row := []string{conv.PrefixRoleName("operator"), movies.Resource, movies.Verb, movies.Domain}

	tests := []struct {
		name  string
		apply func(t *testing.T, m *Manager)
		arms  bool
	}{
		{"a marker-less upsert arms the signal", func(t *testing.T, m *Manager) { applyUpsert(t, m, "operator", latest, false, movies) }, true},
		{"a marker-less remove that changes no rows arms the signal", func(t *testing.T, m *Manager) { applyRemove(t, m, "ghost", latest, false, movies) }, true},
		{"a marker-bearing upsert does not arm it", func(t *testing.T, m *Manager) { applyUpsert(t, m, "operator", latest, true, movies) }, false},
		{"a marker-bearing remove does not arm it", func(t *testing.T, m *Manager) { applyRemove(t, m, "operator", latest, true, movies) }, false},
		{"restoring a pre-bump snapshot arms the signal", func(t *testing.T, m *Manager) {
			require.NoError(t, m.Restore(rbacBlob(t, rbac.SnapshotVersionV1, row)))
		}, true},
		{"restoring a latest-version snapshot does not arm it", func(t *testing.T, m *Manager) {
			require.NoError(t, m.Restore(rbacBlob(t, rbac.SnapshotVersionLatest, row)))
		}, false},
		{"restoring a pre-bump backup blob arms the signal", func(t *testing.T, m *Manager) {
			require.NoError(t, m.RestoreFromBackup(&cmd.RestoreRolesAndUsersRequest{Roles: rbacBlob(t, rbac.SnapshotVersionV1, row)}))
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			tt.apply(t, m)
			assert.Equal(t, tt.arms, len(m.forceSnapshotCh) == 1)
		})
	}
}
