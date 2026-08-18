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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// snapshotOf builds a backup blob from a manager holding exactly the given roles.
func snapshotOf(t *testing.T, roles ...string) []byte {
	t.Helper()
	source := newTestManager(t)
	for _, r := range roles {
		require.NoError(t, applyCreateRole(source, r))
	}
	blob, err := source.authZ.Snapshot()
	require.NoError(t, err)
	return blob
}

// customRoleNames returns the manager's role names minus the built-ins, which
// applyPredefinedRoles rebuilds from this node's own configuration on every
// restore and so say nothing about the blob.
func customRoleNames(t *testing.T, m *Manager) []string {
	t.Helper()
	roles, err := m.authZ.GetRoles()
	require.NoError(t, err)
	var out []string
	for name := range roles {
		if !slices.Contains(authorization.BuiltInRoles, name) {
			out = append(out, name)
		}
	}
	return out
}

// namespacesInState returns an Exister reporting each named namespace in the
// given state; any other name is missing.
func namespacesInState(t *testing.T, states map[string]cmd.NamespaceState) *usecasesNamespaces.MockExister {
	t.Helper()
	m := &usecasesNamespaces.MockExister{}
	m.Test(t)
	exists := func(name string) bool {
		_, ok := states[name]
		return ok
	}
	m.On("Exists", mock.AnythingOfType("string")).Return(exists).Maybe()
	m.On("IsActive", mock.AnythingOfType("string")).Return(func(name string) bool {
		return states[name] == cmd.NamespaceStateActive
	}).Maybe()
	m.On("GetNamespace", mock.AnythingOfType("string")).Return(
		func(name string) cmd.Namespace {
			return cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}, State: states[name]}
		},
		exists,
	).Maybe()
	return m
}

// TestRestoreFromBackupReplacesRoleStore pins the whole-store replace: the
// target's own roles are gone afterwards, and an empty blob is a no-op rather
// than a wipe.
func TestRestoreFromBackupReplacesRoleStore(t *testing.T) {
	tests := []struct {
		name  string
		blob  func(t *testing.T) []byte
		roles []string
	}{
		{
			name:  "blob replaces the target's roles",
			blob:  func(t *testing.T) []byte { return snapshotOf(t, "roleC") },
			roles: []string{"roleC"},
		},
		{
			name:  "empty blob leaves the store untouched",
			blob:  func(t *testing.T) []byte { return nil },
			roles: []string{"roleA", "roleB"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, applyCreateRole(m, "roleA"))
			require.NoError(t, applyCreateRole(m, "roleB"))

			require.NoError(t, m.RestoreFromBackup(&cmd.RestoreRolesAndUsersRequest{Roles: tt.blob(t)}))

			assert.ElementsMatch(t, tt.roles, customRoleNames(t, m))
		})
	}
}

// TestRestoreFromBackupHonoursStripFlag pins that the flag travels: a
// namespace-disabled target must land the role unqualified, and a
// namespace-enabled one must keep the qualification.
func TestRestoreFromBackupHonoursStripFlag(t *testing.T) {
	tests := []struct {
		name  string
		strip bool
		want  string
	}{
		{name: "strip drops the qualification", strip: true, want: "editor"},
		{name: "no strip keeps the qualification", strip: false, want: "ns1:editor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob := snapshotOf(t, "ns1:editor")

			m := newTestManager(t)
			require.NoError(t, m.RestoreFromBackup(&cmd.RestoreRolesAndUsersRequest{
				Roles:           blob,
				StripNamespaces: tt.strip,
			}))

			assert.Equal(t, []string{tt.want}, customRoleNames(t, m))
		})
	}
}

// TestValidateBackupSnapshotNamespaceStates pins the fail-closed namespace
// check: on a namespace-enabled target every namespace the blob references
// must exist and not be deleting, the error names each offender, suspended and
// resuming pass, and the strip arm skips the check because the strip drops
// every qualification anyway.
func TestValidateBackupSnapshotNamespaceStates(t *testing.T) {
	tests := []struct {
		name    string
		roles   []string
		states  map[string]cmd.NamespaceState
		strip   bool
		wantErr string
	}{
		{
			name:   "active namespace passes",
			roles:  []string{"ns1:editor"},
			states: map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateActive},
		},
		{
			name:    "missing namespace is named",
			roles:   []string{"ns1:editor"},
			states:  map[string]cmd.NamespaceState{},
			wantErr: "ns1",
		},
		{
			name:   "suspended namespace passes",
			roles:  []string{"ns1:editor"},
			states: map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateSuspended},
		},
		{
			name:    "deleting namespace is named",
			roles:   []string{"ns1:editor"},
			states:  map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateDeleting},
			wantErr: "ns1",
		},
		{
			name:   "resuming namespace passes",
			roles:  []string{"ns1:editor"},
			states: map[string]cmd.NamespaceState{"ns1": cmd.NamespaceStateResuming},
		},
		{
			name:   "strip skips the check entirely",
			roles:  []string{"ns1:editor"},
			states: map[string]cmd.NamespaceState{},
			strip:  true,
		},
		{
			name:   "unqualified names and built-ins reference no namespace",
			roles:  []string{"editor"},
			states: map[string]cmd.NamespaceState{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &cmd.RestoreRolesAndUsersRequest{
				Roles:           snapshotOf(t, tt.roles...),
				StripNamespaces: tt.strip,
			}

			err := newTestManager(t).ValidateBackupSnapshot(req, namespacesInState(t, tt.states))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestValidateBackupSnapshotNamespaceFromDBSubject covers a blob whose only
// mention of a namespace is a role assignment: "db:ns3:bob -> viewer" names
// ns3, while every role name and every resource path stays unqualified. The
// fixture wires no namespace lister, so the snapshot carries no Namespaces
// list and this pins the fallback path; TestReferencedNamespaces and the
// scheduler case cover the path where the list is present. The users blob
// would carry ns3 too, but
// usersOptions=noRestore turns that check off, so the roles blob has to stand
// alone.
func TestValidateBackupSnapshotNamespaceFromDBSubject(t *testing.T) {
	source := newTestManager(t)
	require.NoError(t, source.authZ.AddRolesForUser("db:ns3:bob", []string{authorization.Viewer}))
	blob, err := source.authZ.Snapshot()
	require.NoError(t, err)

	tests := []struct {
		name    string
		states  map[string]cmd.NamespaceState
		wantErr string
	}{
		{
			name:   "active namespace passes",
			states: map[string]cmd.NamespaceState{"ns3": cmd.NamespaceStateActive},
		},
		{
			name:    "missing namespace is named",
			states:  map[string]cmd.NamespaceState{},
			wantErr: "ns3",
		},
		{
			name:   "suspended namespace passes",
			states: map[string]cmd.NamespaceState{"ns3": cmd.NamespaceStateSuspended},
		},
		{
			name:    "deleting namespace is named",
			states:  map[string]cmd.NamespaceState{"ns3": cmd.NamespaceStateDeleting},
			wantErr: "ns3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &cmd.RestoreRolesAndUsersRequest{Roles: blob}

			err := newTestManager(t).ValidateBackupSnapshot(req, namespacesInState(t, tt.states))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
