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
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// listerOf reports each named namespace as existing. A source manager built
// with it writes those names into its snapshots' Namespaces list, as a cluster
// with namespaces on would.
type listerOf []string

func (l listerOf) List() []cmd.Namespace {
	out := make([]cmd.Namespace, len(l))
	for i, name := range l {
		out[i] = cmd.Namespace{Name: name, State: cmd.NamespaceStateActive}
	}
	return out
}

// snapshotOf builds a backup blob from a manager holding exactly the given
// roles. The source knows the namespaces its role names carry, so the blob
// gets a Namespaces list exactly as a real backup would.
func snapshotOf(t *testing.T, roles ...string) []byte {
	t.Helper()
	var nss listerOf
	for _, r := range roles {
		if ns := namespacing.NamespaceFromQualified(r); ns != "" {
			nss = append(nss, ns)
		}
	}
	source := newTestManagerWithNamespaces(t, nss)
	for _, r := range roles {
		require.NoError(t, applyCreateRole(source, r))
	}
	blob, err := source.authZ.Snapshot()
	require.NoError(t, err)
	return blob
}

// customRoleNames returns the manager's role names minus the built-ins.
// applyPredefinedRoles rebuilds the built-ins from this node's own
// configuration on every restore, so they say nothing about the blob.
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

// TestRestoreFromBackupReplacesRoleStore pins that the restore replaces the
// whole store: the target's own roles are gone afterwards, and an empty blob is
// a no-op rather than a wipe.
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

// TestRestoreFromBackupHonoursStripFlag pins that the flag travels: a target
// with namespaces off must store the role without its namespace prefix, and a
// target with namespaces on must keep the prefix.
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
// check. On a target with namespaces on, every namespace the blob references
// must exist and not be deleting, and the error names each one that fails.
// Suspended and resuming pass. With the strip on, the check is skipped because
// the strip drops every namespace prefix anyway.
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

			err := newTestManager(t).ValidateBackupSnapshot(req, usecasesNamespaces.NewMockExisterInState(t, tt.states))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestValidateBackupSnapshotNamespaceFromDBSubject covers a blob whose only mention
// of a namespace is the assignment "db:ns3:bob -> viewer". The source leaves db
// subjects out of the Namespaces list, so the check has to read them itself.
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

			err := newTestManager(t).ValidateBackupSnapshot(req, usecasesNamespaces.NewMockExisterInState(t, tt.states))
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
