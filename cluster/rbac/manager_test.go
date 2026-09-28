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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func newTestManager(t *testing.T) *Manager {
	return newTestManagerWithNamespaces(t, nil)
}

func newTestManagerWithNamespaces(t *testing.T, namespaces rbac.NamespaceLister) *Manager {
	t.Helper()
	return newTestManagerAt(t, t.TempDir(), namespaces)
}

// newTestManagerAt keeps the policy file under dir, for a test that breaks it.
func newTestManagerAt(t *testing.T, dir string, namespaces rbac.NamespaceLister) *Manager {
	t.Helper()
	authZ, err := rbac.New(dir, rbacconf.Config{Enabled: true}, config.Authentication{}, true, namespaces, logrus.New())
	require.NoError(t, err)
	return NewManager(authZ, config.Authentication{}, logrus.New())
}

func applyCreateRole(m *Manager, name string) error {
	policies := []authorization.Policy{
		{Resource: authorization.Cluster(), Domain: authorization.ClusterDomain, Verb: authorization.READ},
	}
	sub, err := json.Marshal(&cmd.CreateRolesRequest{
		Roles:        map[string][]authorization.Policy{name: policies},
		Version:      cmd.RBACLatestCommandPolicyVersion,
		RoleCreation: true,
	})
	if err != nil {
		return err
	}
	return m.UpsertRolesPermissions(&cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
		SubCommand: sub,
	})
}

// TestUpsertRolesPermissionsShortNameConflict pins the apply-layer enforcement
// of the short-name uniqueness invariant: a global name reserves its short name
// across every namespace and vice versa. The handler runs the same scan, but its
// read is not atomic with this write, so this is where the invariant must hold.
func TestUpsertRolesPermissionsShortNameConflict(t *testing.T) {
	tests := []struct {
		name         string
		existing     string
		candidate    string
		wantConflict bool
	}{
		{
			name:         "local conflicts with existing global",
			existing:     "editor",
			candidate:    "customer1:editor",
			wantConflict: true,
		},
		{
			name:         "global conflicts with existing local",
			existing:     "customer1:editor",
			candidate:    "editor",
			wantConflict: true,
		},
		{
			name:         "exact duplicate is rejected",
			existing:     "editor",
			candidate:    "editor",
			wantConflict: true,
		},
		{
			name:         "same short name in different namespaces coexist",
			existing:     "customer1:editor",
			candidate:    "customer2:editor",
			wantConflict: false,
		},
		{
			name:         "distinct short name is allowed",
			existing:     "editor",
			candidate:    "reviewer",
			wantConflict: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, applyCreateRole(m, tt.existing))

			err := applyCreateRole(m, tt.candidate)
			if tt.wantConflict {
				require.ErrorIs(t, err, ErrBadRequest)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestUpsertRolesPermissionsRejectsMalformedSubCommand pins that a sub-command
// payload that is not a valid CreateRolesRequest is rejected as a bad request
// rather than panicking or applying a partial state.
func TestUpsertRolesPermissionsRejectsMalformedSubCommand(t *testing.T) {
	m := newTestManager(t)
	err := m.UpsertRolesPermissions(&cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
		SubCommand: []byte("not json"),
	})
	require.ErrorIs(t, err, ErrBadRequest)
}

// TestRestoreDropsRowsPolicyFileCannotStore pins that a RAFT snapshot holding a
// row the policy file cannot store still restores, without that row. The raft
// library stops the node from starting when no snapshot restores.
func TestRestoreDropsRowsPolicyFileCannotStore(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, applyCreateRole(m, "roleA"))
	blob, err := json.Marshal(map[string]any{
		"roles_policies": [][]string{{"role:roleB", authorization.Cluster(), authorization.READ, authorization.ClusterDomain}},
		"grouping_policies": [][]string{
			{conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, authentication.AuthTypeDb), "role:roleB"},
			{"oidc:Doe, John", "role:roleB"},
			{"oidc:Jane", "role:roleB"},
		},
		"version": rbac.SnapshotVersionLatest,
	})
	require.NoError(t, err)

	require.NoError(t, m.Restore(blob))

	assert.ElementsMatch(t, []string{"roleB"}, customRoleNames(t, m))
	for user, want := range map[string]int{"Jane": 1, "Doe, John": 0} {
		roles, err := m.authZ.GetRolesForUserOrGroup(user, authentication.AuthTypeOIDC, false)
		require.NoError(t, err)
		assert.Len(t, roles, want, user)
	}
}
