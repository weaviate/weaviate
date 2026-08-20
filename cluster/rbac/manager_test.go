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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	policyPath := filepath.Join(t.TempDir(), "policy.csv")
	authZ, err := rbac.New(policyPath, rbacconf.Config{Enabled: true}, config.Authentication{}, true, nil, logrus.New())
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

// applyAssignRole assigns roles to user through the apply path, at the current
// command version so the migration passes the subject through untouched.
func applyAssignRole(m *Manager, user string, roles ...string) error {
	sub, err := json.Marshal(&cmd.AddRolesForUsersRequest{
		User:    user,
		Roles:   roles,
		Version: cmd.RBACAssignRevokeLatestCommandPolicyVersion,
	})
	if err != nil {
		return err
	}
	return m.AddRolesForUser(&cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_ROLES_FOR_USER,
		SubCommand: sub,
	})
}

// TestUpsertRolesPermissionsIgnoresTheNamespace pins that the apply mints the
// role whatever namespace its name carries, including one no namespace row
// exists for. The state check lives in Store.admitPropose, ahead of the RAFT
// append, so the apply must not re-adjudicate it: a refusal here would have a
// binary without the check create the role a binary with it rejects.
//
// This manager holds no namespace lookup at all, which is what makes the
// property hold today. The test is what says so out loud, and it goes red if a
// namespace gate is ever wired into either arm.
func TestUpsertRolesPermissionsIgnoresTheNamespace(t *testing.T) {
	tests := []struct {
		name string
		role string
	}{
		{name: "global role", role: "editor"},
		{name: "namespaced role", role: "alpha:editor"},
		// A short name of its own: a namespaced role reusing a built-in one is
		// refused by the short-name invariant, which is not this test's subject.
		{name: "role in a namespace nothing knows about", role: "never-existed:auditor"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, applyCreateRole(m, tc.role))

			roles, err := m.authZ.GetRoles(tc.role)
			require.NoError(t, err)
			assert.Contains(t, roles, tc.role, "the apply must persist the role")
		})
	}
}

// TestAddRolesForUserIgnoresTheNamespace is the same property on the assignment
// arm: the subject's namespace does not decide whether the grouping row is
// written. See TestUpsertRolesPermissionsIgnoresTheNamespace for why the apply
// must not re-adjudicate what admitPropose already refused.
func TestAddRolesForUserIgnoresTheNamespace(t *testing.T) {
	tests := []struct {
		name    string
		role    string
		subject string
		userID  string
	}{
		{name: "global subject", role: "editor", subject: "db:bob", userID: "bob"},
		{name: "namespaced subject", role: "alpha:editor", subject: "db:alpha:bob", userID: "alpha:bob"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, applyCreateRole(m, tc.role))
			require.NoError(t, applyAssignRole(m, tc.subject, tc.role))

			assigned, err := m.authZ.GetRolesForUserOrGroup(tc.userID, authentication.AuthTypeDb, false)
			require.NoError(t, err)
			assert.Contains(t, assigned, tc.role, "the apply must persist the assignment")
		})
	}
}
