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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestReplayEquivalence checks that a pre-1.30 raft log replayed onto an empty
// store yields the rows the boot upgrade derives from the matching v1.29
// policy.csv. Store.Open clears the store before replay on a node without a
// raft snapshot, so the two paths must agree.
func TestReplayEquivalence(t *testing.T) {
	authN := config.Authentication{
		OIDC:   config.OIDC{Enabled: true},
		APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"alice"}},
	}

	// v1.29 wrote role commands at V3 and assignments at V0 (no Version field).
	createCustom := func(t *testing.T, m *Manager) {
		sub, err := json.Marshal(&cmd.CreateRolesRequest{
			Roles: map[string][]authorization.Policy{"custom": {
				{Resource: "users/alice", Verb: authorization.UPDATE, Domain: authorization.UsersDomain},
				{Resource: "cluster/.*", Verb: authorization.READ, Domain: authorization.ClusterDomain},
			}},
			RoleCreation: true,
			Version:      cmd.RBACCommandPolicyVersionV3,
		})
		require.NoError(t, err)
		require.NoError(t, m.UpsertRolesPermissions(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, SubCommand: sub}))
	}
	assign := func(user string) func(t *testing.T, m *Manager) {
		return func(t *testing.T, m *Manager) {
			sub, err := json.Marshal(&cmd.AddRolesForUsersRequest{User: user, Roles: []string{"custom"}, Version: cmd.RBACAssignRevokeCommandPolicyVersionV0})
			require.NoError(t, err)
			require.NoError(t, m.AddRolesForUser(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_ADD_ROLES_FOR_USER, SubCommand: sub}))
		}
	}
	revoke := func(user string) func(t *testing.T, m *Manager) {
		return func(t *testing.T, m *Manager) {
			sub, err := json.Marshal(&cmd.RevokeRolesForUserRequest{User: user, Roles: []string{"custom"}, Version: cmd.RBACAssignRevokeCommandPolicyVersionV0})
			require.NoError(t, err)
			require.NoError(t, m.RevokeRolesForUser(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER, SubCommand: sub}))
		}
	}
	// customRows are the rows v1.29 stored for createCustom, the internal
	// placeholder assignment included.
	customRows := []string{
		"p, role:custom, users/alice, U, users",
		"p, role:custom, cluster/.*, R, cluster",
		"g, user:wv_internal_empty, role:custom",
	}

	tests := []struct {
		name      string
		policyCSV []string
		log       []func(t *testing.T, m *Manager)
	}{
		{
			name:      "role created below the latest command version",
			policyCSV: customRows,
			log:       []func(t *testing.T, m *Manager){createCustom},
		},
		{
			name:      "user assignment",
			policyCSV: append(customRows, "g, user:alice, role:custom"),
			log:       []func(t *testing.T, m *Manager){createCustom, assign("user:alice")},
		},
		{
			name:      "group assignment",
			policyCSV: append(customRows, "g, group:eng, role:custom"),
			log:       []func(t *testing.T, m *Manager){createCustom, assign("group:eng")},
		},
		{
			// The user eng shares the group's name, so a revoke split into
			// db/oidc subjects removes the user's grant and leaves the group's.
			name:      "group revocation",
			policyCSV: append(customRows, "g, user:eng, role:custom"),
			log:       []func(t *testing.T, m *Manager){createCustom, assign("group:eng"), assign("user:eng"), revoke("group:eng")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upgradedDir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(upgradedDir, "rbac"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(upgradedDir, "rbac", "policy.csv"), []byte(strings.Join(tt.policyCSV, "\n")+"\n"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(upgradedDir, "rbac", "version"), []byte("1.29.11"), 0o600))
			upgraded, err := rbac.New(upgradedDir, rbacconf.Config{Enabled: true}, authN, false, nil, logrus.New())
			require.NoError(t, err)

			replayedAuthZ, err := rbac.New(t.TempDir(), rbacconf.Config{Enabled: true}, authN, false, nil, logrus.New())
			require.NoError(t, err)
			replayed := NewManager(replayedAuthZ, authN, logrus.New(), make(chan struct{}, 1))
			for _, apply := range tt.log {
				apply(t, replayed)
			}

			want, got := casbinRows(t, upgraded), casbinRows(t, replayedAuthZ)
			require.Contains(t, want.GroupingPolicy, []string{"db:wv_internal_empty", "role:custom"}, "the v1.29 policy.csv must load")
			assert.ElementsMatch(t, want.Policy, got.Policy, "p rows")
			assert.ElementsMatch(t, want.GroupingPolicy, got.GroupingPolicy, "g rows")
		})
	}
}

type casbinRowSet struct {
	Policy         [][]string `json:"roles_policies"`
	GroupingPolicy [][]string `json:"grouping_policies"`
}

func casbinRows(t *testing.T, authZ *rbac.Manager) casbinRowSet {
	t.Helper()
	b, err := authZ.Snapshot()
	require.NoError(t, err)
	var r casbinRowSet
	require.NoError(t, json.Unmarshal(b, &r))
	return r
}
