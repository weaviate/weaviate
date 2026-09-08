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

package cluster

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterdynusers "github.com/weaviate/weaviate/cluster/dynusers"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterrbac "github.com/weaviate/weaviate/cluster/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// rolesAndUsersStores is the pair of managers the state-machine apply drives,
// plus the raw stores the assertions read.
type rolesAndUsersStores struct {
	authZManager *clusterrbac.Manager
	dynManager   *clusterdynusers.Manager
	authZ        *rbac.Manager
	dynUser      *apikey.DBUser
	// policyDir holds policy.csv. Tests delete it to force a store-level failure
	// in the role apply.
	policyDir string
}

// staticLister reports each named namespace as existing, for the source side
// of snapshot fixtures.
type staticLister []string

func (l staticLister) List() []api.Namespace {
	out := make([]api.Namespace, len(l))
	for i, name := range l {
		out[i] = api.Namespace{Name: name, State: api.NamespaceStateActive}
	}
	return out
}

func newRolesAndUsersStores(t *testing.T, ns usecasesNamespaces.Exister) *rolesAndUsersStores {
	t.Helper()
	logger, _ := test.NewNullLogger()

	policyDir := t.TempDir()
	// The lister makes snapshots carry ns1 in their Namespaces list, as a real
	// source with namespaces on would. Nothing else reads it.
	authZ, err := rbac.New(filepath.Join(policyDir, "policy.csv"),
		rbacconf.Config{Enabled: true}, config.Authentication{}, true, staticLister{"ns1"}, logger)
	require.NoError(t, err)
	// enabled is false so the one-minute storeToFile ticker cannot mask a
	// missing persistence call.
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger, ns)
	require.NoError(t, err)

	return &rolesAndUsersStores{
		authZManager: clusterrbac.NewManager(authZ, config.Authentication{}, logger),
		dynManager:   clusterdynusers.NewManager(dynUser, ns, false, logger),
		authZ:        authZ,
		dynUser:      dynUser,
		policyDir:    policyDir,
	}
}

func (s *rolesAndUsersStores) createRole(t *testing.T, name string) {
	t.Helper()
	require.NoError(t, s.authZ.CreateRolesPermissions(map[string][]authorization.Policy{
		name: {{Resource: authorization.Cluster(), Domain: authorization.ClusterDomain, Verb: authorization.READ}},
	}))
}

func (s *rolesAndUsersStores) createUser(t *testing.T, id, namespace string) {
	t.Helper()
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, s.dynUser.CreateUser(id, hash, identifier, "", namespace, time.Now()))
}

func (s *rolesAndUsersStores) customRoles(t *testing.T) []string {
	t.Helper()
	roles, err := s.authZ.GetRoles()
	require.NoError(t, err)
	var out []string
	for name := range roles {
		if !slices.Contains(authorization.BuiltInRoles, name) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func (s *rolesAndUsersStores) userIDs(t *testing.T) []string {
	t.Helper()
	users, err := s.dynUser.GetUsers()
	require.NoError(t, err)
	out := make([]string, 0, len(users))
	for id := range users {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func mustApplyRequest(t *testing.T, req api.RestoreRolesAndUsersRequest) *api.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(&req)
	require.NoError(t, err)
	return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_RESTORE_ROLES_AND_USERS, SubCommand: sub}
}

// TestApplyRestoreRolesAndUsersValidatesBeforeMutating pins that neither store
// is touched unless both payloads pass, and that on success roles land before
// users.
func TestApplyRestoreRolesAndUsersValidatesBeforeMutating(t *testing.T) {
	activeNS := map[string]api.NamespaceState{"ns1": api.NamespaceStateActive}

	// The blobs under test, taken from a source cluster.
	source := newRolesAndUsersStores(t, usecasesNamespaces.NewMockExisterInState(t, activeNS))
	source.createRole(t, "restored")
	source.createRole(t, "ns1:scoped")
	source.createUser(t, "restored-user", "")
	source.createUser(t, apikey.MakeUserKey("scoped-user", "ns1"), "ns1")

	goodRoles, err := source.authZ.Snapshot("restored")
	require.NoError(t, err)
	goodUsers, err := source.dynUser.Snapshot("restored-user")
	require.NoError(t, err)
	nsRoles, err := source.authZ.Snapshot("ns1:scoped")
	require.NoError(t, err)
	nsUsers, err := source.dynUser.Snapshot(apikey.MakeUserKey("scoped-user", "ns1"))
	require.NoError(t, err)
	badVersionUsers, err := json.Marshal(apikey.DBUserSnapshot{Version: apikey.SnapshotVersion + 1})
	require.NoError(t, err)

	tests := []struct {
		name      string
		req       api.RestoreRolesAndUsersRequest
		states    map[string]api.NamespaceState
		wantErr   string
		wantRoles []string
		wantUsers []string
	}{
		{
			name:      "both valid: both stores replaced",
			req:       api.RestoreRolesAndUsersRequest{Roles: goodRoles, Users: goodUsers},
			states:    activeNS,
			wantRoles: []string{"restored"},
			wantUsers: []string{"restored-user"},
		},
		{
			name:      "bad user snapshot version leaves both stores untouched",
			req:       api.RestoreRolesAndUsersRequest{Roles: goodRoles, Users: badVersionUsers},
			states:    activeNS,
			wantErr:   "invalid snapshot version",
			wantRoles: []string{"incumbent"},
			wantUsers: []string{"incumbent-user"},
		},
		{
			name:      "roles naming a missing namespace leave both stores untouched",
			req:       api.RestoreRolesAndUsersRequest{Roles: nsRoles, Users: goodUsers},
			states:    map[string]api.NamespaceState{},
			wantErr:   "ns1",
			wantRoles: []string{"incumbent"},
			wantUsers: []string{"incumbent-user"},
		},
		{
			name:      "users naming a missing namespace leave both stores untouched",
			req:       api.RestoreRolesAndUsersRequest{Roles: goodRoles, Users: nsUsers},
			states:    map[string]api.NamespaceState{},
			wantErr:   "ns1",
			wantRoles: []string{"incumbent"},
			wantUsers: []string{"incumbent-user"},
		},
		{
			// A suspended namespace keeps its rows, so restoring rows for it is
			// legal and must not block the restore.
			name:      "suspended namespace: both stores replaced",
			req:       api.RestoreRolesAndUsersRequest{Roles: nsRoles, Users: nsUsers},
			states:    map[string]api.NamespaceState{"ns1": api.NamespaceStateSuspended},
			wantRoles: []string{"ns1:scoped"},
			wantUsers: []string{apikey.MakeUserKey("scoped-user", "ns1")},
		},
		{
			name:      "deleting namespace leaves both stores untouched",
			req:       api.RestoreRolesAndUsersRequest{Roles: nsRoles, Users: nsUsers},
			states:    map[string]api.NamespaceState{"ns1": api.NamespaceStateDeleting},
			wantErr:   "ns1",
			wantRoles: []string{"incumbent"},
			wantUsers: []string{"incumbent-user"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := usecasesNamespaces.NewMockExisterInState(t, tt.states)
			target := newRolesAndUsersStores(t, ns)
			target.createRole(t, "incumbent")
			target.createUser(t, "incumbent-user", "")

			err := applyRestoreRolesAndUsers(mustApplyRequest(t, tt.req),
				target.authZManager, target.dynManager, ns)

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
			assert.Equal(t, tt.wantRoles, target.customRoles(t))
			assert.Equal(t, tt.wantUsers, target.userIDs(t))
		})
	}
}

func TestApplyRestoreRolesAndUsersRejectsMalformedSubCommand(t *testing.T) {
	ns := usecasesNamespaces.NewMockExisterInState(t, nil)
	target := newRolesAndUsersStores(t, ns)
	target.createRole(t, "incumbent")

	err := applyRestoreRolesAndUsers(&api.ApplyRequest{SubCommand: []byte("not json")},
		target.authZManager, target.dynManager, ns)

	require.Error(t, err)
	assert.Equal(t, []string{"incumbent"}, target.customRoles(t))
}

// TestApplyRestoreRolesAndUsersLeavesUsersOnRoleFailure pins the roles-then-users
// order: the role apply is the only step after validation that can fail, and
// its failure must leave the user store untouched.
func TestApplyRestoreRolesAndUsersLeavesUsersOnRoleFailure(t *testing.T) {
	ns := usecasesNamespaces.NewMockExisterInState(t, nil)

	source := newRolesAndUsersStores(t, ns)
	source.createRole(t, "restored")
	source.createUser(t, "restored-user", "")
	roles, err := source.authZ.Snapshot("restored")
	require.NoError(t, err)
	users, err := source.dynUser.Snapshot("restored-user")
	require.NoError(t, err)

	target := newRolesAndUsersStores(t, ns)
	target.createUser(t, "incumbent-user", "")
	// The casbin file adapter writes policy.csv on every change. With the
	// directory gone, the role apply fails after ClearPolicy.
	require.NoError(t, os.RemoveAll(target.policyDir))

	err = applyRestoreRolesAndUsers(mustApplyRequest(t, api.RestoreRolesAndUsersRequest{Roles: roles, Users: users}),
		target.authZManager, target.dynManager, ns)

	require.Error(t, err)
	assert.Equal(t, []string{"incumbent-user"}, target.userIDs(t),
		"a failed role apply must not have replaced the user store")
}
