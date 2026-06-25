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

package namespaces

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(usecasesNamespaces.NewController(logger), stubLeftovers{}, nil, nil, logger)
}

func newTestManagerWithLeftovers(t *testing.T, schema SchemaNamespaceLister, dynusers DynusersNamespaceLister, rbac RBACNamespaceLister) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(usecasesNamespaces.NewController(logger), schema, dynusers, rbac, logger)
}

func addCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.AddNamespaceRequest{Namespace: cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}}})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func updateCmd(t *testing.T, name, homeNode string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.UpdateNamespaceRequest{Namespace: cmd.Namespace{Name: name, HomeNodes: []string{homeNode}}})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func changeStateCmd(t *testing.T, name string, target cmd.NamespaceState) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.ChangeNamespaceStateRequest{Name: name, TargetState: target})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func removeEntityCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.RemoveNamespaceEntityRequest{Name: name})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

// seedNamespace creates name and transitions it to seedState. An empty
// seedState seeds nothing.
func seedNamespace(t *testing.T, m *Manager, name string, seedState cmd.NamespaceState) {
	t.Helper()
	if seedState == "" {
		return
	}
	require.NoError(t, m.Add(addCmd(t, name)))
	if seedState == cmd.NamespaceStateDeleting {
		require.NoError(t, m.ChangeState(changeStateCmd(t, name, cmd.NamespaceStateDeleting)))
	}
}

func TestNewManager_RequiredArgsPanic(t *testing.T) {
	logger, _ := test.NewNullLogger()
	controller := usecasesNamespaces.NewController(logger)

	tests := []struct {
		name       string
		controller *usecasesNamespaces.Controller
		schema     SchemaNamespaceLister
	}{
		{name: "nil controller panics", controller: nil, schema: stubLeftovers{}},
		{name: "nil schema lister panics", controller: controller, schema: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewManager(tc.controller, tc.schema, nil, nil, logger)
			})
		})
	}
}

func TestManager_Add(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	assert.Equal(t, 1, m.Count())
}

func TestManager_ChangeState(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		target    cmd.NamespaceState
		wantErr   error
	}{
		{name: "active -> deleting flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateDeleting},
		{name: "active -> active is idempotent", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateActive},
		{name: "deleting -> deleting is idempotent", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateDeleting},
		{name: "deleting -> active is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateActive, wantErr: usecasesNamespaces.ErrInvalidStateTransition},
		{name: "missing namespace returns ErrNotFound", target: cmd.NamespaceStateDeleting, wantErr: usecasesNamespaces.ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			seedNamespace(t, m, "customer1", tc.seedState)

			err := m.ChangeState(changeStateCmd(t, "customer1", tc.target))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, m.Exists("customer1"))
			assert.Equal(t, tc.target == cmd.NamespaceStateActive, m.IsActive("customer1"))
		})
	}
}

func TestManager_RemoveEntity(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		wantErr   error
	}{
		{name: "deleting namespace is removed", seedState: cmd.NamespaceStateDeleting},
		{name: "active namespace returns ErrInvalidState", seedState: cmd.NamespaceStateActive, wantErr: usecasesNamespaces.ErrInvalidState},
		{name: "missing namespace returns ErrNotFound", wantErr: usecasesNamespaces.ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			seedNamespace(t, m, "customer1", tc.seedState)

			err := m.RemoveEntity(removeEntityCmd(t, "customer1"))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.False(t, m.Exists("customer1"))
		})
	}
}

// stubLeftovers is a leftovers reader stub.
type stubLeftovers struct {
	classes []string
	aliases []string
	users   []string
}

func (s stubLeftovers) ClassesInNamespace(string) ([]string, error) { return s.classes, nil }
func (s stubLeftovers) AliasesInNamespace(string) []string          { return s.aliases }
func (s stubLeftovers) UsersInNamespace(string) []string            { return s.users }

// stubRBACRows reports a fixed count of surviving RBAC rows (or an error).
type stubRBACRows struct {
	count int
	err   error
}

func (s stubRBACRows) CountNamespaceLocalRBAC(string) (int, error) { return s.count, s.err }

func TestManager_RemoveEntity_Leftovers(t *testing.T) {
	errCount := errors.New("rbac count failed")
	tests := []struct {
		name      string
		leftovers stubLeftovers
		rbac      RBACNamespaceLister
		wantErr   error
	}{
		{name: "no leftovers removes the entity", leftovers: stubLeftovers{}},
		{name: "leftover class blocks", leftovers: stubLeftovers{classes: []string{"customer1:Foo"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover alias blocks", leftovers: stubLeftovers{aliases: []string{"customer1:Bar"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover user blocks", leftovers: stubLeftovers{users: []string{"u1"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover RBAC row blocks", rbac: stubRBACRows{count: 1}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "RBAC count error blocks removal", rbac: stubRBACRows{err: errCount}, wantErr: errCount},
		{name: "no RBAC rows removes the entity", rbac: stubRBACRows{count: 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManagerWithLeftovers(t, tc.leftovers, tc.leftovers, tc.rbac)
			require.NoError(t, m.Add(addCmd(t, "customer1")))
			require.NoError(t, m.ChangeState(changeStateCmd(t, "customer1", cmd.NamespaceStateDeleting)))

			err := m.RemoveEntity(removeEntityCmd(t, "customer1"))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.True(t, m.Exists("customer1"), "namespace must remain when leftovers block removal")
				return
			}
			require.NoError(t, err)
			assert.False(t, m.Exists("customer1"))
		})
	}
}

func TestManager_RejectsMalformedApplyRequest(t *testing.T) {
	bad := &cmd.ApplyRequest{SubCommand: []byte("not-json")}
	tests := []struct {
		name string
		call func(*Manager) error
	}{
		{name: "Add", call: func(m *Manager) error { return m.Add(bad) }},
		{name: "Update", call: func(m *Manager) error { return m.Update(bad) }},
		{name: "ChangeState", call: func(m *Manager) error { return m.ChangeState(bad) }},
		{name: "RemoveEntity", call: func(m *Manager) error { return m.RemoveEntity(bad) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			err := tc.call(m)
			require.Error(t, err)
			assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
		})
	}
}

func TestManager_Update(t *testing.T) {
	tests := []struct {
		name       string
		updateName string
		homeNode   string
		wantErr    error
		// wantHomeNode is checked on success to confirm the controller
		// actually stored the new HomeNode (i.e. dispatch worked).
		wantHomeNode string
	}{
		{name: "happy path dispatches to controller", updateName: "customer1", homeNode: "node-2", wantHomeNode: "node-2"},
		{name: "update missing returns ErrNotFound", updateName: "never-existed", homeNode: "node-2", wantErr: usecasesNamespaces.ErrNotFound},
		{name: "update empty home_node returns ErrBadRequest", updateName: "customer1", homeNode: "", wantErr: usecasesNamespaces.ErrBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, m.Add(addCmd(t, "customer1")))

			err := m.Update(updateCmd(t, tc.updateName, tc.homeNode))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			got := m.controller.Get(tc.updateName)
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantHomeNode, got[0].Primary())
		})
	}
}

func TestManager_ExistsAndIsActiveProxies(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))

	assert.True(t, m.Exists("customer1"))
	assert.True(t, m.IsActive("customer1"))
	assert.False(t, m.Exists("never-existed"))
	assert.False(t, m.IsActive("never-existed"))

	require.NoError(t, m.ChangeState(changeStateCmd(t, "customer1", cmd.NamespaceStateDeleting)))
	assert.True(t, m.Exists("customer1"))
	assert.False(t, m.IsActive("customer1"))
}

func TestManager_Get(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))

	t.Run("happy path dispatches to controller", func(t *testing.T) {
		payload, err := json.Marshal(cmd.QueryGetNamespacesRequest{})
		require.NoError(t, err)
		raw, err := m.Get(&cmd.QueryRequest{SubCommand: payload})
		require.NoError(t, err)

		var resp cmd.QueryGetNamespacesResponse
		require.NoError(t, json.Unmarshal(raw, &resp))
		require.Len(t, resp.Namespaces, 1)
		assert.Equal(t, "customer1", resp.Namespaces[0].Name)
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		_, err := m.Get(&cmd.QueryRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})
}
