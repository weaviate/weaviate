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
	return NewManager(usecasesNamespaces.NewController(logger), stubResiduals{}, nil, logger)
}

func newTestManagerWithResiduals(t *testing.T, schema SchemaNamespaceLister, dynusers DynusersNamespaceLister) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(usecasesNamespaces.NewController(logger), schema, dynusers, logger)
}

func addCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.AddNamespaceRequest{Namespace: cmd.Namespace{Name: name}})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func deleteCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.DeleteNamespaceRequest{Name: name})
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
		{name: "nil controller panics", controller: nil, schema: stubResiduals{}},
		{name: "nil schema lister panics", controller: controller, schema: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewManager(tc.controller, tc.schema, nil, logger)
			})
		})
	}
}

func TestManager_Add(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	assert.Equal(t, 1, m.Count())
}

func TestManager_Delete(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	require.NoError(t, m.Delete(deleteCmd(t, "customer1")))
	assert.Equal(t, 0, m.Count())
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

// stubResiduals is a residuals reader stub.
type stubResiduals struct {
	classes []string
	aliases []string
	users   []string
}

func (s stubResiduals) ClassesInNamespace(string) []string { return s.classes }
func (s stubResiduals) AliasesInNamespace(string) []string { return s.aliases }
func (s stubResiduals) UsersInNamespace(string) []string   { return s.users }

func TestManager_RemoveEntity_Residuals(t *testing.T) {
	tests := []struct {
		name      string
		residuals stubResiduals
		wantErr   error
	}{
		{name: "no residuals removes the entity", residuals: stubResiduals{}},
		{name: "residual class blocks", residuals: stubResiduals{classes: []string{"customer1:Foo"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "residual alias blocks", residuals: stubResiduals{aliases: []string{"customer1:Bar"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "residual user blocks", residuals: stubResiduals{users: []string{"u1"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManagerWithResiduals(t, tc.residuals, tc.residuals)
			require.NoError(t, m.Add(addCmd(t, "customer1")))
			require.NoError(t, m.ChangeState(changeStateCmd(t, "customer1", cmd.NamespaceStateDeleting)))

			err := m.RemoveEntity(removeEntityCmd(t, "customer1"))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.True(t, m.Exists("customer1"), "namespace must remain when residuals block removal")
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
		{name: "Delete", call: func(m *Manager) error { return m.Delete(bad) }},
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
