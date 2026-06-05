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

package dynusers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// newNamespacesMock returns an Exister mock that reports `known` as active
// namespaces (Exists and IsActive both return true).
func newNamespacesMock(t *testing.T, known ...string) *usecasesNamespaces.MockExister {
	t.Helper()
	m := &usecasesNamespaces.MockExister{}
	m.Test(t)
	set := make(map[string]struct{}, len(known))
	for _, n := range known {
		set[n] = struct{}{}
	}
	m.On("Exists", mock.AnythingOfType("string")).Return(func(name string) bool {
		_, ok := set[name]
		return ok
	}).Maybe()
	m.On("IsActive", mock.AnythingOfType("string")).Return(func(name string) bool {
		_, ok := set[name]
		return ok
	}).Maybe()
	return m
}

// newNamespacesMockDeleting reports `name` as existing-but-deleting
// (Exists=true, IsActive=false).
func newNamespacesMockDeleting(t *testing.T, name string) *usecasesNamespaces.MockExister {
	t.Helper()
	m := &usecasesNamespaces.MockExister{}
	m.Test(t)
	m.On("Exists", mock.AnythingOfType("string")).Return(func(n string) bool {
		return n == name
	}).Maybe()
	m.On("IsActive", mock.AnythingOfType("string")).Return(false).Maybe()
	return m
}

func newTestManager(t *testing.T, ns usecasesNamespaces.Exister) (*Manager, *apikey.DBUser) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger)
	require.NoError(t, err)
	return NewManager(dynUser, ns, false, logger), dynUser
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestNewManager_NilNamespacesPanics(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger)
	require.NoError(t, err)
	assert.Panics(t, func() { NewManager(dynUser, nil, false, logger) })
}

func TestManager_CreateUser(t *testing.T) {
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	tests := []struct {
		name      string
		namespace string
		makeMock  func(t *testing.T) *usecasesNamespaces.MockExister
		wantErrIs error
	}{
		{
			name:      "active namespace passes",
			namespace: "ns1",
			makeMock:  func(t *testing.T) *usecasesNamespaces.MockExister { return newNamespacesMock(t, "ns1") },
		},
		{
			name:      "empty namespace skips check",
			namespace: "",
			makeMock:  func(t *testing.T) *usecasesNamespaces.MockExister { return newNamespacesMock(t) },
		},
		{
			name:      "missing namespace returns ErrNamespaceGone",
			namespace: "ns1",
			makeMock:  func(t *testing.T) *usecasesNamespaces.MockExister { return newNamespacesMock(t) },
			wantErrIs: usecasesNamespaces.ErrNamespaceGone,
		},
		{
			name:      "deleting namespace returns ErrNamespaceDeleting",
			namespace: "ns1",
			makeMock:  func(t *testing.T) *usecasesNamespaces.MockExister { return newNamespacesMockDeleting(t, "ns1") },
			wantErrIs: usecasesNamespaces.ErrNamespaceDeleting,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, dynUser := newTestManager(t, tc.makeMock(t))

			apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
				UserId:         "u1",
				SecureHash:     hash,
				UserIdentifier: identifier,
				Namespace:      tc.namespace,
				CreatedAt:      time.Now(),
			})}
			err := m.CreateUser(apply)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
				users, _ := dynUser.GetUsers("u1")
				assert.Empty(t, users, "user must not be persisted on rejection")
				return
			}
			require.NoError(t, err)
			users, err := dynUser.GetUsers("u1")
			require.NoError(t, err)
			require.NotNil(t, users["u1"])
			assert.Equal(t, tc.namespace, users["u1"].Namespace)
		})
	}
}

// TestManager_CreateUser_NamespacesEnabledRejectsEmpty exercises the
// apply-layer defense-in-depth: when NamespacesEnabled is true, an empty
// Namespace must be rejected even though the handler should never produce
// such a request.
func TestManager_CreateUser_NamespacesEnabledRejectsEmpty(t *testing.T) {
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	ns := newNamespacesMock(t)
	logger, _ := test.NewNullLogger()
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger)
	require.NoError(t, err)
	m := NewManager(dynUser, ns, true, logger)

	apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
		UserId:         "u1",
		SecureHash:     hash,
		UserIdentifier: identifier,
		Namespace:      "",
		CreatedAt:      time.Now(),
	})}
	require.Error(t, m.CreateUser(apply))
	users, _ := dynUser.GetUsers("u1")
	assert.Empty(t, users)
}

// TestManager_MalformedJSON exercises every apply/query method's defensive
// json.Unmarshal path. Each method must wrap the underlying error with
// ErrBadRequest so the FSM dispatcher can classify it distinctly.
func TestManager_MalformedJSON(t *testing.T) {
	bad := []byte("not-json")
	apply := &cmd.ApplyRequest{SubCommand: bad}
	query := &cmd.QueryRequest{SubCommand: bad}

	tests := []struct {
		name string
		call func(m *Manager) error
	}{
		{name: "CreateUser", call: func(m *Manager) error { return m.CreateUser(apply) }},
		{name: "DeleteUser", call: func(m *Manager) error { return m.DeleteUser(apply) }},
		{name: "DeleteUsersInNamespace", call: func(m *Manager) error { return m.DeleteUsersInNamespace(apply) }},
		{name: "ActivateUser", call: func(m *Manager) error { return m.ActivateUser(apply) }},
		{name: "SuspendUser", call: func(m *Manager) error { return m.SuspendUser(apply) }},
		{name: "RotateKey", call: func(m *Manager) error { return m.RotateKey(apply) }},
		{name: "CreateUserWithKeyRequest", call: func(m *Manager) error { return m.CreateUserWithKeyRequest(apply) }},
		{name: "GetUsers", call: func(m *Manager) error { _, err := m.GetUsers(query); return err }},
		{name: "CheckUserIdentifierExists", call: func(m *Manager) error { _, err := m.CheckUserIdentifierExists(query); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newTestManager(t, newNamespacesMock(t))
			err := tc.call(m)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrBadRequest)
		})
	}
}

func TestManager_CheckUserIdentifierExists(t *testing.T) {
	m, _ := newTestManager(t, newNamespacesMock(t))

	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
		UserId:         "u1",
		SecureHash:     hash,
		UserIdentifier: identifier,
		CreatedAt:      time.Now(),
	})}
	require.NoError(t, m.CreateUser(apply))

	query := &cmd.QueryRequest{SubCommand: mustMarshalJSON(t, cmd.QueryUserIdentifierExistsRequest{
		UserIdentifier: identifier,
	})}
	payload, err := m.CheckUserIdentifierExists(query)
	require.NoError(t, err)

	var response cmd.QueryUserIdentifierExistsResponse
	require.NoError(t, json.Unmarshal(payload, &response))
	require.True(t, response.Exists)

	query = &cmd.QueryRequest{SubCommand: mustMarshalJSON(t, cmd.QueryUserIdentifierExistsRequest{
		UserIdentifier: "u1",
	})}
	payload, err = m.CheckUserIdentifierExists(query)
	require.NoError(t, err)

	require.NoError(t, json.Unmarshal(payload, &response))
	require.False(t, response.Exists)
}

func TestManager_DeleteUsersInNamespace(t *testing.T) {
	seed := func(t *testing.T, m *Manager, id, namespace string) {
		t.Helper()
		_, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
			UserId:         id,
			SecureHash:     hash,
			UserIdentifier: identifier,
			Namespace:      namespace,
			CreatedAt:      time.Now(),
		})}
		require.NoError(t, m.CreateUser(apply))
	}

	type user struct{ id, namespace string }
	tests := []struct {
		name            string
		known           []string
		seeds           []user
		deleteNamespace string
		extraRuns       int
		wantErrIs       error
		wantRemaining   []string
	}{
		{
			name:            "removes only matching users",
			known:           []string{"alpha", "beta"},
			seeds:           []user{{"u-alpha-1", "alpha"}, {"u-alpha-2", "alpha"}, {"u-beta", "beta"}},
			deleteNamespace: "alpha",
			wantRemaining:   []string{"u-beta"},
		},
		{
			name:            "rerun on already-empty namespace is a no-op",
			known:           []string{"alpha"},
			seeds:           []user{{"u1", "alpha"}},
			deleteNamespace: "alpha",
			extraRuns:       1,
		},
		{
			name:            "empty namespace is rejected",
			deleteNamespace: "",
			wantErrIs:       ErrBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, dyn := newTestManager(t, newNamespacesMock(t, tc.known...))
			for _, s := range tc.seeds {
				seed(t, m, s.id, s.namespace)
			}

			apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.DeleteUsersInNamespaceRequest{Namespace: tc.deleteNamespace})}
			var err error
			for i := 0; i <= tc.extraRuns; i++ {
				err = m.DeleteUsersInNamespace(apply)
			}
			if tc.wantErrIs != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErrIs)
				return
			}
			require.NoError(t, err)
			if tc.wantRemaining != nil {
				users, err := dyn.GetUsers()
				require.NoError(t, err)
				require.Len(t, users, len(tc.wantRemaining))
				for _, id := range tc.wantRemaining {
					require.Contains(t, users, id)
				}
			}
		})
	}

	t.Run("nil dynUser is a no-op", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		ns := newNamespacesMock(t)
		m := NewManager(nil, ns, false, logger)
		apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.DeleteUsersInNamespaceRequest{Namespace: "alpha"})}
		require.NoError(t, m.DeleteUsersInNamespace(apply))
	})
}
