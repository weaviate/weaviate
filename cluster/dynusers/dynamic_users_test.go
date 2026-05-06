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

// newNamespacesMock returns an Exister mock that reports `known` as the set
// of existing namespaces. AssertExpectations is intentionally suppressed via
// MaybeCalled so tests that exercise the empty-namespace short-circuit don't
// fail when Exists never gets called.
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
		nsKnown   bool
		wantErr   bool
	}{
		{name: "namespace exists", namespace: "ns1", nsKnown: true},
		{name: "empty namespace skips check", namespace: ""},
		{name: "namespace deleted before apply", namespace: "ns1", nsKnown: false, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ns *usecasesNamespaces.MockExister
			if tc.nsKnown {
				ns = newNamespacesMock(t, tc.namespace)
			} else {
				ns = newNamespacesMock(t)
			}
			m, dynUser := newTestManager(t, ns)

			apply := &cmd.ApplyRequest{SubCommand: mustMarshalJSON(t, cmd.CreateUsersRequest{
				UserId:         "u1",
				SecureHash:     hash,
				UserIdentifier: identifier,
				Namespace:      tc.namespace,
				CreatedAt:      time.Now(),
			})}
			err := m.CreateUser(apply)
			if tc.wantErr {
				require.Error(t, err)
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
