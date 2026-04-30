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
	return NewManager(usecasesNamespaces.NewController(logger), logger)
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

func TestNewManager_NilControllerPanics(t *testing.T) {
	logger, _ := test.NewNullLogger()
	assert.Panics(t, func() {
		NewManager(nil, logger)
	})
}

func TestManager_Add(t *testing.T) {
	m := newTestManager(t)

	t.Run("happy path dispatches to controller", func(t *testing.T) {
		require.NoError(t, m.Add(addCmd(t, "customer1")))
		assert.Equal(t, 1, m.Count())
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		err := m.Add(&cmd.ApplyRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})
}

func TestManager_Delete(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))

	t.Run("happy path dispatches to controller", func(t *testing.T) {
		require.NoError(t, m.Delete(deleteCmd(t, "customer1")))
		assert.Equal(t, 0, m.Count())
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		err := m.Delete(&cmd.ApplyRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})
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
