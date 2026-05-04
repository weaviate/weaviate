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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// setupRaftForNamespaceTests spins up a single-node RAFT cluster wired through
// NewMockStore and waits until it has leadership. Tests can then exercise the
// AddNamespace / DeleteNamespace / GetNamespaces endpoints end-to-end (from
// client call through Execute/Query, RAFT apply, back up).
func setupRaftForNamespaceTests(t *testing.T) (*Raft, context.Context, func()) {
	t.Helper()
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)

	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("Close", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second*1, make(chan struct{})))
	require.True(t, tryNTimesWithWait(20, time.Millisecond*100, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))

	cleanup := func() {
		_ = srv.Close(ctx)
	}
	return srv, ctx, cleanup
}

func TestRaftNamespaceEndpoints(t *testing.T) {
	srv, _, cleanup := setupRaftForNamespaceTests(t)
	defer cleanup()

	// Initially empty.
	assert.Equal(t, 0, srv.NamespaceCount())
	all, err := srv.GetNamespaces()
	require.NoError(t, err)
	assert.Empty(t, all)

	t.Run("add a namespace", func(t *testing.T) {
		require.NoError(t, srv.AddNamespace(cmd.Namespace{Name: "customer1"}))
		assert.Equal(t, 1, srv.NamespaceCount())
	})

	t.Run("add a duplicate returns ErrAlreadyExists", func(t *testing.T) {
		err := srv.AddNamespace(cmd.Namespace{Name: "customer1"})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrAlreadyExists)
	})

	t.Run("add an invalid name returns ErrBadRequest", func(t *testing.T) {
		err := srv.AddNamespace(cmd.Namespace{Name: "BadName"})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})

	t.Run("add a reserved name returns ErrBadRequest", func(t *testing.T) {
		err := srv.AddNamespace(cmd.Namespace{Name: "admin"})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})

	t.Run("add a second namespace and list all", func(t *testing.T) {
		require.NoError(t, srv.AddNamespace(cmd.Namespace{Name: "customer2"}))
		assert.Equal(t, 2, srv.NamespaceCount())

		all, err := srv.GetNamespaces()
		require.NoError(t, err)
		got := make([]string, 0, len(all))
		for _, ns := range all {
			got = append(got, ns.Name)
		}
		assert.ElementsMatch(t, []string{"customer1", "customer2"}, got)
	})

	t.Run("get specific names", func(t *testing.T) {
		got, err := srv.GetNamespaces("customer1", "never-existed")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "customer1", got[0].Name)
	})

	t.Run("delete an existing namespace", func(t *testing.T) {
		require.NoError(t, srv.DeleteNamespace("customer1"))
		assert.Equal(t, 1, srv.NamespaceCount())
	})

	t.Run("delete a missing namespace returns ErrNotFound", func(t *testing.T) {
		err := srv.DeleteNamespace("never-existed")
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrNotFound)

		// State was not altered.
		assert.Equal(t, 1, srv.NamespaceCount())
	})
}
