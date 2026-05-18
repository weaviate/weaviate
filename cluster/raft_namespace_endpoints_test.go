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
// namespace endpoints end-to-end (from client call through Execute/Query,
// RAFT apply, back up).
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
	require.True(t, tryNTimesWithWait(20, time.Millisecond*200, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))

	cleanup := func() {
		_ = srv.Close(ctx)
	}
	return srv, ctx, cleanup
}

func TestRaftNamespaceEndpoints(t *testing.T) {
	srv, ctx, cleanup := setupRaftForNamespaceTests(t)
	defer cleanup()

	// seed adds a namespace and blocks until the apply is visible. Subtests
	// use unique names so they can run in any order without colliding.
	seed := func(t *testing.T, name string) {
		t.Helper()
		_, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: name})
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
	}

	t.Run("AddNamespace persists and bumps the count", func(t *testing.T) {
		before := srv.NamespaceCount()
		seed(t, "add-target")
		assert.Equal(t, before+1, srv.NamespaceCount())

		got, err := srv.GetNamespaces("add-target")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "add-target", got[0].Name)
	})

	t.Run("AddNamespace error mapping", func(t *testing.T) {
		seed(t, "dup-target")
		errCases := []struct {
			name    string
			input   string
			wantErr error
		}{
			{name: "duplicate", input: "dup-target", wantErr: usecasesNamespaces.ErrAlreadyExists},
			{name: "invalid name", input: "BadName", wantErr: usecasesNamespaces.ErrBadRequest},
			{name: "reserved name", input: "admin", wantErr: usecasesNamespaces.ErrBadRequest},
		}
		for _, tc := range errCases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := srv.AddNamespace(ctx, cmd.Namespace{Name: tc.input})
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
			})
		}
	})

	t.Run("GetNamespaces returns only the requested names", func(t *testing.T) {
		seed(t, "get-target")
		got, err := srv.GetNamespaces("get-target", "never-existed")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "get-target", got[0].Name)
	})

	t.Run("GetNamespaces with no args returns all seeded namespaces", func(t *testing.T) {
		seed(t, "list-a")
		seed(t, "list-b")
		all, err := srv.GetNamespaces()
		require.NoError(t, err)
		names := make([]string, 0, len(all))
		for _, ns := range all {
			names = append(names, ns.Name)
		}
		assert.Contains(t, names, "list-a")
		assert.Contains(t, names, "list-b")
	})

	t.Run("two-phase delete removes the entity", func(t *testing.T) {
		seed(t, "delete-target")
		before := srv.NamespaceCount()

		version, err := srv.ChangeNamespaceState(ctx, "delete-target", cmd.NamespaceStateDeleting)
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.Equal(t, before, srv.NamespaceCount(), "entity stays until RemoveNamespaceEntity")

		version, err = srv.RemoveNamespaceEntity(ctx, "delete-target")
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.Equal(t, before-1, srv.NamespaceCount())
	})

	t.Run("ChangeNamespaceState on missing returns ErrNotFound", func(t *testing.T) {
		_, err := srv.ChangeNamespaceState(ctx, "never-existed", cmd.NamespaceStateDeleting)
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrNotFound)
	})
}

// TestRaftNamespaceEndpoints_HomeNode verifies that AddNamespace fills
// HomeNode from the current storage candidates when the caller omits it,
// and that an explicitly supplied HomeNode is persisted as-is.
func TestRaftNamespaceEndpoints_HomeNode(t *testing.T) {
	srv, ctx, cleanup := setupRaftForNamespaceTests(t)
	defer cleanup()

	t.Run("empty home_node is filled from storage candidates", func(t *testing.T) {
		created, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "autohome"})
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.NotEmpty(t, created.HomeNode, "home_node should be populated by AddNamespace")
		assert.Contains(t, srv.StorageCandidates(), created.HomeNode)

		got, err := srv.GetNamespaces("autohome")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, created.HomeNode, got[0].HomeNode)
	})

	t.Run("explicit home_node is persisted as-is", func(t *testing.T) {
		hn := srv.StorageCandidates()[0]
		created, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "explicithome", HomeNode: hn})
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.Equal(t, hn, created.HomeNode)

		got, err := srv.GetNamespaces("explicithome")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, hn, got[0].HomeNode)
	})
}

// TestRaftNamespaceEndpoints_Update covers UpdateNamespace: it rewrites the
// stored HomeNode for existing namespaces and returns typed errors for
// missing ones or invalid payloads.
func TestRaftNamespaceEndpoints_Update(t *testing.T) {
	srv, ctx, cleanup := setupRaftForNamespaceTests(t)
	defer cleanup()

	// Seed once; the wantErr cases below don't mutate the stored namespace,
	// so cases share the same fixture.
	_, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "customer1", HomeNode: "node-a"})
	require.NoError(t, err)
	require.NoError(t, srv.WaitForUpdate(ctx, version))

	tests := []struct {
		name       string
		ns         cmd.Namespace
		wantErr    error
		wantStored string // expected HomeNode after a successful update
	}{
		{name: "update rewrites home_node", ns: cmd.Namespace{Name: "customer1", HomeNode: "node-b"}, wantStored: "node-b"},
		{name: "update missing returns ErrNotFound", ns: cmd.Namespace{Name: "never-existed", HomeNode: "node-a"}, wantErr: usecasesNamespaces.ErrNotFound},
		{name: "update empty home_node returns ErrBadRequest", ns: cmd.Namespace{Name: "customer1"}, wantErr: usecasesNamespaces.ErrBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := srv.UpdateNamespace(ctx, tc.ns)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, srv.WaitForUpdate(ctx, v))
			got, err := srv.GetNamespaces(tc.ns.Name)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantStored, got[0].HomeNode)
		})
	}
}

// TestNextHomeNode_RoundRobin exercises the persisted iterator with a fixed
// candidate list. After the (random) first pick, successive calls advance
// by exactly one position; once the iterator reaches the end of the list
// it wraps. The test doesn't pin which slot is picked first (that's random
// by design), only that no slot is repeated within a full N-call rotation.
func TestNextHomeNode_RoundRobin(t *testing.T) {
	r := &Raft{}
	nodes := []string{"A", "B", "C", "D"}

	first, err := r.nextHomeNode(nodes)
	require.NoError(t, err)
	require.Contains(t, nodes, first)

	seen := map[string]int{first: 1}
	for i := 0; i < len(nodes)-1; i++ {
		got, err := r.nextHomeNode(nodes)
		require.NoError(t, err)
		seen[got]++
	}
	for _, n := range nodes {
		assert.Equal(t, 1, seen[n], "node %s should be picked exactly once in one rotation, got %d", n, seen[n])
	}

	wrap, err := r.nextHomeNode(nodes)
	require.NoError(t, err)
	assert.Equal(t, first, wrap, "after a full rotation the iterator should wrap back to the first pick")
}

// TestNextHomeNode_RebuildsOnCandidateChange covers the membership-change
// case: when the candidate set differs from the one the cached iterator was
// built with, the iterator is rebuilt so newly added nodes become eligible
// and removed nodes drop out. Without the rebuild, callers would keep
// rotating through the stale set indefinitely.
func TestNextHomeNode_RebuildsOnCandidateChange(t *testing.T) {
	r := &Raft{}
	initial := []string{"A", "B"}

	// Burn through one full rotation on the initial set to seed the iterator.
	seen := map[string]bool{}
	for i := 0; i < len(initial); i++ {
		got, err := r.nextHomeNode(initial)
		require.NoError(t, err)
		seen[got] = true
	}
	assert.Equal(t, map[string]bool{"A": true, "B": true}, seen, "initial rotation should cover the initial set")

	// Membership change: drop B, add C and D. The iterator must rebuild;
	// otherwise it would still hand back B (and never C or D).
	updated := []string{"A", "C", "D"}
	seen = map[string]bool{}
	for i := 0; i < len(updated); i++ {
		got, err := r.nextHomeNode(updated)
		require.NoError(t, err)
		assert.Contains(t, updated, got, "post-change pick must come from the updated set, never the removed node")
		seen[got] = true
	}
	assert.Equal(t, map[string]bool{"A": true, "C": true, "D": true}, seen, "post-change rotation should cover the updated set exactly once")
}

// TestNextHomeNode_NoCandidates returns an error rather than picking nothing.
func TestNextHomeNode_NoCandidates(t *testing.T) {
	r := &Raft{}
	_, err := r.nextHomeNode(nil)
	require.Error(t, err)
}
