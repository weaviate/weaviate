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
	"slices"
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
// RAFT apply, back up). MaximumFactor is 1, so a create without HomeNodes
// auto-picks one node.
func setupRaftForNamespaceTests(t *testing.T) (*Raft, context.Context, func()) {
	t.Helper()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), func(c *Config) { c.Replication.MaximumFactor = 1 })
	return startSingleNodeRaft(t, m)
}

// startSingleNodeRaft opens m's store as a single-node RAFT cluster and waits
// until it has leadership. StorageCandidates returns storageNodes when there
// are more of them than the one RAFT server.
func startSingleNodeRaft(t *testing.T, m MockStore, storageNodes ...string) (*Raft, context.Context, func()) {
	t.Helper()
	ctx := context.Background()
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)

	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("Close", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	srv := NewRaft(mocks.NewMockNodeSelector(storageNodes...), m.store, nil)
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

// nsStateOf reads a namespace's state, failing the test if it is absent.
func nsStateOf(t *testing.T, srv *Raft, name string) cmd.NamespaceState {
	t.Helper()
	got, err := srv.GetNamespaces(name)
	require.NoError(t, err)
	require.Len(t, got, 1)
	return got[0].State
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

	// The two halves of ChangeNamespaceStateIfUnchanged are pinned
	// separately: that the pre-read reports the current index, and that a
	// non-matching index is refused all the way through the apply. On a
	// single node the calls are serialized, so the read always matches and no
	// behavioral test can tell a correctly-forwarded index from a hardcoded
	// unconditional 0 — that one line stays uncovered.

	// stateChangeIndex is the pre-read ChangeNamespaceStateIfUnchanged
	// conditions on. It must re-read per call: a value cached across the
	// suspend below would make every later flip propose a stale index.
	t.Run("stateChangeIndex reports the current index on every call", func(t *testing.T) {
		seed(t, "cas-read")

		atCreate, err := srv.stateChangeIndex("cas-read")
		require.NoError(t, err)
		assert.NotZero(t, atCreate, "a zero index would propose unconditionally")

		version, err := srv.ChangeNamespaceState(ctx, "cas-read", cmd.NamespaceStateSuspended)
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))

		atSuspend, err := srv.stateChangeIndex("cas-read")
		require.NoError(t, err)
		assert.Greater(t, atSuspend, atCreate, "the read must reflect the flip, not the earlier value")
	})

	t.Run("stateChangeIndex on missing returns ErrNotFound", func(t *testing.T) {
		_, err := srv.stateChangeIndex("never-existed")
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrNotFound)
	})

	// The proposed index has to survive marshalling, the apply dispatch and
	// the FSM to reach the precondition.
	t.Run("a stale expected index is refused end to end", func(t *testing.T) {
		seed(t, "cas-stale")
		atCreate, err := srv.stateChangeIndex("cas-stale")
		require.NoError(t, err)

		version, err := srv.ChangeNamespaceStateIfUnchanged(ctx, "cas-stale", cmd.NamespaceStateSuspended)
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))

		// Re-propose the same flip carrying the pre-suspend index, the shape
		// an Execute retry takes after another operator resumed.
		_, err = srv.changeNamespaceState(ctx, "cas-stale", cmd.NamespaceStateActive, atCreate)
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrStateChangedConcurrently)

		got, err := srv.GetNamespaces("cas-stale")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, cmd.NamespaceStateSuspended, got[0].State, "the refused flip must not apply")
	})

	t.Run("ChangeNamespaceStateIfUnchanged flips and is reversible", func(t *testing.T) {
		seed(t, "cas-target")

		version, err := srv.ChangeNamespaceStateIfUnchanged(ctx, "cas-target", cmd.NamespaceStateSuspended)
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.Equal(t, cmd.NamespaceStateSuspended, nsStateOf(t, srv, "cas-target"))

		version, err = srv.ChangeNamespaceStateIfUnchanged(ctx, "cas-target", cmd.NamespaceStateActive)
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		assert.Equal(t, cmd.NamespaceStateActive, nsStateOf(t, srv, "cas-target"))
	})

	t.Run("ChangeNamespaceStateIfUnchanged on missing returns ErrNotFound", func(t *testing.T) {
		before := srv.NamespaceCount()
		_, err := srv.ChangeNamespaceStateIfUnchanged(ctx, "never-existed", cmd.NamespaceStateSuspended)
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrNotFound)
		assert.Equal(t, before, srv.NamespaceCount(), "a zero-row pre-read must propose nothing")
	})
}

// TestRaftNamespaceEndpoints_HomeNode verifies that AddNamespace fills an
// omitted HomeNodes from the storage candidates and persists an explicit one
// as-is. factorRows pin the bounds the two replication factors put on the list.
func TestRaftNamespaceEndpoints_HomeNode(t *testing.T) {
	srv, ctx, cleanup := setupRaftForNamespaceTests(t)
	defer cleanup()

	t.Run("empty home_node is filled from storage candidates", func(t *testing.T) {
		created, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "autohome"})
		require.NoError(t, err)
		require.NoError(t, srv.WaitForUpdate(ctx, version))
		require.Len(t, created.HomeNodes, 1, "AddNamespace should return the home node it picked")
		assert.Contains(t, srv.StorageCandidates(), created.HomeNodes[0])

		got, err := srv.GetNamespaces("autohome")
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, created.HomeNodes, got[0].HomeNodes)
	})

	factorRows := []struct {
		name       string
		minFactor  int
		maxFactor  int
		candidates []string // storage candidates; n1, n2 and n3 when nil
		homeNodes  []string
		wantErr    []string // substrings of the ErrBadRequest text; nil when the create is stored
		wantLen    int
	}{
		{name: "min 2 max 2 auto-picks two of three", minFactor: 2, maxFactor: 2, wantLen: 2},
		{name: "min 2 refuses an explicit one-entry list", minFactor: 2, homeNodes: []string{"n1"}, wantErr: []string{"home_nodes has 1 entries"}},
		{name: "min 3 refuses an auto-pick from two candidates", minFactor: 3, maxFactor: 3, candidates: []string{"n1", "n2"}, wantErr: []string{"only 2 storage node(s)"}},
		{name: "negative maximum refuses an empty create", maxFactor: -1, wantErr: []string{"REPLICATION_MAXIMUM_FACTOR"}},
	}
	for _, tc := range factorRows {
		t.Run(tc.name, func(t *testing.T) {
			candidates := tc.candidates
			if candidates == nil {
				candidates = []string{"n1", "n2", "n3"}
			}
			m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), func(c *Config) {
				c.Replication.MinimumFactor = tc.minFactor
				c.Replication.MaximumFactor = tc.maxFactor
			})
			srv, ctx, cleanup := startSingleNodeRaft(t, m, candidates...)
			defer cleanup()

			created, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "bounded", HomeNodes: tc.homeNodes})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
				for _, text := range tc.wantErr {
					assert.ErrorContains(t, err, text)
				}
				assert.Zero(t, srv.NamespaceCount(), "a refused create must propose nothing")
				return
			}
			require.NoError(t, err)
			require.NoError(t, srv.WaitForUpdate(ctx, version))
			got, err := srv.GetNamespaces("bounded")
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, created.HomeNodes, got[0].HomeNodes)
			assert.Len(t, got[0].HomeNodes, tc.wantLen)
			assert.Subset(t, candidates, got[0].HomeNodes)
		})
	}
}

// TestRaftNamespaceEndpoints_AutoPickNoCandidates pins that an auto-pick with
// no storage candidates is refused with ErrBadRequest, which the REST handler
// maps to 422. The Raft is never opened, so StorageCandidates is empty.
func TestRaftNamespaceEndpoints_AutoPickNoCandidates(t *testing.T) {
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), func(c *Config) {
		c.Replication.MinimumFactor = 1
		c.Replication.MaximumFactor = 1
	})
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	_, _, err := srv.AddNamespace(context.Background(), cmd.Namespace{Name: "nocand"})
	require.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	require.ErrorContains(t, err, "no storage candidates")
}

// TestRaftNamespaceEndpoints_Update seeds each row's store with [n1 n2]. It
// checks that UpdateNamespace rewrites the list, and that a refused update,
// including one shorter than REPLICATION_MINIMUM_FACTOR, leaves it unchanged.
func TestRaftNamespaceEndpoints_Update(t *testing.T) {
	seeded := []string{"n1", "n2"}
	tests := []struct {
		name       string
		minFactor  int
		ns         cmd.Namespace
		wantErr    error
		wantStored []string // customer1's HomeNodes after the update
	}{
		{name: "update rewrites home_node", ns: cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-b"}}, wantStored: []string{"node-b"}},
		{name: "update missing returns ErrNotFound", ns: cmd.Namespace{Name: "never-existed", HomeNodes: []string{"node-a"}}, wantErr: usecasesNamespaces.ErrNotFound, wantStored: seeded},
		{name: "update empty home_node returns ErrBadRequest", ns: cmd.Namespace{Name: "customer1"}, wantErr: usecasesNamespaces.ErrBadRequest, wantStored: seeded},
		{name: "update below the minimum returns ErrBadRequest", minFactor: 2, ns: cmd.Namespace{Name: "customer1", HomeNodes: []string{"n1"}}, wantErr: usecasesNamespaces.ErrBadRequest, wantStored: seeded},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), func(c *Config) { c.Replication.MinimumFactor = tc.minFactor })
			srv, ctx, cleanup := startSingleNodeRaft(t, m)
			defer cleanup()
			_, version, err := srv.AddNamespace(ctx, cmd.Namespace{Name: "customer1", HomeNodes: seeded})
			require.NoError(t, err)
			require.NoError(t, srv.WaitForUpdate(ctx, version))

			v, err := srv.UpdateNamespace(ctx, tc.ns)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, srv.WaitForUpdate(ctx, v))
			}
			got, err := srv.GetNamespaces("customer1")
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantStored, got[0].HomeNodes)
		})
	}
}

// TestNextHomeNodes_RoundRobin pins the picks of successive calls on one
// candidate list, listed in call order in wantCycle. The first head is random,
// so the test finds the first pick in wantCycle and expects the rest to follow.
func TestNextHomeNodes_RoundRobin(t *testing.T) {
	tests := []struct {
		name      string
		nodes     []string
		n         int
		wantCycle [][]string
	}{
		{
			name: "two of three", nodes: []string{"A", "B", "C"}, n: 2,
			wantCycle: [][]string{{"A", "B"}, {"B", "C"}, {"C", "A"}},
		},
		{
			name: "three of three, unsorted input", nodes: []string{"B", "A", "C"}, n: 3,
			wantCycle: [][]string{{"A", "B", "C"}, {"B", "C", "A"}, {"C", "A", "B"}},
		},
		{
			name: "three of one", nodes: []string{"A"}, n: 3,
			wantCycle: [][]string{{"A"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Raft{}
			first, err := r.nextHomeNodes(tc.nodes, tc.n)
			require.NoError(t, err)
			start := slices.IndexFunc(tc.wantCycle, func(want []string) bool { return slices.Equal(want, first) })
			require.NotEqual(t, -1, start, "first pick %v is not in the expected cycle", first)

			for i := 1; i <= len(tc.wantCycle); i++ {
				got, err := r.nextHomeNodes(tc.nodes, tc.n)
				require.NoError(t, err)
				assert.Equal(t, tc.wantCycle[(start+i)%len(tc.wantCycle)], got, "call %d", i)
			}
		})
	}
}

// TestNextHomeNodes_RebuildsOnCandidateChange covers the membership-change
// case: when the candidate set differs from the one the cached iterator was
// built with, the iterator is rebuilt so newly added nodes become eligible
// and removed nodes drop out. Without the rebuild, callers would keep
// rotating through the stale set indefinitely.
func TestNextHomeNodes_RebuildsOnCandidateChange(t *testing.T) {
	r := &Raft{}
	pick := func(nodes []string) string {
		t.Helper()
		got, err := r.nextHomeNodes(nodes, 1)
		require.NoError(t, err)
		require.Len(t, got, 1)
		return got[0]
	}
	initial := []string{"A", "B"}

	// Burn through one full rotation on the initial set to seed the iterator.
	seen := map[string]bool{}
	for i := 0; i < len(initial); i++ {
		seen[pick(initial)] = true
	}
	assert.Equal(t, map[string]bool{"A": true, "B": true}, seen, "initial rotation should cover the initial set")

	// Membership change: drop B, add C and D. The iterator must rebuild;
	// otherwise it would still hand back B (and never C or D).
	updated := []string{"A", "C", "D"}
	seen = map[string]bool{}
	for i := 0; i < len(updated); i++ {
		got := pick(updated)
		assert.Contains(t, updated, got, "post-change pick must come from the updated set, never the removed node")
		seen[got] = true
	}
	assert.Equal(t, map[string]bool{"A": true, "C": true, "D": true}, seen, "post-change rotation should cover the updated set exactly once")
}

// TestNextHomeNodes_NoCandidates returns an error rather than picking nothing.
func TestNextHomeNodes_NoCandidates(t *testing.T) {
	r := &Raft{}
	_, err := r.nextHomeNodes(nil, 1)
	require.Error(t, err)
}
