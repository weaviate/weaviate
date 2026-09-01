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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestReplicaCandidates(t *testing.T) {
	m := NewMockStore(t, "node-1", utils.MustGetFreeTCPPort())
	require.NoError(t, m.cfg.NamespacesController.Create(
		cmd.Namespace{Name: "tenant-a", HomeNodes: []string{"node-3"}}, 1))
	require.NoError(t, m.cfg.NamespacesController.Create(
		cmd.Namespace{Name: "home-gone", HomeNodes: []string{"node-9"}}, 2))

	r := NewRaft(mocks.NewMockNodeSelector("node-1", "node-2", "node-3"), m.store, nil)

	tests := []struct {
		name      string
		className string
		want      []string
		wantErr   string
	}{
		{
			name:      "unnamespaced class may use every storage candidate",
			className: "Foo",
			want:      []string{"node-1", "node-2", "node-3"},
		},
		{
			name:      "namespaced class is pinned to its home node",
			className: "tenant-a:Foo",
			want:      []string{"node-3"},
		},
		{
			name:      "unknown namespace leaves no candidate",
			className: "ghost:Foo",
			wantErr:   `namespace "ghost" not found`,
		},
		{
			name:      "home node outside the cluster leaves no candidate",
			className: "home-gone:Foo",
			wantErr:   `home_node "node-9" is not a storage candidate`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.replicaCandidates(tt.className)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Empty(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRemoveRefusesNamespaceHomeNode(t *testing.T) {
	ctx := context.Background()

	m := NewMockStore(t, "node-1", utils.MustGetFreeTCPPort())
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil).Maybe()
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return().Maybe()

	ns := m.cfg.NamespacesController
	require.NoError(t, ns.Create(cmd.Namespace{Name: "tenant-a", HomeNodes: []string{"node-3"}}, 1))
	require.NoError(t, ns.Create(cmd.Namespace{Name: "zeta", HomeNodes: []string{"node-4"}}, 2))
	require.NoError(t, ns.Create(cmd.Namespace{Name: "alpha", HomeNodes: []string{"node-4"}}, 3))
	require.NoError(t, ns.Create(cmd.Namespace{Name: "paused", HomeNodes: []string{"node-5"}}, 4))
	require.NoError(t, ns.Create(cmd.Namespace{Name: "going", HomeNodes: []string{"node-6"}}, 5))
	require.NoError(t, ns.ChangeState("paused", cmd.NamespaceStateSuspended,
		usecasesNamespaces.StateChange{AppliedIndex: 6}))
	require.NoError(t, ns.ChangeState("going", cmd.NamespaceStateDeleting,
		usecasesNamespaces.StateChange{AppliedIndex: 7}))

	r := NewRaft(mocks.NewMockNodeSelector("node-1", "node-2", "node-3", "node-4"), m.store, nil)
	require.NoError(t, r.Open(ctx, m.indexer))
	defer r.Close(ctx)
	require.NoError(t, r.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.True(t, tryNTimesWithWait(40, 200*time.Millisecond, r.store.IsLeader))

	tests := []struct {
		name        string
		removedNode string
		wantErr     string
	}{
		{
			name:        "refuses the home node of a single namespace",
			removedNode: "node-3",
			wantErr:     `cannot remove node "node-3": it is the home_node of namespace(s) tenant-a`,
		},
		{
			name:        "names every namespace pinned to the node, sorted",
			removedNode: "node-4",
			wantErr:     `cannot remove node "node-4": it is the home_node of namespace(s) alpha, zeta`,
		},
		{
			name:        "refuses the home node of a suspended namespace",
			removedNode: "node-5",
			wantErr:     `cannot remove node "node-5": it is the home_node of namespace(s) paused`,
		},
		{
			name:        "a namespace being deleted does not pin its home node",
			removedNode: "node-6",
		},
		{
			name:        "no namespace pins the node",
			removedNode: "node-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.Remove(ctx, tt.removedNode)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRebalanceReplicasAfterNodeRemoval(t *testing.T) {
	ctx := context.Background()

	m := NewMockStore(t, "node-1", utils.MustGetFreeTCPPort())
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil).Maybe()
	m.indexer.On("AddClass", mock.Anything).Return(nil)
	m.indexer.On("DeleteClass", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddReplicaToShard", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	m.indexer.On("DeleteReplicaFromShard", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	m.indexer.On("ReconcileAsyncReplicationForShard", mock.Anything, mock.Anything).Return(nil).Maybe()
	m.replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()
	m.replicationFSM.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	m.replicationFSM.EXPECT().DeleteReplicationsByCollection(mock.Anything).Return(nil).Maybe()

	require.NoError(t, m.cfg.NamespacesController.Create(
		cmd.Namespace{Name: "tenant-a", HomeNodes: []string{"node-4"}}, 1))
	require.NoError(t, m.cfg.NamespacesController.Create(
		cmd.Namespace{Name: "tenant-b", HomeNodes: []string{"node-1"}}, 2))
	require.NoError(t, m.cfg.NamespacesController.Create(
		cmd.Namespace{Name: "home-gone", HomeNodes: []string{"node-9"}}, 3))

	r := NewRaft(mocks.NewMockNodeSelector("node-1", "node-2", "node-3", "node-4"), m.store, nil)
	require.NoError(t, r.Open(ctx, m.indexer))
	defer r.Close(ctx)
	require.NoError(t, r.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.True(t, tryNTimesWithWait(40, 200*time.Millisecond, r.store.IsLeader))

	tests := []struct {
		name        string
		className   string
		factor      int64
		shards      map[string][]string
		removedNode string
		want        map[string][]string
	}{
		{
			// Several shards: a candidate picked without regard for the home
			// node would have to hit it for all of them to satisfy this.
			name:      "every shard of a namespaced class lands on its home node",
			className: "tenant-a:Pinned",
			factor:    1,
			shards: map[string][]string{
				"S1": {"node-1"}, "S2": {"node-1"}, "S3": {"node-1"},
				"S4": {"node-1"}, "S5": {"node-1"}, "S6": {"node-1"},
			},
			removedNode: "node-1",
			want: map[string][]string{
				"S1": {"node-4"}, "S2": {"node-4"}, "S3": {"node-4"},
				"S4": {"node-4"}, "S5": {"node-4"}, "S6": {"node-4"},
			},
		},
		{
			name:        "shard stays put when its own home node is the one leaving",
			className:   "tenant-b:Home",
			factor:      1,
			shards:      map[string][]string{"S1": {"node-1"}},
			removedNode: "node-1",
			want:        map[string][]string{"S1": {"node-1"}},
		},
		{
			name:        "shard stays put when its home node already left the cluster",
			className:   "home-gone:Orphan",
			factor:      1,
			shards:      map[string][]string{"S1": {"node-1"}},
			removedNode: "node-1",
			want:        map[string][]string{"S1": {"node-1"}},
		},
		{
			name:        "unnamespaced class replaces from the whole cluster",
			className:   "Plain",
			factor:      3,
			shards:      map[string][]string{"S1": {"node-1", "node-2", "node-3"}},
			removedNode: "node-2",
			want:        map[string][]string{"S1": {"node-1", "node-3", "node-4"}},
		},
		{
			name:        "shard without the removed node is untouched",
			className:   "Bystander",
			factor:      1,
			shards:      map[string][]string{"S1": {"node-2"}},
			removedNode: "node-1",
			want:        map[string][]string{"S1": {"node-2"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			physical := make(map[string]sharding.Physical, len(tt.shards))
			virtual := make([]sharding.Virtual, 0, len(tt.shards))
			for shard, nodes := range tt.shards {
				physical[shard] = sharding.Physical{Name: shard, BelongsToNodes: nodes}
				virtual = append(virtual, sharding.Virtual{Name: shard + "_v", AssignedToPhysical: shard})
			}
			_, err := r.AddClass(ctx,
				&models.Class{
					Class:             tt.className,
					ReplicationConfig: &models.ReplicationConfig{Factor: tt.factor},
				},
				&sharding.State{ReplicationFactor: tt.factor, Physical: physical, Virtual: virtual},
			)
			require.NoError(t, err)
			defer func() {
				_, err := r.DeleteClass(ctx, tt.className)
				require.NoError(t, err)
			}()

			require.NoError(t, r.rebalanceReplicasAfterNodeRemoval(ctx, tt.removedNode))

			for shard, want := range tt.want {
				got, err := r.SchemaReader().ShardReplicas(tt.className, shard)
				require.NoError(t, err)
				require.ElementsMatch(t, want, got)
			}
		})
	}
}
