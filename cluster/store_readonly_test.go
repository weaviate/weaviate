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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestReadOnlyFollowerHydratesFromCopiedRaftState verifies that a node booted
// with ReadOnlyFollower reconstructs the schema FSM from a writer's raft dir
// (snapshot + post-snapshot log replay) WITHOUT becoming a RAFT node, reports
// ready (no leader required), and rejects schema writes.
func TestReadOnlyFollowerHydratesFromCopiedRaftState(t *testing.T) {
	ctx := context.Background()

	// --- Writer: build schema state, snapshot, then a post-snapshot log entry ---
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	m.indexer.On("Open", Anything).Return(nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	require.True(t, tryNTimesWithWait(20, time.Millisecond*200, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))

	m.indexer.On("AddClass", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	cls := &models.Class{
		Class:              "C",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	ss := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"T0": {Name: "T0", Status: "S0"}}}
	_, err := srv.AddClass(ctx, cls, ss)
	require.NoError(t, err)

	// Snapshot captures C + T0/S0.
	require.NoError(t, srv.store.raft.Barrier(2*time.Second).Error())
	require.NoError(t, srv.store.raft.Snapshot().Error())

	// Post-snapshot, applied log entries (not in the snapshot): drop T0 and
	// re-add it with state S1 so the final committed state differs from the
	// snapshot. This exercises log replay on top of the restored snapshot.
	m.indexer.On("DeleteTenants", Anything, Anything).Return(nil)
	m.replicationFSM.On("DeleteReplicationsByTenants", Anything, Anything).Return(nil)
	_, err = srv.DeleteTenants(ctx, cls.Class, &api.DeleteTenantsRequest{Tenants: []string{"T0"}})
	require.NoError(t, err)

	m.indexer.On("AddTenants", Anything, Anything).Return(nil)
	_, err = srv.AddTenants(ctx, cls.Class, &api.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*api.Tenant{{Name: "T0", Status: "S1"}},
	})
	require.NoError(t, err)

	// Close the writer so the follower can open the raft bolt read-only.
	m.indexer.On("Close", Anything).Return(nil)
	require.NoError(t, srv.Close(ctx))
	m.indexer.AssertExpectations(t)

	// --- Follower: boot read-only on the SAME raft dir ---
	m.cfg.ReadOnlyFollower = true
	s := NewFSM(m.cfg, nil, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	m.indexer = fakes.NewMockSchemaExecutor()
	srv = NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	require.NoError(t, srv.Open(ctx, m.indexer))

	// It is NOT a RAFT node.
	require.Nil(t, srv.store.raft, "a read-only follower must not construct a raft node")
	// The DB is hydrated and the follower reports ready (no leader required).
	require.True(t, srv.store.dbLoaded.Load())
	require.True(t, srv.Ready(), "a read-only follower must be ready without a leader")

	// Schema reconstructed to the final state: snapshot (C, T0/S0) + replayed
	// log (T0 -> S1).
	schemaReader := srv.SchemaReader()
	require.Equal(t, cls.Class, schemaReader.ClassEqual(cls.Class))
	require.Equal(t, "S1", getTenantStatus(t, schemaReader, cls.Class, "T0"))

	// Schema writes are rejected (no raft node to apply to).
	_, execErr := srv.store.Execute(&api.ApplyRequest{
		Type:  api.ApplyRequest_TYPE_ADD_CLASS,
		Class: "D",
	})
	require.Error(t, execErr, "a read-only follower must reject schema writes")

	m.indexer.On("Close", Anything).Return(nil)
	require.NoError(t, srv.Close(ctx))
}
