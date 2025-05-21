//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestSnapshotRestoreSchemaOnly ensures that when restoring a snapshot we correctly restore the state of the schema
// without impacting the underlying database if it has integrated changes already
func TestSnapshotRestoreSchemaOnly(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	// Open
	m.indexer.On("Open", Anything).Return(nil)
	assert.Nil(t, srv.Open(ctx, m.indexer))

	// Ensure Raft starts and a leader is elected
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1, make(chan struct{})))
	assert.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))
	tryNTimesWithWait(20, time.Millisecond*100, srv.store.IsLeader)
	assert.True(t, srv.store.IsLeader())

	// DeleteClass
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("DeleteClass", Anything).Return(nil)
	m.replicationFSM.On("DeleteReplicationsByCollection", Anything).Return(nil)
	_, err := srv.DeleteClass(ctx, "C")
	assert.Nil(t, err)

	// Add a class C with a tenant T0 with state S0
	m.indexer.On("AddClass", Anything).Return(nil)
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	cls := &models.Class{
		Class:              "C",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	// Get a shema reader to verify our schema operation are working
	schemaReader := srv.SchemaReader()
	ss := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"T0": {Name: "T0", Status: "S0"}}}
	_, err = srv.AddClass(ctx, cls, ss)
	assert.Nil(t, err)
	assert.Equal(t, schemaReader.ClassEqual(cls.Class), cls.Class)
	assert.Equal(t, "S0", schemaReader.CopyShardingState(cls.Class).Physical["T0"].Status)

	// Create a snapshot here with the class and the tenant existing
	assert.Nil(t, srv.store.raft.Barrier(2*time.Second).Error())
	assert.Nil(t, srv.store.raft.Snapshot().Error())

	m.indexer.On("DeleteTenants", Anything, Anything).Return(nil)
	m.replicationFSM.On("DeleteReplicationsByTenants", Anything, Anything).Return(nil)
	// Now let's drop the tenant T0 (this will be a log entry and not included in the snapshot)
	_, err = srv.DeleteTenants(ctx, cls.Class, &api.DeleteTenantsRequest{Tenants: []string{"T0"}})
	require.NoError(t, err)

	// Now re-add the tenant T0 with state S1
	m.indexer.On("AddTenants", Anything, Anything).Return(nil)
	_, err = srv.AddTenants(ctx, cls.Class, &api.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*api.Tenant{{Name: "T0", Status: "S1"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "S1", schemaReader.CopyShardingState(cls.Class).Physical["T0"].Status)

	// close service
	m.indexer.On("Close", Anything).Return(nil)
	assert.Nil(t, srv.Close(ctx))
	m.indexer.AssertExpectations(t)

	// Create a new FSM that will restore from it's state from the disk (using snapshot and logs)
	s := NewFSM(m.cfg, nil, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	// We refresh the mock schema to ensure that we can assert no calls except Open are sent to the database
	m.indexer = fakes.NewMockSchemaExecutor()
	// NewRaft will try to restore from any snapshot it can find on disk
	srv = NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	// Ensure raft starts and a leader is elected
	m.indexer.On("Open", Anything).Return(nil)
	// shall be called because of restoring from snapshot
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return().Once()
	assert.Nil(t, srv.Open(ctx, m.indexer))
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1, make(chan struct{})))
	assert.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))
	tryNTimesWithWait(20, time.Millisecond*100, srv.store.IsLeader)

	// Ensure that the class has been restored and that the tenant is present with the right state
	schemaReader = srv.SchemaReader()
	assert.Equal(t, cls.Class, schemaReader.ClassEqual(cls.Class))
	assert.Equal(t, "S1", schemaReader.CopyShardingState(cls.Class).Physical["T0"].Status)

	// Ensure there was no supplementary call to the underlying DB as we were just recovering the schema
	m.indexer.AssertExpectations(t)
}
