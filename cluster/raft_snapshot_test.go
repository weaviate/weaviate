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
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	// Open
	m.indexer.On("Open", Anything).Return(nil)
	assert.Nil(t, srv.Open(ctx, m.indexer))

	// Ensure Raft starts and a leader is elected
	electRaftLeader(t, srv, &m)

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
	assert.Equal(t, "S0", getTenantStatus(t, schemaReader, cls.Class, "T0"))

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
	assert.Equal(t, "S1", getTenantStatus(t, schemaReader, cls.Class, "T0"))

	// close service
	m.indexer.On("Close", Anything).Return(nil)
	assert.Nil(t, srv.Close(ctx))
	m.indexer.AssertExpectations(t)

	// Create a new FSM that will restore from it's state from the disk (using snapshot and logs)
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
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
	electRaftLeader(t, srv, &m)

	// Ensure that the class has been restored and that the tenant is present with the right state
	schemaReader = srv.SchemaReader()
	assert.Equal(t, cls.Class, schemaReader.ClassEqual(cls.Class))
	assert.Equal(t, "S1", getTenantStatus(t, schemaReader, cls.Class, "T0"))

	// Ensure there was no supplementary call to the underlying DB as we were just recovering the schema
	m.indexer.AssertExpectations(t)
}

// TestSnapshotRestoreReloadsDBBeforeWaitToRestoreDB pins the startup ordering
// that decides where progress logging can live. On a node restarting with a
// snapshot, the DB reload runs inside raft.NewRaft, inside Store.Open —
// before the caller can invoke WaitToRestoreDB. So WaitToRestoreDB returns
// having logged nothing, and only the tracker inside reloadDBFromSchema can
// report that load.
func TestSnapshotRestoreReloadsDBBeforeWaitToRestoreDB(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	// Seed a node: one class, then snapshot as the last raft entry so the
	// reopen restores through the snapshot path.
	m.indexer.On("Open", Anything).Return(nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	electRaftLeader(t, srv, &m)

	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddClass", Anything).Return(nil)
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"T0": {Name: "T0", Status: "S0"}}}
	_, err := srv.AddClass(ctx, cls, ss)
	require.NoError(t, err)

	require.NoError(t, srv.store.raft.Barrier(2*time.Second).Error())
	require.NoError(t, srv.store.raft.Snapshot().Error())

	m.indexer.On("Close", Anything).Return(nil)
	require.NoError(t, srv.Close(ctx))

	// Reopen from disk. TriggerSchemaUpdateCallbacks runs inside the reload,
	// so it records whether Open had already returned at that moment.
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	m.indexer = fakes.NewMockSchemaExecutor()
	srv = NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	logHook := logrustest.NewLocal(m.logger)
	countMsg := func(msg string) int {
		n := 0
		for _, e := range logHook.AllEntries() {
			if e.Message == msg {
				n++
			}
		}
		return n
	}

	var openReturned, reloadRanDuringOpen atomic.Bool
	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Run(func(mock.Arguments) {
		reloadRanDuringOpen.Store(!openReturned.Load())
	}).Return()

	require.NoError(t, srv.Open(ctx, m.indexer))
	openReturned.Store(true)

	m.indexer.AssertCalled(t, "TriggerSchemaUpdateCallbacks")
	assert.True(t, reloadRanDuringOpen.Load(),
		"the snapshot-path reload must run inside Open, via raft.NewRaft restoring the FSM")
	assert.True(t, m.store.dbLoaded.Load(),
		"the DB must be fully loaded by the time Open returns")
	assert.Equal(t, 1, countMsg("local DB loaded from schema"),
		"the tracker inside reloadDBFromSchema is what reported this load")

	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	assert.Equal(t, 0, countMsg("waiting for database to be restored"),
		"WaitToRestoreDB never logs on this path: the load finished before it was called")

	m.indexer.On("Close", Anything).Return(nil)
	require.NoError(t, srv.Close(ctx))
}

func getTenantStatus(t *testing.T, schemaReader interface{}, className, tenantName string) string {
	type schemaReaderWithRead interface {
		Read(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error
	}

	reader, ok := schemaReader.(schemaReaderWithRead)
	if !ok {
		t.Fatalf("schemaReader does not have Read method")
	}

	var tenantStatus string

	err := reader.Read(className, true, func(_ *models.Class, state *sharding.State) error {
		if state == nil {
			return fmt.Errorf("no sharding state found for class %s", className)
		}

		physical, exists := state.Physical[tenantName]
		if !exists {
			return fmt.Errorf("tenant %s	 not found in class %s", tenantName, className)
		}

		tenantStatus = physical.Status
		return nil
	})

	require.NoError(t, err)
	return tenantStatus
}
