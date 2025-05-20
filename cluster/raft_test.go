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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestRaftEndpoints(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("Close", Anything).Return(nil)
	m.indexer.On("AddClass", Anything).Return(nil)
	m.indexer.On("RestoreClassDir", Anything).Return(nil)
	m.indexer.On("UpdateClass", Anything).Return(nil)
	m.indexer.On("DeleteClass", Anything).Return(nil)
	m.indexer.On("AddProperty", Anything, Anything).Return(nil)
	m.indexer.On("UpdateShardStatus", Anything).Return(nil)
	m.indexer.On("AddTenants", Anything, Anything).Return(nil)
	m.indexer.On("UpdateTenants", Anything, Anything).Return(nil)
	m.indexer.On("DeleteTenants", Anything, Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddReplicaToShard", Anything, Anything, Anything).Return(nil)
	m.indexer.On("DeleteReplicaFromShard", Anything, Anything, Anything).Return(nil)

	m.parser.On("ParseClass", mock.Anything).Return(nil)
	m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	// LeaderNotFound
	_, err := srv.Execute(ctx, &command.ApplyRequest{})
	assert.ErrorIs(t, err, types.ErrLeaderNotFound)
	assert.ErrorIs(t, srv.Join(ctx, m.store.cfg.NodeID, addr, true), types.ErrLeaderNotFound)
	assert.ErrorIs(t, srv.Remove(ctx, m.store.cfg.NodeID), types.ErrLeaderNotFound)

	// Deadline exceeded while waiting for DB to be restored
	func() {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*30)
		defer cancel()
		assert.ErrorIs(t, srv.WaitUntilDBRestored(ctx, 5*time.Millisecond, make(chan struct{})), context.DeadlineExceeded)
	}()

	// Open
	defer srv.Close(ctx)
	assert.Nil(t, srv.Open(ctx, m.indexer))

	// node lose leadership after service call
	assert.ErrorIs(t, srv.store.Join(m.store.cfg.NodeID, addr, true), types.ErrNotLeader)
	assert.ErrorIs(t, srv.store.Remove(m.store.cfg.NodeID), types.ErrNotLeader)

	// Connect
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))

	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1, make(chan struct{})))
	assert.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))
	tryNTimesWithWait(20, time.Millisecond*100, srv.store.IsLeader)
	assert.True(t, srv.store.IsLeader())
	schemaReader := srv.SchemaReader()
	assert.Equal(t, schemaReader.Len(), 0)

	// AddClass
	_, err = srv.AddClass(ctx, nil, nil)
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	assert.Equal(t, schemaReader.ClassEqual("C"), "")

	cls := &models.Class{
		Class:              "C",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	ss := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"T0": {Name: "T0"}}}
	version0, err := srv.AddClass(ctx, cls, ss)
	assert.Nil(t, err)
	assert.Equal(t, schemaReader.ClassEqual("C"), "C")

	// Add same class again
	_, err = srv.AddClass(ctx, cls, ss)
	assert.Error(t, err)
	assert.Equal(t, "class name C already exists", err.Error())

	// Add similar class
	_, err = srv.AddClass(ctx, &models.Class{Class: "c"}, ss)
	assert.ErrorIs(t, err, schema.ErrClassExists)

	// QueryReadOnlyClass
	readOnlyVClass, err := srv.QueryReadOnlyClasses(cls.Class)
	assert.NoError(t, err)
	assert.NotNil(t, readOnlyVClass[cls.Class].Class)
	assert.Equal(t, cls, readOnlyVClass[cls.Class].Class)

	// QueryClassVersions
	classVersions, err := srv.QueryClassVersions(cls.Class)
	assert.NoError(t, err)
	assert.Equal(t, readOnlyVClass[cls.Class].Version, classVersions[cls.Class])

	// QuerySchema
	getSchema, err := srv.QuerySchema()
	assert.NoError(t, err)
	assert.NotNil(t, getSchema)
	assert.Equal(t, models.Schema{Classes: []*models.Class{readOnlyVClass[cls.Class].Class}}, getSchema)

	// QueryTenants all
	getTenantsAll, _, err := srv.QueryTenants(cls.Class, []string{})
	assert.NoError(t, err)
	assert.NotNil(t, getTenantsAll)
	assert.Equal(t, []*models.Tenant{{
		Name:           "T0",
		ActivityStatus: models.TenantActivityStatusHOT,
	}}, getTenantsAll)

	// QueryTenants one
	getTenantsOne, _, err := srv.QueryTenants(cls.Class, []string{"T0"})
	assert.NoError(t, err)
	assert.NotNil(t, getTenantsOne)
	assert.Equal(t, []*models.Tenant{{
		Name:           "T0",
		ActivityStatus: models.TenantActivityStatusHOT,
	}}, getTenantsOne)

	// QueryTenants one
	getTenantsNone, _, err := srv.QueryTenants(cls.Class, []string{"T"})
	assert.NoError(t, err)
	assert.NotNil(t, getTenantsNone)
	assert.Equal(t, []*models.Tenant{}, getTenantsNone)

	// Query ShardTenant
	getTenantShards, _, err := srv.QueryTenantsShards(cls.Class, "T0")
	for tenant, status := range getTenantShards {
		assert.Nil(t, err)
		assert.Equal(t, "T0", tenant)
		assert.Equal(t, models.TenantActivityStatusHOT, status)
	}

	// QueryShardOwner - Err
	_, _, err = srv.QueryShardOwner(cls.Class, "T0")
	assert.NotNil(t, err)

	// QueryShardOwner
	srv.UpdateClass(ctx, cls, &sharding.State{Physical: map[string]sharding.Physical{"T0": {BelongsToNodes: []string{"N0"}}}})
	getShardOwner, _, err := srv.QueryShardOwner(cls.Class, "T0")
	assert.Nil(t, err)
	assert.Equal(t, "N0", getShardOwner)

	// QueryShardingState
	shardingState := &sharding.State{Physical: map[string]sharding.Physical{"T0": {BelongsToNodes: []string{"N0"}}}, ReplicationFactor: 2}
	srv.UpdateClass(ctx, cls, shardingState)
	getShardingState, _, err := srv.QueryShardingState(cls.Class)
	assert.Nil(t, err)
	assert.Equal(t, shardingState, getShardingState)

	// UpdateClass
	info := schema.ClassInfo{
		Exists:            true,
		MultiTenancy:      models.MultiTenancyConfig{Enabled: true},
		ReplicationFactor: 1,
		Tenants:           1,
	}
	_, err = srv.UpdateClass(ctx, nil, nil)
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	cls.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	ss.Physical = map[string]sharding.Physical{"T0": {Name: "T0"}}
	version, err := srv.UpdateClass(ctx, cls, nil)
	info.ClassVersion = version
	info.ShardVersion = version0
	assert.Nil(t, err)
	assert.Nil(t, srv.store.WaitForAppliedIndex(ctx, time.Millisecond*10, version))
	assert.Equal(t, info, schemaReader.ClassInfo("C"))
	assert.ErrorIs(t, srv.store.WaitForAppliedIndex(ctx, time.Millisecond*10, srv.store.lastAppliedIndex.Load()+1), types.ErrDeadlineExceeded)

	// DeleteClass
	m.replicationFSM.EXPECT().DeleteReplicationsByCollection(Anything).Return(nil).Times(2)
	_, err = srv.DeleteClass(ctx, "X")
	assert.Nil(t, err)
	_, err = srv.DeleteClass(ctx, "C")
	assert.Nil(t, err)
	assert.Equal(t, schema.ClassInfo{}, schemaReader.ClassInfo("C"))

	// RestoreClass
	_, err = srv.RestoreClass(ctx, nil, nil)
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.RestoreClass(ctx, cls, ss)
	assert.Nil(t, err)
	info.ClassVersion = version
	info.ShardVersion = version
	assert.Equal(t, info, schemaReader.ClassInfo("C"))

	// AddProperty
	_, err = srv.AddProperty(ctx, "C", nil)
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	_, err = srv.AddProperty(ctx, "", &models.Property{Name: "P1"})
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.AddProperty(ctx, "C", &models.Property{Name: "P1"})
	assert.Nil(t, err)
	info.ClassVersion = version
	info.Properties = 1
	assert.Equal(t, info, schemaReader.ClassInfo("C"))

	// UpdateStatus
	_, err = srv.UpdateShardStatus(ctx, "", "A", "ACTIVE")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	_, err = srv.UpdateShardStatus(ctx, "C", "", "ACTIVE")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	_, err = srv.UpdateShardStatus(ctx, "C", "A", "ACTIVE")
	assert.Nil(t, err)

	// AddTenants
	_, err = srv.AddTenants(ctx, "", &command.AddTenantsRequest{})
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.AddTenants(ctx, "C", &command.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*command.Tenant{nil, {Name: "T2", Status: "S1"}, nil},
	})
	assert.Nil(t, err)
	info.ShardVersion = version
	info.Tenants += 1
	assert.Equal(t, info, schemaReader.ClassInfo("C"))

	// AddReplicaToShard
	_, err = srv.AddReplicaToShard(ctx, "", "", "")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.AddReplicaToShard(ctx, "C", "T2", "Node-2")
	assert.Nil(t, err)
	info.ClassVersion = version
	assert.Equal(t, info, schemaReader.ClassInfo("C"))
	assert.Equal(t, []string{"Node-1", "Node-2"}, schemaReader.CopyShardingState("C").Physical["T2"].BelongsToNodes)

	// DeleteReplicaFromShard
	_, err = srv.DeleteReplicaFromShard(ctx, "", "", "")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.DeleteReplicaFromShard(ctx, "C", "T2", "Node-2")
	assert.Nil(t, err)
	info.ClassVersion = version
	assert.Equal(t, info, schemaReader.ClassInfo("C"))
	assert.Equal(t, []string{"Node-1"}, schemaReader.CopyShardingState("C").Physical["T2"].BelongsToNodes)

	// SyncShard with active tenant
	_, err = srv.SyncShard(ctx, "", "", "")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	m.indexer.On("ShutdownShard", mock.Anything, mock.Anything).Return(nil).Times(0)
	m.indexer.On("LoadShard", "C", "A").Return(nil).Times(1)
	_, err = srv.SyncShard(ctx, "C", "A", "Node-1")
	assert.Nil(t, err)

	// SyncShard with inactive tenant
	_, err = srv.UpdateShardStatus(ctx, "C", "A", "INACTIVE")
	assert.Nil(t, err)

	_, err = srv.SyncShard(ctx, "", "", "")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	m.indexer.On("ShutdownShard", "C", "A").Return(nil).Times(1)
	m.indexer.On("LoadShard", mock.Anything, mock.Anything).Return(nil).Times(0)
	_, err = srv.SyncShard(ctx, "C", "A", "Node-1")
	assert.Nil(t, err)

	_, err = srv.UpdateShardStatus(ctx, "C", "A", "ACTIVE")
	assert.Nil(t, err)

	// SyncShard with absent tenant
	_, err = srv.SyncShard(ctx, "", "", "")
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	m.indexer.On("ShutdownShard", "C", "T0").Return(nil).Times(1)
	m.indexer.On("LoadShard", mock.Anything, mock.Anything).Return(nil).Times(0)
	_, err = srv.SyncShard(ctx, "C", "T0", "Node-1")
	assert.Nil(t, err)

	// Add single-tenant collection
	cls = &models.Class{
		Class: "D",
	}
	ss = &sharding.State{PartitioningEnabled: false, Physical: map[string]sharding.Physical{"S0": {Name: "S0"}}}
	_, err = srv.AddClass(ctx, cls, ss)
	assert.Nil(t, err)
	assert.Equal(t, schemaReader.ClassEqual("D"), "D")

	// SyncShard with ST collection and present shard
	m.indexer.On("ShutdownShard", mock.Anything, mock.Anything).Return(nil).Times(0)
	m.indexer.On("LoadShard", "D", "S0").Return(nil).Times(1)
	_, err = srv.SyncShard(ctx, "D", "S0", "Node-1")
	assert.Nil(t, err)

	// SyncShard with ST collection and absent shard
	m.indexer.On("ShutdownShard", "D", "S0").Return(nil).Times(1)
	m.indexer.On("LoadShard", mock.Anything, mock.Anything).Return(nil).Times(0)
	_, err = srv.SyncShard(ctx, "D", "S0", "Node-1")
	assert.Nil(t, err)

	// UpdateTenants
	_, err = srv.UpdateTenants(ctx, "", &command.UpdateTenantsRequest{})
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	_, err = srv.UpdateTenants(ctx, "C", &command.UpdateTenantsRequest{Tenants: []*command.Tenant{{Name: "T2", Status: "S2"}}})
	assert.Nil(t, err)

	// DeleteTenants
	m.replicationFSM.EXPECT().DeleteReplicationsByTenants(Anything, Anything).Return(nil)
	_, err = srv.DeleteTenants(ctx, "", &command.DeleteTenantsRequest{})
	assert.ErrorIs(t, err, schema.ErrBadRequest)
	version, err = srv.DeleteTenants(ctx, "C", &command.DeleteTenantsRequest{Tenants: []string{"T0", "Tn"}})
	assert.Nil(t, err)
	info.Tenants -= 1
	info.ShardVersion = version
	assert.Equal(t, info, schemaReader.ClassInfo("C"))
	assert.Equal(t, "S2", schemaReader.CopyShardingState("C").Physical["T2"].Status)

	// Self Join
	assert.Nil(t, srv.Join(ctx, m.store.cfg.NodeID, addr, true))
	assert.True(t, srv.store.IsLeader())
	assert.Nil(t, srv.Join(ctx, m.store.cfg.NodeID, addr, false))
	assert.True(t, srv.store.IsLeader())
	assert.ErrorContains(t, srv.Remove(ctx, m.store.cfg.NodeID), "configuration")
	assert.True(t, srv.store.IsLeader())

	// Stats
	stats := srv.Stats()
	// stats:raft_state
	assert.Equal(t, "Leader", stats["raft"].(map[string]string)["state"])
	// stats:leader_address
	leaderAddress := string(stats["leader_address"].(raft.ServerAddress))
	splitAddress := strings.Split(leaderAddress, ":")
	assert.Len(t, splitAddress, 2)
	ipAddress, portStr := splitAddress[0], splitAddress[1]
	assert.Equal(t, "127.0.0.1", ipAddress)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Errorf("Port should have been parsable as an int but was: %v", portStr)
	}
	assert.GreaterOrEqual(t, port, 0)
	// stats:leader_id
	leaderID := string(stats["leader_id"].(raft.ServerID))
	assert.Equal(t, m.store.cfg.NodeID, leaderID)

	// create snapshot
	assert.Nil(t, srv.store.raft.Barrier(2*time.Second).Error())
	assert.Nil(t, srv.store.raft.Snapshot().Error())

	// restore from snapshot
	assert.Nil(t, srv.Close(ctx))

	s := NewFSM(m.cfg, nil, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	srv = NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	assert.Nil(t, srv.Open(ctx, m.indexer))
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1, make(chan struct{})))
	assert.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))
	tryNTimesWithWait(20, time.Millisecond*100, srv.store.IsLeader)
	schemaReader = srv.SchemaReader()
	assert.Equal(t, info, schemaReader.ClassInfo("C"))
}

func TestRaftStoreInit(t *testing.T) {
	var (
		ctx   = context.Background()
		m     = NewMockStore(t, "Node-1", 9093)
		store = m.store
		addr  = fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	)

	// NotOpen
	assert.ErrorIs(t, store.Join(m.store.cfg.NodeID, addr, true), types.ErrNotOpen)
	assert.ErrorIs(t, store.Remove(m.store.cfg.NodeID), types.ErrNotOpen)
	assert.ErrorIs(t, store.Notify(m.store.cfg.NodeID, addr), types.ErrNotOpen)

	// Already Open
	store.open.Store(true)
	assert.Nil(t, store.Open(ctx))

	// notify non voter
	store.cfg.BootstrapExpect = 0
	assert.Nil(t, store.Notify("A", "localhost:123"))

	// not enough voter
	store.cfg.BootstrapExpect = 2
	assert.Nil(t, store.Notify("A", "localhost:123"))
}

func TestRaftClose(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	s := NewFSM(m.cfg, nil, nil, prometheus.NewPedanticRegistry())
	m.store = &s
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	m.indexer.On("Open", mock.Anything).Return(nil)
	assert.Nil(t, srv.Open(ctx, m.indexer))
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	close := make(chan struct{})
	go func() {
		time.Sleep(time.Second)
		close <- struct{}{}
	}()
	now := time.Now()
	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*10, close))
	after := time.Now()
	assert.Less(t, after.Sub(now), 2*time.Second)
}

func TestRaftPanics(t *testing.T) {
	m := NewMockStore(t, "Node-1", 9091)

	// Assert Correct Response Type
	ret := m.store.Apply(&raft.Log{Type: raft.LogNoop})
	resp, ok := ret.(Response)
	assert.True(t, ok)
	assert.Equal(t, resp, Response{})

	// Not a Valid Payload
	assert.Panics(t, func() { m.store.Apply(&raft.Log{Data: []byte("a")}) })

	// Cannot Open File Store
	m.indexer.On("Open", mock.Anything).Return(errAny)
	assert.Panics(t, func() { m.store.openDatabase(context.TODO()) })
}
