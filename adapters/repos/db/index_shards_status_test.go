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

package db

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const shardStatusLocalNode = "coordinator"

// statusReply is one node's answer to a shard status or queue size request.
type statusReply struct {
	status string
	size   int64
	err    error
}

// shardStatusRemoteClient answers by "node/shard", then "node" (hostname == node name).
type shardStatusRemoteClient struct {
	FakeRemoteClient
	replies map[string]statusReply

	mu    sync.Mutex
	asked []string
}

func (c *shardStatusRemoteClient) reply(ctx context.Context, host, shard string) (statusReply, error) {
	c.mu.Lock()
	c.asked = append(c.asked, host+"/"+shard)
	c.mu.Unlock()

	r, ok := c.replies[host+"/"+shard]
	if !ok {
		r, ok = c.replies[host]
	}
	if !ok {
		return r, errors.New("no reply configured")
	}
	if err := ctx.Err(); err != nil {
		return r, err
	}
	return r, r.err
}

func (c *shardStatusRemoteClient) GetShardStatus(ctx context.Context, host, _, shard string) (string, error) {
	r, err := c.reply(ctx, host, shard)
	return r.status, err
}

func (c *shardStatusRemoteClient) GetShardQueueSize(ctx context.Context, host, _, shard string) (int64, error) {
	r, err := c.reply(ctx, host, shard)
	return r.size, err
}

func (c *shardStatusRemoteClient) askedFor() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.asked)
}

// physicalShards reports each shard's first replica as its owner.
type physicalShards map[string]sharding.Physical

func (p physicalShards) ShardOwner(_, shard string) (string, error) {
	return p[shard].BelongsToNodes[0], nil
}

func (p physicalShards) ShardReplicas(_, shard string) ([]string, error) {
	return p[shard].BelongsToNodes, nil
}

type localShardKind int

const (
	localReady localShardKind = iota
	localShuttingDown
	localLocked         // an unload or freeze holds its create lock
	localLoading        // a warm-up or activation loads the lazy shard under its create lock
	localLoadingForRead // a request loads the lazy shard
)

// newShardStatusIndex lists deleted shards without replicas and fails any tenant status lookup, which could activate a tenant.
func newShardStatusIndex(t *testing.T, mt bool, physical map[string]sharding.Physical,
	local map[string]localShardKind, reachable []string, client *shardStatusRemoteClient, deleted ...string,
) (*Index, *test.Hook) {
	t.Helper()
	const className = "Abc"

	state := &sharding.State{Physical: physical, PartitioningEnabled: mt}
	state.SetLocalName(shardStatusLocalNode)
	names := make([]string, 0, len(physical))
	for name := range physical {
		names = append(names, name)
	}
	slices.Sort(names)

	schemaReader := schemaUC.NewMockSchemaReader(t)
	schemaReader.EXPECT().Shards(className).Return(append(names, deleted...), nil).Maybe()
	schemaReader.EXPECT().ShardReplicas(className, mock.Anything).RunAndReturn(
		func(_, shard string) ([]string, error) {
			p, ok := physical[shard]
			if !ok {
				return nil, clusterSchema.ErrShardNotFound
			}
			return p.BelongsToNodes, nil
		}).Maybe()
	schemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(&models.Class{Class: className}, state)
		}).Maybe()
	fsmReader := replicationTypes.NewMockReplicationFSMReader(t)
	fsmReader.EXPECT().FilterOneShardReplicasRead(className, mock.Anything, mock.Anything).RunAndReturn(
		func(_, _ string, replicas []string) []string { return replicas }).Maybe()
	schemaGetter := schemaUC.NewMockSchemaGetter(t)
	schemaGetter.EXPECT().NodeName().Return(shardStatusLocalNode).Maybe()

	nodeSelector := clusterMocks.NewMockNodeSelector(append([]string{shardStatusLocalNode}, reachable...)...)
	indexRouter := router.NewBuilder(className, mt, nodeSelector, schemaGetter, schemaReader, fsmReader).Build()

	logger, hook := test.NewNullLogger()
	replicaMetrics, err := replica.NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err, "create replica metrics")

	index := &Index{
		Config: IndexConfig{
			ClassName:            schema.ClassName(className),
			ReplicationFactor:    3,
			AutoTenantActivation: mt,
		},
		partitioningEnabled: mt,
		router:              indexRouter,
		schemaReader:        schemaReader,
		getSchema:           schemaGetter,
		remote:              sharding.NewRemoteIndex(className, physicalShards(physical), nodeSelector, client),
		replicator: &replica.Replicator{
			Finder: replica.NewFinder(className, indexRouter, cluster.NewMockNodeResolver(t), shardStatusLocalNode,
				nil, replicaMetrics, logger, func() string { return "Delete" }),
		},
		shardCreateLocks: esync.NewKeyRWLocker(),
		logger:           logger,
	}
	var fsm replicationTypes.ReplicationFSMReader = fsmReader
	index.replicationFSMReader.Store(&fsm)
	for name, kind := range local {
		switch kind {
		case localShuttingDown:
			shard := NewMockShardLike(t)
			shard.EXPECT().preventShutdown().Return(func() {}, errAlreadyShutdown).Maybe()
			index.shards.Store(name, shard)
		case localLocked:
			index.shards.Store(name, NewMockShardLike(t))
			index.shardCreateLocks.Lock(name)
			t.Cleanup(func() { index.shardCreateLocks.Unlock(name) })
		case localLoading, localLoadingForRead:
			lazy := &LazyLoadShard{}
			lazy.mutex.Lock()
			t.Cleanup(lazy.mutex.Unlock)
			index.shards.Store(name, lazy)
			if kind == localLoading {
				index.shardCreateLocks.Lock(name)
				t.Cleanup(func() { index.shardCreateLocks.Unlock(name) })
			}
		default:
			shard := NewMockShardLike(t)
			shard.EXPECT().preventShutdown().Return(func() {}, nil).Maybe()
			shard.EXPECT().GetStatus().Return(storagestate.StatusReady).Maybe()
			shard.EXPECT().ForEachVectorQueue(mock.Anything).Return(nil).Maybe()
			shard.EXPECT().ForEachGeoQueue(mock.Anything).Return(nil).Maybe()
			index.shards.Store(name, shard)
		}
	}
	return index, hook
}

func TestIndex_ShardsStatus_ReplicaFailures(t *testing.T) {
	const (
		ready       = "READY"
		indexing    = "INDEXING"
		readonly    = "READONLY"
		unavailable = "UNAVAILABLE"
	)
	require.Equal(t, unavailable, storagestate.StatusUnavailable.String())

	shardNotFound := statusReply{err: errors.New("status code: 500 body: (local shard not found)")}
	refused := statusReply{err: errors.New("connect: connection refused")}

	for _, tc := range []struct {
		name       string
		mt         bool
		physical   map[string]sharding.Physical
		deleted    []string // listed by the schema, then gone
		local      map[string]localShardKind
		unreadable []string // replicas the replication FSM excludes from reads
		reachable  []string // nodes besides the coordinator that resolve to a host
		replies    map[string]statusReply
		wantStatus map[string]string // the first replica that answers, local first
		notAsked   []string          // "node/shard"
	}{
		{
			name:       "unresolvable replica falls over to the next one",
			physical:   map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"down", "node2"}}},
			reachable:  []string{"node2"},
			replies:    map[string]statusReply{"node2": {status: indexing, size: 7}},
			wantStatus: map[string]string{"s1": indexing},
		},
		{
			name:       "failing replica falls over to the next one",
			physical:   map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node1", "node2"}}},
			reachable:  []string{"node1", "node2"},
			replies:    map[string]statusReply{"node1": refused, "node2": {status: indexing, size: 7}},
			wantStatus: map[string]string{"s1": indexing},
		},
		{
			name: "one unreachable node does not fail the other shards",
			physical: map[string]sharding.Physical{
				"s1": {Name: "s1", BelongsToNodes: []string{"down", "node2"}},
				"s2": {Name: "s2", BelongsToNodes: []string{"node2", "down"}},
				"s3": {Name: "s3", BelongsToNodes: []string{"down"}},
			},
			reachable:  []string{"node2"},
			replies:    map[string]statusReply{"node2": {status: ready, size: 0}},
			wantStatus: map[string]string{"s1": ready, "s2": ready, "s3": unavailable},
		},
		{
			name:       "a tenant copy on this node that does not serve reads falls over to a peer",
			mt:         true,
			physical:   map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{shardStatusLocalNode, "node1"}, Status: models.TenantActivityStatusHOT}},
			local:      map[string]localShardKind{"hot": localReady},
			unreadable: []string{shardStatusLocalNode},
			reachable:  []string{"node1"},
			replies:    map[string]statusReply{"node1": {status: ready, size: 7}},
			wantStatus: map[string]string{"hot": ready},
		},
		{
			name:       "a single-tenant copy on this node that does not serve reads reports its storage status",
			physical:   map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node1"}}},
			local:      map[string]localShardKind{"s1": localReady},
			unreadable: []string{shardStatusLocalNode},
			reachable:  []string{"node1"},
			replies:    map[string]statusReply{"node1": {status: ready, size: 7}},
			wantStatus: map[string]string{"s1": ready},
		},
		{
			name:       "local replica answers without asking a peer listed first",
			physical:   map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2", shardStatusLocalNode}}},
			local:      map[string]localShardKind{"s1": localReady},
			reachable:  []string{"node2"},
			replies:    map[string]statusReply{"node2": {status: indexing, size: 7}},
			wantStatus: map[string]string{"s1": ready},
			notAsked:   []string{"node2/s1"},
		},
		{
			name:      "local replica missing its shard asks a peer, not itself",
			physical:  map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2", shardStatusLocalNode}}},
			reachable: []string{"node2"},
			replies: map[string]statusReply{
				shardStatusLocalNode: shardNotFound,
				"node2":              {status: indexing, size: 7},
			},
			wantStatus: map[string]string{"s1": indexing},
			notAsked:   []string{shardStatusLocalNode + "/s1"},
		},
		{
			name:       "local replica shutting down falls over to a peer",
			physical:   map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node2"}}},
			local:      map[string]localShardKind{"s1": localShuttingDown},
			reachable:  []string{"node2"},
			replies:    map[string]statusReply{"node2": {status: ready, size: 0}},
			wantStatus: map[string]string{"s1": ready},
		},
		{
			name: "non-HOT tenants report what their replicas hold, a queue size of 0, and are never activated",
			mt:   true,
			physical: map[string]sharding.Physical{
				"hot":    {Name: "hot", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusHOT},
				"cold":   {Name: "cold", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusCOLD},
				"frozen": {Name: "frozen", BelongsToNodes: []string{"node1", "node2"}, Status: models.TenantActivityStatusFROZEN},
			},
			reachable: []string{"node1", "node2"},
			replies: map[string]statusReply{
				"node1/hot":  {status: ready, size: 0},
				"node2/hot":  {status: ready, size: 0},
				"node1/cold": {status: readonly, size: 5},
				"node2/cold": {status: readonly, size: 5},
				"node1":      shardNotFound,
				"node2":      shardNotFound,
			},
			wantStatus: map[string]string{"hot": ready, "cold": readonly, "frozen": unavailable},
		},
		{
			name:       "tenant deleted while listing is left out",
			mt:         true,
			physical:   map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT}},
			deleted:    []string{"gone"},
			reachable:  []string{"node1"},
			replies:    map[string]statusReply{"node1": {status: ready, size: 0}},
			wantStatus: map[string]string{"hot": ready},
		},
		{
			name:       "listing a multi-tenant collection reads a local HOT tenant without a tenant status lookup",
			mt:         true,
			physical:   map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{shardStatusLocalNode, "node1"}, Status: models.TenantActivityStatusHOT}},
			local:      map[string]localShardKind{"hot": localReady},
			reachable:  []string{"node1"},
			replies:    map[string]statusReply{"node1": {status: ready, size: 0}},
			wantStatus: map[string]string{"hot": ready},
			notAsked:   []string{"node1/hot"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			client := &shardStatusRemoteClient{replies: tc.replies}
			index, hook := newShardStatusIndex(t, tc.mt, tc.physical, tc.local, tc.reachable, client, tc.deleted...)
			if tc.unreadable != nil {
				fsmReader := replicationTypes.NewMockReplicationFSMReader(t)
				fsmReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(_, _ string, replicas []string) []string {
						return slices.DeleteFunc(slices.Clone(replicas), func(n string) bool { return slices.Contains(tc.unreadable, n) })
					}).Maybe()
				var fsm replicationTypes.ReplicationFSMReader = fsmReader
				index.replicationFSMReader.Store(&fsm)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			// Act
			status, err := index.getShardsStatus(ctx, "")

			// Assert
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, status)
			for _, call := range tc.notAsked {
				require.NotContains(t, client.askedFor(), call, "replica asked")
			}
			for _, entry := range hook.AllEntries() {
				require.Greater(t, entry.Level, logrus.ErrorLevel, "logged at error or above: %s", entry.Message)
			}
		})
	}
}
