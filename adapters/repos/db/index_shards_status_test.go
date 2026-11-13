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
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespaces"
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
	localRecovering
	localShuttingDown
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
		case localRecovering:
			rec := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
			rec.blockLoad(enterrors.ErrShardRecovering)
			index.shards.Store(name, rec)
		case localShuttingDown:
			shard := NewMockShardLike(t)
			shard.EXPECT().preventShutdown().Return(func() {}, errAlreadyShutdown).Maybe()
			index.shards.Store(name, shard)
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

func TestIndex_ShardsStatusAndQueueSize_ReplicaFailures(t *testing.T) {
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
		name          string
		mt            bool
		physical      map[string]sharding.Physical
		deleted       []string // listed by the schema, then gone
		local         map[string]localShardKind
		unreadable    []string // replicas the replication FSM excludes from reads
		reachable     []string // nodes besides the coordinator that resolve to a host
		replies       map[string]statusReply
		wantPerNode   map[string]map[string]string
		wantStatus    map[string]string // legacy: the local replica, else the first listed
		wantSize      map[string]int64
		notAsked      []string // "node/shard", by either lookup
		queueNotAsked []string // "node/shard", by the queue lookup
	}{
		{
			name:        "unresolvable replica falls over to the next one",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"down", "node2"}}},
			reachable:   []string{"node2"},
			replies:     map[string]statusReply{"node2": {status: indexing, size: 7}},
			wantPerNode: map[string]map[string]string{"s1": {"down": unavailable, "node2": indexing}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 7},
		},
		{
			name:        "failing replica falls over to the next one",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node1", "node2"}}},
			reachable:   []string{"node1", "node2"},
			replies:     map[string]statusReply{"node1": refused, "node2": {status: indexing, size: 7}},
			wantPerNode: map[string]map[string]string{"s1": {"node1": unavailable, "node2": indexing}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 7},
		},
		{
			name: "one unreachable node does not fail the other shards",
			physical: map[string]sharding.Physical{
				"s1": {Name: "s1", BelongsToNodes: []string{"down", "node2"}},
				"s2": {Name: "s2", BelongsToNodes: []string{"node2", "down"}},
				"s3": {Name: "s3", BelongsToNodes: []string{"down"}},
			},
			reachable: []string{"node2"},
			replies:   map[string]statusReply{"node2": {status: ready, size: 0}},
			wantPerNode: map[string]map[string]string{
				"s1": {"down": unavailable, "node2": ready},
				"s2": {"node2": ready, "down": unavailable},
				"s3": {"down": unavailable},
			},
			wantStatus: map[string]string{"s1": unavailable, "s2": ready, "s3": unavailable},
			wantSize:   map[string]int64{"s1": 0, "s2": 0, "s3": 0},
		},
		{
			name:          "queue size skips a replica that does not serve reads",
			physical:      map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node1", "node2"}}},
			unreadable:    []string{"node1"},
			reachable:     []string{"node1", "node2"},
			replies:       map[string]statusReply{"node1": {status: ready, size: 3}, "node2": {status: indexing, size: 7}},
			wantPerNode:   map[string]map[string]string{"s1": {"node1": ready, "node2": indexing}},
			wantStatus:    map[string]string{"s1": ready},
			wantSize:      map[string]int64{"s1": 7},
			queueNotAsked: []string{"node1/s1"},
		},
		{
			name:        "a tenant copy on this node that does not serve reads is unavailable",
			mt:          true,
			physical:    map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{shardStatusLocalNode, "node1"}, Status: models.TenantActivityStatusHOT}},
			local:       map[string]localShardKind{"hot": localReady},
			unreadable:  []string{shardStatusLocalNode},
			reachable:   []string{"node1"},
			replies:     map[string]statusReply{"node1": {status: ready, size: 7}},
			wantPerNode: map[string]map[string]string{"hot": {shardStatusLocalNode: unavailable, "node1": ready}},
			wantStatus:  map[string]string{"hot": unavailable},
			wantSize:    map[string]int64{"hot": 7},
		},
		{
			name:        "a single-tenant copy on this node that does not serve reads reports its storage status",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node1"}}},
			local:       map[string]localShardKind{"s1": localReady},
			unreadable:  []string{shardStatusLocalNode},
			reachable:   []string{"node1"},
			replies:     map[string]statusReply{"node1": {status: ready, size: 7}},
			wantPerNode: map[string]map[string]string{"s1": {shardStatusLocalNode: ready, "node1": ready}},
			wantStatus:  map[string]string{"s1": ready},
			wantSize:    map[string]int64{"s1": 7},
		},
		{
			name:          "local replica answers the queue size without asking a peer",
			physical:      map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2", shardStatusLocalNode}}},
			local:         map[string]localShardKind{"s1": localReady},
			reachable:     []string{"node2"},
			replies:       map[string]statusReply{"node2": {status: indexing, size: 7}},
			wantPerNode:   map[string]map[string]string{"s1": {shardStatusLocalNode: ready, "node2": indexing}},
			wantStatus:    map[string]string{"s1": ready},
			wantSize:      map[string]int64{"s1": 0},
			queueNotAsked: []string{"node2/s1"},
		},
		{
			name:      "local replica missing its shard asks a peer, not itself",
			physical:  map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2", shardStatusLocalNode}}},
			reachable: []string{"node2"},
			replies: map[string]statusReply{
				shardStatusLocalNode: shardNotFound,
				"node2":              {status: indexing, size: 7},
			},
			wantPerNode: map[string]map[string]string{"s1": {shardStatusLocalNode: unavailable, "node2": indexing}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 7},
			notAsked:    []string{shardStatusLocalNode + "/s1"},
		},
		{
			name:      "recovering local replica asks a peer, not itself",
			physical:  map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node2"}}},
			local:     map[string]localShardKind{"s1": localRecovering},
			reachable: []string{"node2"},
			replies: map[string]statusReply{
				shardStatusLocalNode: {err: errors.New("status code: 422 body: (local s1 shard is not ready)")},
				"node2":              {status: indexing, size: 7},
			},
			wantPerNode: map[string]map[string]string{"s1": {shardStatusLocalNode: unavailable, "node2": indexing}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 7},
			notAsked:    []string{shardStatusLocalNode + "/s1"},
		},
		{
			name:        "local replica shutting down marks only itself unavailable",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node2"}}},
			local:       map[string]localShardKind{"s1": localShuttingDown},
			reachable:   []string{"node2"},
			replies:     map[string]statusReply{"node2": {status: ready, size: 0}},
			wantPerNode: map[string]map[string]string{"s1": {shardStatusLocalNode: unavailable, "node2": ready}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 0},
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
			wantPerNode: map[string]map[string]string{
				"hot":    {"node1": ready, "node2": ready},
				"cold":   {"node1": readonly, "node2": readonly},
				"frozen": {"node1": unavailable, "node2": unavailable},
			},
			wantStatus:    map[string]string{"hot": ready, "cold": readonly, "frozen": unavailable},
			wantSize:      map[string]int64{"hot": 0, "cold": 0, "frozen": 0},
			queueNotAsked: []string{"node1/cold", "node2/cold", "node1/frozen", "node2/frozen"},
		},
		{
			name:        "tenant deleted while listing is left out",
			mt:          true,
			physical:    map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT}},
			deleted:     []string{"gone"},
			reachable:   []string{"node1"},
			replies:     map[string]statusReply{"node1": {status: ready, size: 0}},
			wantPerNode: map[string]map[string]string{"hot": {"node1": ready}},
			wantStatus:  map[string]string{"hot": ready},
			wantSize:    map[string]int64{"hot": 0},
		},
		{
			name:          "listing a multi-tenant collection reads a local HOT tenant without a tenant status lookup",
			mt:            true,
			physical:      map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{shardStatusLocalNode, "node1"}, Status: models.TenantActivityStatusHOT}},
			local:         map[string]localShardKind{"hot": localReady},
			reachable:     []string{"node1"},
			replies:       map[string]statusReply{"node1": {status: ready, size: 0}},
			wantPerNode:   map[string]map[string]string{"hot": {shardStatusLocalNode: ready, "node1": ready}},
			wantStatus:    map[string]string{"hot": ready},
			wantSize:      map[string]int64{"hot": 0},
			queueNotAsked: []string{"node1/hot"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, lookup := range []struct {
				name     string
				run      func(t *testing.T, ctx context.Context, index *Index) error
				notAsked []string
			}{
				{
					name: "status",
					run: func(t *testing.T, ctx context.Context, index *Index) error {
						perNode, status, err := index.getShardsStorageStatus(ctx, "")
						if err != nil {
							return err
						}
						require.Equal(t, tc.wantPerNode, perNode, "per node status")
						require.Equal(t, tc.wantStatus, status, "legacy status")
						return nil
					},
					notAsked: tc.notAsked,
				},
				{
					name: "queue size",
					run: func(t *testing.T, ctx context.Context, index *Index) error {
						size, err := index.getShardsQueueSize(ctx, "")
						if err != nil {
							return err
						}
						require.Equal(t, tc.wantSize, size)
						return nil
					},
					notAsked: append(slices.Clone(tc.notAsked), tc.queueNotAsked...),
				},
			} {
				t.Run(lookup.name, func(t *testing.T) {
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
					err := lookup.run(t, ctx, index)

					// Assert
					require.NoError(t, err)
					for _, call := range lookup.notAsked {
						require.NotContains(t, client.askedFor(), call, "replica asked")
					}
					for _, entry := range hook.AllEntries() {
						require.Greater(t, entry.Level, logrus.ErrorLevel, "logged at error or above: %s", entry.Message)
					}
				})
			}
		})
	}
}

// shardLookups runs each lookup over the whole collection.
var shardLookups = map[string]func(context.Context, *Index) error{
	"status": func(ctx context.Context, index *Index) error {
		_, _, err := index.getShardsStorageStatus(ctx, "")
		return err
	},
	"queue size": func(ctx context.Context, index *Index) error {
		_, err := index.getShardsQueueSize(ctx, "")
		return err
	},
}

func TestIndex_ShardsStatusAndQueueSize_NamespaceGate(t *testing.T) {
	for _, tc := range []struct {
		state   api.NamespaceState
		wantErr error
	}{
		{state: api.NamespaceStateSuspended, wantErr: namespaces.ErrNamespaceSuspended},
		{state: api.NamespaceStateActive},
	} {
		for name, lookup := range shardLookups {
			t.Run(string(tc.state)+"/"+name, func(t *testing.T) {
				// Arrange
				client := &shardStatusRemoteClient{replies: map[string]statusReply{"node2": {status: "READY", size: 7}}}
				index, _ := newShardStatusIndex(t, false,
					map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2"}}},
					nil, []string{"node2"}, client)
				index.namespace = "alpha"
				index.namespacesExister = existerWithState(t, tc.state)

				// Act
				err := lookup(t.Context(), index)

				// Assert
				if tc.wantErr == nil {
					require.NoError(t, err)
					require.NotEmpty(t, client.askedFor(), "replica asked")
					return
				}
				require.ErrorIs(t, err, tc.wantErr)
				require.Empty(t, client.askedFor(), "replica asked")
			})
		}
	}
}

func TestIndex_ShardsStatusAndQueueSize_SkipPeerThatTimedOut(t *testing.T) {
	limit := runtime.GOMAXPROCS(0) * 4
	physical := make(map[string]sharding.Physical, 3*limit)
	for n := range 3 * limit {
		name := fmt.Sprintf("s%03d", n)
		physical[name] = sharding.Physical{Name: name, BelongsToNodes: []string{"stuck", "node2"}}
	}
	replies := map[string]statusReply{
		"stuck": {err: fmt.Errorf("connect: %w", context.DeadlineExceeded)},
		"node2": {status: "READY", size: 7},
	}
	stuckCalls := func(client *shardStatusRemoteClient) int {
		calls := 0
		for _, call := range client.askedFor() {
			if strings.HasPrefix(call, "stuck/") {
				calls++
			}
		}
		return calls
	}
	for name, lookup := range shardLookups {
		t.Run(name, func(t *testing.T) {
			// Arrange
			client := &shardStatusRemoteClient{replies: replies}
			index, _ := newShardStatusIndex(t, false, physical, nil, []string{"stuck", "node2"}, client)

			// Act
			err := lookup(t.Context(), index)

			// Assert
			require.NoError(t, err)
			require.LessOrEqual(t, stuckCalls(client), limit, "calls to the peer that timed out")
		})
	}
	t.Run("queue size after status in one listing", func(t *testing.T) {
		// Arrange
		client := &shardStatusRemoteClient{replies: replies}
		index, _ := newShardStatusIndex(t, false, physical, nil, []string{"stuck", "node2"}, client)
		ctx := sharding.WithTimedOutPeers(t.Context())
		require.NoError(t, shardLookups["status"](ctx, index))
		afterStatus := stuckCalls(client)

		// Act
		err := shardLookups["queue size"](ctx, index)

		// Assert
		require.NoError(t, err)
		require.Equal(t, afterStatus, stuckCalls(client), "queue size asked the peer that timed out on status")
	})
}
