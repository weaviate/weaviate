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
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
	hang   bool // block until the caller's context ends
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
	if r.hang {
		<-ctx.Done()
		return r, ctx.Err()
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

// physicalShards owns each shard by its first replica.
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

// newShardStatusIndex uses the real router; the schema getter fails any tenant (activation) lookup.
func newShardStatusIndex(t *testing.T, mt bool, physical map[string]sharding.Physical,
	local map[string]localShardKind, reachable []string, client *shardStatusRemoteClient,
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
	schemaReader.EXPECT().Shards(className).Return(names, nil).Maybe()
	schemaReader.EXPECT().ShardReplicas(className, mock.Anything).RunAndReturn(
		func(_, shard string) ([]string, error) { return physical[shard].BelongsToNodes, nil }).Maybe()
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
		unavailable = "UNAVAILABLE"
	)
	require.Equal(t, unavailable, storagestate.StatusUnavailable.String())

	hotAndCold := func(nodes ...string) map[string]sharding.Physical {
		return map[string]sharding.Physical{
			"hot":  {Name: "hot", BelongsToNodes: nodes, Status: models.TenantActivityStatusHOT},
			"cold": {Name: "cold", BelongsToNodes: nodes, Status: models.TenantActivityStatusCOLD},
		}
	}
	shardNotFound := statusReply{err: errors.New("status code: 500 body: (local shard not found)")}
	refused := statusReply{err: errors.New("connect: connection refused")}

	for _, tc := range []struct {
		name        string
		mt          bool
		tenant      string
		physical    map[string]sharding.Physical
		local       map[string]localShardKind
		reachable   []string // nodes besides the coordinator that resolve to a host
		replies     map[string]statusReply
		wantPerNode map[string]map[string]string
		wantStatus  map[string]string // legacy: the local replica, else the first listed
		wantSize    map[string]int64
		notAsked    []string // "node/shard", by either lookup
		queueAsks   []string // "node/shard" the queue lookup must not ask
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
			name:        "shard no replica answers for is unavailable",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node1", "node2"}}},
			reachable:   []string{"node1", "node2"},
			replies:     map[string]statusReply{"node1": refused, "node2": refused},
			wantPerNode: map[string]map[string]string{"s1": {"node1": unavailable, "node2": unavailable}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 0},
		},
		{
			name:        "shard whose replicas are all unresolvable is unavailable",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"down1", "down2"}}},
			wantPerNode: map[string]map[string]string{"s1": {"down1": unavailable, "down2": unavailable}},
			wantStatus:  map[string]string{"s1": unavailable},
			wantSize:    map[string]int64{"s1": 0},
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
			name:        "local replica answers the queue size without asking a peer",
			physical:    map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"node2", shardStatusLocalNode}}},
			local:       map[string]localShardKind{"s1": localReady},
			reachable:   []string{"node2"},
			replies:     map[string]statusReply{"node2": {status: indexing, size: 7}},
			wantPerNode: map[string]map[string]string{"s1": {shardStatusLocalNode: ready, "node2": indexing}},
			wantStatus:  map[string]string{"s1": ready},
			wantSize:    map[string]int64{"s1": 0},
			queueAsks:   []string{"node2/s1"},
		},
		{
			name:      "local replica missing its shard asks a peer, not itself",
			physical:  map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{shardStatusLocalNode, "node2"}}},
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
			name:      "listing a multi-tenant collection reports a COLD tenant unavailable without asking replicas",
			mt:        true,
			physical:  hotAndCold("node1", "node2"),
			reachable: []string{"node1", "node2"},
			replies: map[string]statusReply{
				"node1/hot": {status: ready, size: 0},
				"node2/hot": {status: ready, size: 0},
				"node1":     shardNotFound,
				"node2":     shardNotFound,
			},
			wantPerNode: map[string]map[string]string{
				"hot":  {"node1": ready, "node2": ready},
				"cold": {"node1": unavailable, "node2": unavailable},
			},
			wantStatus: map[string]string{"hot": ready, "cold": unavailable},
			wantSize:   map[string]int64{"hot": 0, "cold": 0},
			notAsked:   []string{"node1/cold", "node2/cold"},
		},
		{
			name: "COLD tenants listed after HOT ones are recorded alongside the replica reads",
			mt:   true,
			physical: map[string]sharding.Physical{
				"a-hot":  {Name: "a-hot", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT},
				"b-hot":  {Name: "b-hot", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT},
				"y-cold": {Name: "y-cold", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusCOLD},
				"z-cold": {Name: "z-cold", BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusFROZEN},
			},
			reachable: []string{"node1"},
			replies:   map[string]statusReply{"node1": {status: ready, size: 0}},
			wantPerNode: map[string]map[string]string{
				"a-hot":  {"node1": ready},
				"b-hot":  {"node1": ready},
				"y-cold": {"node1": unavailable},
				"z-cold": {"node1": unavailable},
			},
			wantStatus: map[string]string{"a-hot": ready, "b-hot": ready, "y-cold": unavailable, "z-cold": unavailable},
			wantSize:   map[string]int64{"a-hot": 0, "b-hot": 0, "y-cold": 0, "z-cold": 0},
			notAsked:   []string{"node1/y-cold", "node1/z-cold"},
		},
		{
			name:        "asking for a COLD tenant held locally neither activates it nor asks replicas",
			mt:          true,
			tenant:      "cold",
			physical:    hotAndCold(shardStatusLocalNode, "node1"),
			reachable:   []string{"node1"},
			replies:     map[string]statusReply{"node1": shardNotFound},
			wantPerNode: map[string]map[string]string{"cold": {shardStatusLocalNode: unavailable, "node1": unavailable}},
			wantStatus:  map[string]string{"cold": unavailable},
			wantSize:    map[string]int64{"cold": 0},
			notAsked:    []string{"node1/cold"},
		},
		{
			name:        "listing a multi-tenant collection reads a local HOT tenant without a tenant status lookup",
			mt:          true,
			physical:    map[string]sharding.Physical{"hot": {Name: "hot", BelongsToNodes: []string{shardStatusLocalNode, "node1"}, Status: models.TenantActivityStatusHOT}},
			local:       map[string]localShardKind{"hot": localReady},
			reachable:   []string{"node1"},
			replies:     map[string]statusReply{"node1": {status: ready, size: 0}},
			wantPerNode: map[string]map[string]string{"hot": {shardStatusLocalNode: ready, "node1": ready}},
			wantStatus:  map[string]string{"hot": ready},
			wantSize:    map[string]int64{"hot": 0},
			queueAsks:   []string{"node1/hot"},
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
						perNode, status, err := index.getShardsStorageStatus(ctx, tc.tenant)
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
						size, err := index.getShardsQueueSize(ctx, tc.tenant)
						if err != nil {
							return err
						}
						require.Equal(t, tc.wantSize, size)
						return nil
					},
					notAsked: append(slices.Clone(tc.notAsked), tc.queueAsks...),
				},
			} {
				t.Run(lookup.name, func(t *testing.T) {
					// Arrange
					client := &shardStatusRemoteClient{replies: tc.replies}
					index, hook := newShardStatusIndex(t, tc.mt, tc.physical, tc.local, tc.reachable, client)
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
