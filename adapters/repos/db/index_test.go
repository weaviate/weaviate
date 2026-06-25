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
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/cluster/router/types"
	routertypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
)

func TestIndex_aggregateCount(t *testing.T) {
	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(logger, monitoring.GetMetrics(), "Abc", "n/a")
	require.NoError(t, err, "create index metrics")

	replicaMetrics, err := replica.NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err, "create replica metrics")

	shardRegex := regexp.MustCompile(`\/shards\/([^\/]*)`)
	addReplicas := func(t *testing.T, id int, shards map[string]int) []types.Replica {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			groups := shardRegex.FindStringSubmatch(r.URL.Path)
			count := shards[groups[1]]
			io.WriteString(w, strconv.Itoa(count))
		}))
		t.Cleanup(srv.Close)

		var replicas []types.Replica
		for name := range shards {
			replicas = append(replicas, types.Replica{
				NodeName:  fmt.Sprintf("node-%d", id),
				HostAddr:  srv.URL[7:],
				ShardName: name,
			})
		}
		return replicas
	}

	// Each test case will spin up N test HTTP servers, where N=len(counts).
	// Each server mocks a replica and reports its counts[i].
	// To simplify the setup, we assume the test collection "Abc" is
	// fully contained within a single shard "abc" if tt.nodes is nil.
	// Otherwise [tt.nodes] defines the counts for each shard and [tt.shards]
	// defines all shards belonging to "Abc".
	// The aggregation requests satisfies [aggregate.IsCountStar], so we expect
	// each of the replicas to be queried and their results "averaged".
	for _, tt := range []struct {
		name   string // Test case name
		counts []int  // Counts reported by each replica.

		nodes  []map[string]int // Per-shard count in each node.
		shards []string         // Complete list of shards in collection.
		tenant string

		want int // Expected aggregated count
	}{
		{
			name:   "consistent count",
			counts: []int{92, 92, 92},
			want:   92,
		},
		{
			name:   "mode (2/3 replicas agree)",
			counts: []int{92, 13, 92},
			want:   92,
		},
		{
			name:   "median (3 replicas disagree)",
			counts: []int{80085, 33, 92},
			want:   92,
		},
		{
			name:   "shifted median (4 replicas disagree)",
			counts: []int{1, 2, 3, 4},
			want:   3,
		},
		{
			name:   "multiple shards",
			shards: []string{"abc", "xyz"},
			nodes: []map[string]int{
				{
					"abc": 1,
					"xyz": 2,
				},
				{
					"abc": 1,
					"xyz": 2,
				},
			},
			want: 3,
		},
		{
			name:   "one tenant",
			shards: []string{"john_doe", "jane_doe"},
			nodes: []map[string]int{
				{
					"john_doe": 1,
					"jane_doe": 2,
				},
				{
					"john_doe": 1,
					"jane_doe": 2,
				},
			},
			tenant: "john_doe",
			want:   1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			if tt.shards == nil {
				tt.shards = append(tt.shards, "abc")
			}

			var replicas []types.Replica
			for i, totalCount := range tt.counts {
				replicas = append(replicas, addReplicas(t, i, map[string]int{"abc": totalCount})...)
			}
			for i, shards := range tt.nodes {
				replicas = append(replicas, addReplicas(t, i, shards)...)
			}
			slices.SortFunc(replicas, func(r1, r2 types.Replica) int {
				return strings.Compare(r1.ShardName, r2.ShardName)
			})

			router := &fakeRouter{
				readPlan: types.ReadRoutingPlan{
					IntConsistencyLevel: len(tt.counts) + len(tt.nodes),
					ConsistencyLevel:    types.ConsistencyLevelAll,
					ReplicaSet: types.ReadReplicaSet{
						Replicas: replicas,
					},
				},
			}
			index := Index{
				router: router,
				replicator: &replica.Replicator{
					Finder: replica.NewFinder(
						"Abc",
						router,
						cluster.NewMockNodeResolver(t),
						"node-1",
						func() replica.Client {
							c, err := clients.NewReplicationClient(&http.Client{})
							require.NoError(t, err)
							return c
						}(),
						replicaMetrics,
						logger,
						func() string { return "Delete" }),
				},
				shardCreateLocks: esync.NewKeyRWLocker(),
				Config:           IndexConfig{ClassName: schema.ClassName("Abc")},
				metrics:          metrics,
				logger:           logger,
			}

			// Act
			res, err := index.aggregate(t.Context(), nil, aggregation.Params{
				IncludeMetaCount: true,
				Tenant:           tt.tenant,
			}, nil)

			// Assert
			require.NoError(t, err, "aggregate")
			require.Len(t, res.Groups, 1, "number of groups")
			require.Equal(t, tt.want, res.Groups[0].Count, "object count")
		})
	}
}

type fakeRouter struct {
	hostnames map[string]string
	options   types.RoutingPlanBuildOptions
	readPlan  types.ReadRoutingPlan
	writePlan types.WriteRoutingPlan
	readSet   types.ReadReplicaSet
	writeSet  types.WriteReplicaSet
}

// AllHostnames implements [types.Router].
func (f *fakeRouter) AllHostnames() []string {
	return slices.Collect(maps.Values(f.hostnames))
}

var _ types.Router = (*fakeRouter)(nil)

func (f *fakeRouter) BuildReadRoutingPlan(opt types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readPlan := f.readPlan
	readPlan.Shard = opt.Shard
	readPlan.Tenant = opt.Tenant

	if opt.Tenant != "" {
		var filtered []types.Replica
		for _, r := range readPlan.ReplicaSet.Replicas {
			if r.ShardName == opt.Tenant {
				filtered = append(filtered, r)
			}
		}
		readPlan.ReplicaSet.Replicas = filtered
	}
	return readPlan, nil
}

func (f *fakeRouter) BuildRoutingPlanOptions(tenant, shard string, cl types.ConsistencyLevel, directCandidate string) types.RoutingPlanBuildOptions {
	return f.options
}

func (f *fakeRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return f.writePlan, nil
}

func (f *fakeRouter) GetReadReplicasLocation(collection, tenant, shard string) (types.ReadReplicaSet, error) {
	return f.readSet, nil
}

func (f *fakeRouter) GetReadWriteReplicasLocation(collection, tenant, shard string) (types.ReadReplicaSet, types.WriteReplicaSet, error) {
	return f.readSet, f.writeSet, nil
}

func (f *fakeRouter) GetWriteReplicasLocation(collection, tenant, shard string) (types.WriteReplicaSet, error) {
	panic("unimplemented")
}

func (f *fakeRouter) NodeHostname(nodeName string) (string, bool) {
	host, ok := f.hostnames[nodeName]
	return host, ok
}

// TestIndex_ShardHasMultipleReplicasWrite_RoutesThroughReplicatorDuringMovement pins the
// rf=1 scale-out lost-write fix: while a replica movement is active for the shard, a write
// must be routed through the replicator (so it registers in the in-flight fence and the
// source's change-capture log cannot be sealed over it), even at replicationFactor=1 with a
// single-node write-set. Without the fix the "active movement, single replica" row returns
// false — the write would take the direct-local path that bypasses the fence.
func TestIndex_ShardHasMultipleReplicasWrite_RoutesThroughReplicatorDuringMovement(t *testing.T) {
	const (
		className = "MoveClass"
		shardName = "shard1"
		tenant    = ""
	)

	tests := []struct {
		name           string
		replicationF   int64
		movementActive bool
		// writeReplicas is the router's write-set node names. nil means the router must NOT
		// be consulted (the call should short-circuit before GetWriteReplicasLocation).
		writeReplicas []string
		want          bool
	}{
		{
			name:           "rf=1, no movement, single replica -> direct-local",
			replicationF:   1,
			movementActive: false,
			writeReplicas:  []string{"node-A"},
			want:           false,
		},
		{
			name:           "rf=1, ACTIVE movement, single replica -> replicator (regression pin)",
			replicationF:   1,
			movementActive: true,
			writeReplicas:  nil, // movement short-circuits before the router is consulted
			want:           true,
		},
		{
			name:           "rf=1, no movement, two replicas -> replicator",
			replicationF:   1,
			movementActive: false,
			writeReplicas:  []string{"node-A", "node-B"},
			want:           true,
		},
		{
			name:           "rf>1 -> replicator (short-circuit, FSM+router untouched)",
			replicationF:   3,
			movementActive: false,
			writeReplicas:  nil,
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsm := &fakeFSMReader{active: tt.movementActive}

			mockRouter := routertypes.NewMockRouter(t)
			if tt.writeReplicas != nil {
				replicas := make([]routertypes.Replica, 0, len(tt.writeReplicas))
				for _, n := range tt.writeReplicas {
					replicas = append(replicas, routertypes.Replica{NodeName: n, ShardName: shardName})
				}
				mockRouter.EXPECT().
					GetWriteReplicasLocation(className, tenant, shardName).
					Return(routertypes.WriteReplicaSet{Replicas: replicas}, nil).
					Once()
			}
			// When writeReplicas is nil we set no expectation, so any router call fails the
			// test — proving the call short-circuited before consulting the router.

			idx := &Index{
				Config:               IndexConfig{ClassName: schema.ClassName(className), ReplicationFactor: tt.replicationF},
				replicationFSMReader: fsm,
				router:               mockRouter,
			}

			got := idx.shardHasMultipleReplicasWrite(tenant, shardName)
			require.Equal(t, tt.want, got)

			if tt.replicationF > 1 {
				require.Zero(t, fsm.callCount, "rf>1 must short-circuit before consulting the replication FSM")
			}
			if tt.movementActive {
				require.Equal(t, className, fsm.gotColl)
				require.Equal(t, shardName, fsm.gotShard)
			}
		})
	}
}
