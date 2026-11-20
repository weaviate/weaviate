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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding/remote"
)

const (
	recoveringSearchClass = "RecoveringSearch"
	recoveringSearchShard = "S1"
	recoveringSearchLocal = "node1"
	recoveringSearchCount = 7
	recoveringSearchTime  = int64(100)
)

var (
	recoveringSearchNodes = []string{"node1", "node2", "node3"}
	recoveringSearchID    = strfmt.UUID("8c29da7a-600a-43dc-85fb-83ab2b08c294")
)

func recoveringSearchHost(node string) string { return node + "-host" }

func recoveringSearchObject() *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 recoveringSearchID,
			Class:              recoveringSearchClass,
			LastUpdateTimeUnix: recoveringSearchTime,
		},
	}
}

// recoveringSearchPeers answers as the cluster would: servedBy[host] serves a peer's request in-process.
type recoveringSearchPeers struct {
	FakeRemoteClient
	servedBy map[string]*Index
}

func (p *recoveringSearchPeers) SearchShard(ctx context.Context, host, _, shard string,
	vector []models.Vector, targetVector []string, distance float32, limit int,
	filters *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy, adds additional.Properties,
	targetCombination *dto.TargetCombination, properties []string, vectorSearchParams *searchparams.VectorSearchParams,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	if idx, ok := p.servedBy[host]; ok {
		return idx.IncomingSearch(ctx, shard, vector, targetVector, distance, limit, filters,
			keywordRanking, sort, cursor, groupBy, adds, targetCombination, properties, vectorSearchParams)
	}
	return []*storobj.Object{recoveringSearchObject()}, []float32{0.5}, nil, nil
}

func (p *recoveringSearchPeers) Aggregate(ctx context.Context, host, _, shard string,
	params aggregation.Params,
) (*aggregation.Result, error) {
	if idx, ok := p.servedBy[host]; ok {
		return idx.IncomingAggregate(ctx, shard, params, nil)
	}
	return &aggregation.Result{Groups: []aggregation.Group{{Count: 1}}}, nil
}

func (p *recoveringSearchPeers) GetShardStatus(ctx context.Context, host, _, shard string) (string, error) {
	if idx, ok := p.servedBy[host]; ok {
		return idx.IncomingGetShardStatus(ctx, shard)
	}
	return storagestate.StatusReady.String(), nil
}

// recoveringSearchReplicas is the replica read client: the local host is served in-process, a peer over gRPC.
type recoveringSearchReplicas struct {
	replica.Client
	servedBy map[string]*Index
}

func (r *recoveringSearchReplicas) serve(host string, call func(idx *Index) error) (bool, error) {
	idx, ok := r.servedBy[host]
	if !ok {
		return false, nil
	}
	err := call(idx)
	if err != nil && host != recoveringSearchHost(recoveringSearchLocal) {
		err = fmt.Errorf("gRPC: %w", status.Error(codes.FailedPrecondition, err.Error()))
	}
	return true, err
}

func (r *recoveringSearchReplicas) DigestObjects(ctx context.Context, host, _, shard string,
	ids []strfmt.UUID, _ int,
) ([]types.RepairResponse, error) {
	var out []types.RepairResponse
	if served, err := r.serve(host, func(idx *Index) (err error) {
		out, err = idx.DigestObjects(ctx, shard, ids)
		return err
	}); served {
		return out, err
	}
	out = make([]types.RepairResponse, len(ids))
	for i, id := range ids {
		out[i] = types.RepairResponse{ID: id.String(), UpdateTime: recoveringSearchTime}
	}
	return out, nil
}

func (r *recoveringSearchReplicas) CountObjects(ctx context.Context, host, _, shard string) (int, error) {
	var out int
	if served, err := r.serve(host, func(idx *Index) (err error) {
		out, err = idx.IncomingCountObjects(ctx, shard)
		return err
	}); served {
		return out, err
	}
	return recoveringSearchCount, nil
}

func recoveringSearchRouter(t *testing.T) *types.MockRouter {
	router := types.NewMockRouter(t)
	replicas := make([]types.Replica, 0, len(recoveringSearchNodes))
	for _, n := range recoveringSearchNodes {
		replicas = append(replicas, types.Replica{NodeName: n, ShardName: recoveringSearchShard, HostAddr: recoveringSearchHost(n)})
	}
	router.EXPECT().BuildRoutingPlanOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(tenant, shard string, cl types.ConsistencyLevel, direct string) types.RoutingPlanBuildOptions {
			return types.RoutingPlanBuildOptions{Shard: shard, Tenant: tenant, ConsistencyLevel: cl, DirectCandidateNode: direct}
		}).Maybe()
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).RunAndReturn(
		func(opts types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
			preferred := opts.DirectCandidateNode
			if preferred == "" {
				preferred = recoveringSearchLocal
			}
			ordered := slices.Clone(replicas)
			slices.SortStableFunc(ordered, func(a, b types.Replica) int {
				switch {
				case a.NodeName == preferred:
					return -1
				case b.NodeName == preferred:
					return 1
				}
				return 0
			})
			cl := opts.ConsistencyLevel
			if cl == "" {
				cl = types.ConsistencyLevelOne
			}
			return types.ReadRoutingPlan{
				LocalHostname:       recoveringSearchLocal,
				Shard:               opts.Shard,
				ReplicaSet:          types.ReadReplicaSet{Replicas: ordered},
				ConsistencyLevel:    cl,
				IntConsistencyLevel: cl.ToInt(len(replicas)),
			}, nil
		}).Maybe()
	router.EXPECT().GetReadReplicasLocation(mock.Anything, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{Replicas: replicas}, nil).Maybe()
	return router
}

func recoveringShardStub() *RecoveringShard {
	rec := &RecoveringShard{LazyLoadShard: &LazyLoadShard{}}
	rec.blockLoad(enterrors.ErrShardRecovering)
	return rec
}

// recoveringSearchIndex coordinates on node1 with recovering's replica of the shard mid-recovery.
func recoveringSearchIndex(t *testing.T, recovering string) *Index {
	t.Helper()
	logger, _ := test.NewNullLogger()

	var local ShardLike
	if recovering == recoveringSearchLocal {
		local = recoveringShardStub()
	} else {
		healthy := NewMockShardLike(t)
		healthy.EXPECT().preventShutdown().Return(func() {}, nil).Maybe()
		healthy.EXPECT().GetStatus().Return(storagestate.StatusReady).Maybe()
		healthy.EXPECT().ID().Return(recoveringSearchShard).Maybe()
		healthy.EXPECT().Name().Return(recoveringSearchShard).Maybe()
		healthy.EXPECT().ObjectSearch(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(context.Context, int, *filters.LocalFilter, *searchparams.KeywordRanking, []filters.Sort, *filters.Cursor, additional.Properties, []string) ([]*storobj.Object, []float32, error) {
				return []*storobj.Object{recoveringSearchObject()}, []float32{0.5}, nil
			}).Maybe()
		healthy.EXPECT().ObjectVectorSearch(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(context.Context, []models.Vector, []string, float32, int, *filters.LocalFilter, []filters.Sort, *searchparams.GroupBy, additional.Properties, *dto.TargetCombination, []string, *searchparams.VectorSearchParams) ([]*storobj.Object, []float32, error) {
				return []*storobj.Object{recoveringSearchObject()}, []float32{0.5}, nil
			}).Maybe()
		healthy.EXPECT().Aggregate(mock.Anything, mock.Anything, mock.Anything).Return(
			&aggregation.Result{Groups: []aggregation.Group{{Count: 1}}}, nil).Maybe()
		healthy.EXPECT().ObjectCount(mock.Anything).Return(recoveringSearchCount, nil).Maybe()
		healthy.EXPECT().ObjectDigests(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, ids []multi.Identifier) ([]types.RepairResponse, error) {
				out := make([]types.RepairResponse, len(ids))
				for i, id := range ids {
					out[i] = types.RepairResponse{ID: id.ID, UpdateTime: recoveringSearchTime}
				}
				return out, nil
			}).Maybe()
		local = healthy
	}

	idx := newTestIndex(t, logger, recoveringSearchClass, nil, map[string]ShardLike{recoveringSearchShard: local})
	idx.shardCreateLocks = esync.NewKeyRWLocker()
	idx.Config.ReplicationFactor = int64(len(recoveringSearchNodes))
	idx.Config.ClassName = schema.ClassName(recoveringSearchClass)

	idx.Config.NodeName = recoveringSearchLocal
	idx.getSchema = schemaUC.NewMockSchemaGetter(t)

	router := recoveringSearchRouter(t)
	idx.router = router

	nodeResolver := cluster.NewMockNodeResolver(t)
	for _, n := range recoveringSearchNodes {
		nodeResolver.EXPECT().NodeHostname(n).Return(recoveringSearchHost(n), true).Maybe()
	}

	servedBy := map[string]*Index{recoveringSearchHost(recoveringSearchLocal): idx}
	if recovering != recoveringSearchLocal {
		peer := newTestIndex(t, logger, recoveringSearchClass, nil, map[string]ShardLike{recoveringSearchShard: recoveringShardStub()})
		peer.shardCreateLocks = esync.NewKeyRWLocker()
		peer.Config.ReplicationFactor = idx.Config.ReplicationFactor
		servedBy[recoveringSearchHost(recovering)] = peer
	}
	idx.remote = remote.NewIndex(recoveringSearchClass, shardReplicasOn(recoveringSearchNodes), nodeResolver,
		&recoveringSearchPeers{servedBy: servedBy})

	rep, err := replica.NewReplicator(recoveringSearchClass, router, nodeResolver, recoveringSearchLocal,
		func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
		&recoveringSearchReplicas{servedBy: servedBy}, monitoring.GetMetrics(), logger)
	require.NoError(t, err)
	idx.replicator = rep
	return idx
}

// A search coordinated next to a recovering replica must be served by a healthy one, without waiting out a timeout.
func TestSearchFailsOverOffARecoveringReplica(t *testing.T) {
	const bound = 5 * time.Second
	searches := []struct {
		name string
		run  func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error)
	}{
		{"vector", func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error) {
			objs, _, err := idx.objectVectorSearch(ctx, []models.Vector{[]float32{1, 2}}, nil, 0, 10, nil, nil, nil,
				additional.Properties{}, repl, "", nil, nil, nil)
			return len(objs), err
		}},
		{"bm25", func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error) {
			objs, _, err := idx.objectSearch(ctx, 10, nil,
				&searchparams.KeywordRanking{Type: "bm25", Query: "q", Properties: []string{"name"}},
				nil, nil, additional.Properties{}, repl, "", 0, nil)
			return len(objs), err
		}},
		{"filtered", func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error) {
			objs, _, err := idx.objectSearch(ctx, 10, &filters.LocalFilter{Root: &filters.Clause{Operator: filters.OperatorEqual}},
				nil, nil, nil, additional.Properties{}, repl, "", 0, nil)
			return len(objs), err
		}},
		{"aggregate", func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error) {
			res, err := idx.aggregate(ctx, repl, aggregation.Params{
				ClassName:        schema.ClassName(recoveringSearchClass),
				IncludeMetaCount: true, Filters: &filters.LocalFilter{},
			}, nil)
			if err != nil {
				return 0, err
			}
			return res.Groups[0].Count, nil
		}},
		{"count", func(ctx context.Context, idx *Index, repl *additional.ReplicationProperties) (int, error) {
			res, err := idx.aggregate(ctx, repl, aggregation.Params{
				ClassName:        schema.ClassName(recoveringSearchClass),
				IncludeMetaCount: true,
			}, nil)
			if err != nil {
				return 0, err
			}
			if res.Groups[0].Count == recoveringSearchCount {
				return 1, nil
			}
			return 0, fmt.Errorf("count %d, want the healthy replicas' %d", res.Groups[0].Count, recoveringSearchCount)
		}},
	}
	for _, s := range searches {
		for _, cl := range []types.ConsistencyLevel{types.ConsistencyLevelOne, types.ConsistencyLevelQuorum, types.ConsistencyLevelAll} {
			for _, recovering := range recoveringSearchNodes {
				t.Run(fmt.Sprintf("%s/%s/recovering_%s", s.name, cl, recovering), func(t *testing.T) {
					t.Parallel()
					idx := recoveringSearchIndex(t, recovering)

					start := time.Now()
					got, err := s.run(t.Context(), idx, &additional.ReplicationProperties{ConsistencyLevel: string(cl)})
					elapsed := time.Since(start)

					require.NoError(t, err)
					require.Equal(t, 1, got, "a healthy replica's result must be returned")
					require.Less(t, elapsed, bound, "a recovering replica must not be waited on")
				})
			}
		}
	}
}
