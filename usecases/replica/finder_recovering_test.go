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

package replica_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

type recoveringRPC string

const (
	rpcCount  recoveringRPC = "CountObjects"
	rpcFetch  recoveringRPC = "FetchObject"
	rpcDigest recoveringRPC = "DigestObjects"
	rpcFind   recoveringRPC = "FindUUIDs"
)

// recoveringReplicaErr is what a replica's recovering shard answers rpc with, as the coordinator receives it over transport.
func recoveringReplicaErr(t *testing.T, rpc recoveringRPC, transport, shard string) error {
	t.Helper()
	cause := enterrors.NewErrUnprocessable(fmt.Errorf("get/init local shard %q, no shutdown: %w", shard,
		fmt.Errorf("LazyLoadShard::preventShutdown: %w", enterrors.ErrShardRecovering)))
	var served error
	var restBody string
	restCode := http.StatusUnprocessableEntity
	switch rpc {
	case rpcCount:
		served = fmt.Errorf("%w: shard %q", cause, shard)
		restBody, restCode = "count objects: "+served.Error(), http.StatusInternalServerError
	case rpcFetch:
		served = fmt.Errorf("shard %q does not exist locally: %w", shard, cause)
		restBody = "fetch objects: " + served.Error()
	case rpcDigest:
		served = fmt.Errorf("shard %q not found locally: %w", shard, cause)
		restBody = "digest objects: " + served.Error()
	case rpcFind:
		served = cause
		restBody = served.Error()
	}
	switch transport {
	case "local":
		return served
	case "grpc":
		st := status.Error(codes.FailedPrecondition, served.Error())
		if rpc == rpcCount {
			return fmt.Errorf("%w: %w", replica.ErrAsyncReplicationNotActive, st)
		}
		return fmt.Errorf("gRPC %s: %w", rpc, st)
	case "rest":
		if rpc == rpcFind {
			return fmt.Errorf("unexpected status code %d (%s)", restCode, restBody)
		}
		return &clients.HTTPError{Code: restCode, Body: []byte(restBody)}
	}
	t.Fatalf("unknown transport %q", transport)
	return nil
}

func TestFinderFailsOverOffARecoveringReplica(t *testing.T) {
	const (
		cls        = "C1"
		shard      = "S1"
		id         = strfmt.UUID("8c29da7a-600a-43dc-85fb-83ab2b08c294")
		updateTime = int64(100)
		count      = 7
		bound      = time.Second
	)
	nodes := []string{"A", "B", "C"}

	type outcome struct {
		err error
		ok  bool
	}
	callers := []struct {
		name string
		// skipDirect: the direct candidate answers from the caller's own copy, so it cannot be the recovering one.
		skipDirect bool
		call       func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome
	}{
		{
			name: "CountObjects",
			call: func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome {
				replicas := make([]types.Replica, 0, len(nodes))
				for _, n := range nodes {
					replicas = append(replicas, types.Replica{NodeName: n, ShardName: shard, HostAddr: n})
				}
				got, err := f.CountObjects(ctx, types.ReadRoutingPlan{
					Shard:               shard,
					ReplicaSet:          types.ReadReplicaSet{Replicas: replicas},
					ConsistencyLevel:    level,
					IntConsistencyLevel: level.ToInt(len(nodes)),
				})
				return outcome{err, err == nil && got == count}
			},
		},
		{
			name: "GetOne",
			call: func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome {
				got, err := f.GetOne(ctx, level, shard, id, nil, additional.Properties{})
				return outcome{err, err == nil && got != nil && got.ID() == id}
			},
		},
		{
			name: "Exists",
			call: func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome {
				got, err := f.Exists(ctx, level, shard, id)
				return outcome{err, err == nil && got}
			},
		},
		{
			name:       "CheckConsistency",
			skipDirect: true,
			call: func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome {
				xs := []*storobj.Object{objectEx(id, updateTime, shard, nodes[0])}
				err := f.CheckConsistency(ctx, level, xs)
				return outcome{err, err == nil && xs[0].IsConsistent}
			},
		},
		{
			name: "FindUUIDs",
			call: func(ctx context.Context, f *replica.Finder, level types.ConsistencyLevel) outcome {
				got, err := f.FindUUIDs(ctx, cls, shard, nil, level, 0)
				return outcome{err, err == nil && len(got) == 1 && got[0] == id}
			},
		},
	}

	for _, caller := range callers {
		for _, level := range []types.ConsistencyLevel{types.ConsistencyLevelOne, types.ConsistencyLevelQuorum, types.ConsistencyLevelAll} {
			for pos, recovering := range nodes {
				if pos == 0 && caller.skipDirect {
					continue
				}
				for _, transport := range []string{"local", "grpc", "rest"} {
					name := fmt.Sprintf("%s/%s/recovering_hosts[%d]/%s", caller.name, level, pos, transport)
					t.Run(name, func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							f := newFakeFactory(t, cls, shard, nodes, false)
							finder := f.newFinder(nodes[0])

							f.RClient.EXPECT().CountObjects(mock.Anything, mock.Anything, cls, shard).RunAndReturn(
								func(_ context.Context, host, _, _ string) (int, error) {
									if host == recovering {
										return 0, recoveringReplicaErr(t, rpcCount, transport, shard)
									}
									return count, nil
								}).Maybe()
							f.RClient.EXPECT().FetchObject(mock.Anything, mock.Anything, cls, shard, id, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
								func(_ context.Context, host, _, _ string, _ strfmt.UUID, _ search.SelectProperties, _ additional.Properties, _ int) (replica.Replica, error) {
									if host == recovering {
										return replica.Replica{}, recoveringReplicaErr(t, rpcFetch, transport, shard)
									}
									return repl(id, updateTime, false), nil
								}).Maybe()
							f.RClient.EXPECT().DigestObjects(mock.Anything, mock.Anything, cls, shard, []strfmt.UUID{id}, mock.Anything).RunAndReturn(
								func(_ context.Context, host, _, _ string, _ []strfmt.UUID, _ int) ([]types.RepairResponse, error) {
									if host == recovering {
										return nil, recoveringReplicaErr(t, rpcDigest, transport, shard)
									}
									return []types.RepairResponse{{ID: id.String(), UpdateTime: updateTime}}, nil
								}).Maybe()
							f.RClient.EXPECT().FindUUIDs(mock.Anything, mock.Anything, cls, shard, mock.Anything, mock.Anything).RunAndReturn(
								func(_ context.Context, host, _, _ string, _ *filters.LocalFilter, _ int) ([]strfmt.UUID, error) {
									if host == recovering {
										return nil, recoveringReplicaErr(t, rpcFind, transport, shard)
									}
									return []strfmt.UUID{id}, nil
								}).Maybe()

							start := time.Now()
							got := caller.call(t.Context(), finder, level)
							elapsed := time.Since(start)

							require.Less(t, elapsed, bound, "a recovering replica must not be retried into the caller's timeout")
							reachable := level != types.ConsistencyLevelAll || caller.name == "CountObjects" || caller.name == "FindUUIDs"
							if reachable {
								require.NoError(t, got.err)
								require.True(t, got.ok, "the healthy replicas' answer must be returned")
								return
							}
							require.ErrorIs(t, got.err, replicaerrors.ErrRead)
							require.ErrorContains(t, got.err, enterrors.ErrShardRecovering.Error())
						})
					})
				}
			}
		}
	}
}
