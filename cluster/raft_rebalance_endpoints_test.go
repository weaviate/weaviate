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

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// queryForwardingClient answers every Query call with the query func. Any
// other method panics, because the embedded client is nil.
type queryForwardingClient struct {
	client
	query func(req *api.QueryRequest) (*api.QueryResponse, error)
}

func (c queryForwardingClient) Query(_ context.Context, _ string, req *api.QueryRequest) (*api.QueryResponse, error) {
	return c.query(req)
}

// toLeader sends the query to the leader's Store.Query. It turns any error into
// plain text, as a gRPC status does on the wire.
func toLeader(leader *Store) func(*api.QueryRequest) (*api.QueryResponse, error) {
	return func(req *api.QueryRequest) (*api.QueryResponse, error) {
		resp, err := leader.Query(req)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return resp, nil
	}
}

func failWith(err error) func(*api.QueryRequest) (*api.QueryResponse, error) {
	return func(*api.QueryRequest) (*api.QueryResponse, error) { return nil, err }
}

// followerRaft returns a Raft on a follower node. The follower holds no plan
// and knows a leader, so every query goes to the query func.
func followerRaft(t *testing.T, query func(*api.QueryRequest) (*api.QueryResponse, error)) *Raft {
	t.Helper()
	m := NewMockStore(t, "node-1", 0)
	followerTransport := attachFollowerRaft(t, m.store)
	leaderAddr, leaderTransport := raft.NewInmemTransport("leader")
	leaderTransport.Connect(followerTransport.LocalAddr(), followerTransport)
	require.NoError(t, leaderTransport.AppendEntries("node-1", followerTransport.LocalAddr(),
		&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("leader"), Addr: []byte(leaderAddr),
		}, Term: 1},
		&raft.AppendEntriesResponse{}))
	require.False(t, m.store.IsLeader())
	return NewRaft(mocks.NewMockNodeSelector(), m.store, queryForwardingClient{query: query})
}

func TestGetRebalancePlanFromLeader(t *testing.T) {
	t.Run("returns the leader's plan", func(t *testing.T) {
		leader := NewMockStore(t, "leader", 0)
		resp, ok := leader.store.Apply(&raft.Log{
			Index: 1,
			Type:  raft.LogCommand,
			Data: cmdAsBytes("", api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN, replication.CreatePlanRequest{
				PlanID:          "plan-1",
				SourceNodes:     []string{"node-1"},
				TargetNodes:     []string{"node-2"},
				CreatedAtUnixMs: 1000,
			}, nil),
		}).(Response)
		require.True(t, ok)
		require.NoError(t, resp.Error)
		want, ok := leader.store.replicationManager.GetReplicationFSM().GetPlan()
		require.True(t, ok)

		plan, err := followerRaft(t, toLeader(leader.store)).QueryRebalancePlan(context.Background())

		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, want, *plan)
	})

	t.Run("a leader without a plan is not found and no error", func(t *testing.T) {
		leader := NewMockStore(t, "leader", 0)

		plan, err := followerRaft(t, toLeader(leader.store)).QueryRebalancePlan(context.Background())

		require.NoError(t, err)
		assert.Nil(t, plan)
		assert.Zero(t, plan)
	})

	t.Run("any other error is returned", func(t *testing.T) {
		tests := []struct {
			name string
			err  error
		}{
			{"transport error mentioning not", status.Error(codes.Unavailable, "leader not reachable")},
			{"leader that does not know the query type", status.Error(codes.Internal,
				fmt.Errorf("%w type %s", types.ErrUnknownCommand, api.QueryRequest_TYPE_GET_REBALANCE_PLAN).Error())},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := followerRaft(t, failWith(tt.err)).QueryRebalancePlan(context.Background())

				require.ErrorIs(t, err, tt.err)
			})
		}
	})
}
