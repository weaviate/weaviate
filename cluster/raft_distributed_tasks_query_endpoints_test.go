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
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/resolver"
	"github.com/weaviate/weaviate/cluster/rpc"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

func TestListDistributedTasksUnreachableLeader(t *testing.T) {
	tests := []struct {
		name            string
		consistencyWait time.Duration
		callerCancel    time.Duration
		wantCode        codes.Code
	}{
		{name: "caller without deadline", consistencyWait: 100 * time.Millisecond, wantCode: codes.DeadlineExceeded},
		{name: "caller without deadline cancels first", consistencyWait: time.Hour, callerCancel: 100 * time.Millisecond, wantCode: codes.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			m.store.cfg.ConsistencyWaitTimeout = tt.consistencyWait
			// The RPC client below dials a freed port, so this leader never answers.
			heartbeatFromLeader(t, m.store)

			cl := rpc.NewClient(resolver.NewRpc(false, utils.MustGetFreeTCPPort()), 1<<20, false, m.logger)
			t.Cleanup(cl.Close)
			srv := NewRaft(mocks.NewMockNodeSelector(), m.store, cl)

			ctx := context.Background()
			if tt.callerCancel > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(tt.callerCancel, cancel)
			}
			_, err := srv.ListDistributedTasks(ctx)
			require.Equal(t, tt.wantCode, status.Code(err))
		})
	}
}

// heartbeatFromLeader makes store a follower of a leader at 127.0.0.1:1.
func heartbeatFromLeader(t *testing.T, store *Store) {
	t.Helper()
	followerTransport := attachFollowerRaft(t, store)
	leaderAddr, leaderTransport := raft.NewInmemTransport("127.0.0.1:1")
	leaderTransport.Connect(followerTransport.LocalAddr(), followerTransport)
	require.NoError(t, leaderTransport.AppendEntries("node-1", followerTransport.LocalAddr(),
		&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{
			ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("leader"), Addr: []byte(leaderAddr),
		}, Term: 1},
		&raft.AppendEntriesResponse{}))
	require.Equal(t, string(leaderAddr), store.Leader())
}

// The scheduler's lister reads this node's own copy, but refuses while no
// leader is known so the tick is skipped as it was with the leader's list.
func TestLocalTaskLister(t *testing.T) {
	for _, tt := range []struct {
		name   string
		leader bool
	}{
		{name: "leader known", leader: true},
		{name: "no leader"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			sub, err := json.Marshal(&cmd.AddDistributedTaskRequest{Namespace: "ns", Id: "t", UnitIds: []string{"u"}})
			require.NoError(t, err)
			require.NoError(t, m.store.distributedTasksManager.AddTask(&cmd.ApplyRequest{SubCommand: sub}, 1))
			if tt.leader {
				heartbeatFromLeader(t, m.store)
			} else {
				attachFollowerRaft(t, m.store)
				require.Empty(t, m.store.Leader())
			}

			tasks, err := NewRaft(mocks.NewMockNodeSelector(), m.store, nil).LocalTaskLister().
				ListDistributedTasks(context.Background())
			if !tt.leader {
				require.ErrorIs(t, err, types.ErrLeaderNotFound)
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks["ns"], 1)
			require.Equal(t, "t", tasks["ns"][0].ID)
		})
	}
}
