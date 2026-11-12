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
			followerTransport := attachFollowerRaft(t, m.store)

			// Send a heartbeat naming a leader at 127.0.0.1:1. The RPC client below
			// dials a freed port, so that leader never answers.
			leaderAddr, leaderTransport := raft.NewInmemTransport("127.0.0.1:1")
			leaderTransport.Connect(followerTransport.LocalAddr(), followerTransport)
			require.NoError(t, leaderTransport.AppendEntries("node-1", followerTransport.LocalAddr(),
				&raft.AppendEntriesRequest{RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax, ID: []byte("leader"), Addr: []byte(leaderAddr),
				}, Term: 1},
				&raft.AppendEntriesResponse{}))
			require.Equal(t, string(leaderAddr), m.store.Leader())

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

// The scheduler's lister reads this node's own copy, so it answers with no
// leader at all, and it refuses until the log held at store open is replayed.
func TestLocalTaskLister(t *testing.T) {
	for _, tt := range []struct {
		name            string
		replayedThrough uint64
		wantErr         bool
	}{
		{name: "log replayed", replayedThrough: 1},
		{name: "log still replaying", replayedThrough: 2, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "node-1", 0)
			sub, err := json.Marshal(&cmd.AddDistributedTaskRequest{Namespace: "ns", Id: "t", UnitIds: []string{"u"}})
			require.NoError(t, err)
			require.NoError(t, m.store.distributedTasksManager.AddTask(&cmd.ApplyRequest{SubCommand: sub}, 1))
			m.store.lastAppliedIndex.Store(1)
			m.store.lastAppliedIndexToDB.Store(tt.replayedThrough)

			tasks, err := NewRaft(mocks.NewMockNodeSelector(), m.store, nil).LocalTaskLister().
				ListDistributedTasks(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks["ns"], 1)
			require.Equal(t, "t", tasks["ns"][0].ID)
		})
	}
}
