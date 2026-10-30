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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// queryErrClient answers every forwarded Query with err. Its other methods
// are nil and panic if called.
type queryErrClient struct {
	client
	err error
}

func (c *queryErrClient) Query(context.Context, string, *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	return &cmd.QueryResponse{}, c.err
}

// attachFollowerWithLeader gives store a real raft instance that is a
// nonvoter following a separate single-voter leader, so Raft.Query forwards
// to the leader through the rpc client.
func attachFollowerWithLeader(t *testing.T, store *Store) {
	t.Helper()
	leaderAddr, leaderTrans := raft.NewInmemTransport("leader")
	followerAddr, followerTrans := raft.NewInmemTransport("follower")
	leaderTrans.Connect(followerAddr, followerTrans)
	followerTrans.Connect(leaderAddr, leaderTrans)

	newNode := func(id raft.ServerID, fsm raft.FSM, trans raft.Transport) *raft.Raft {
		cfg := raft.DefaultConfig()
		cfg.LocalID = id
		cfg.Logger = hclog.NewNullLogger()
		cfg.HeartbeatTimeout = 50 * time.Millisecond
		cfg.ElectionTimeout = 50 * time.Millisecond
		cfg.LeaderLeaseTimeout = 50 * time.Millisecond
		inmem := raft.NewInmemStore()
		r, err := raft.NewRaft(cfg, fsm, inmem, inmem, raft.NewInmemSnapshotStore(), trans)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, r.Shutdown().Error()) })
		return r
	}

	leader := newNode("leader", &raft.MockFSM{}, leaderTrans)
	require.NoError(t, leader.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
		{Suffrage: raft.Voter, ID: "leader", Address: leaderAddr},
		{Suffrage: raft.Nonvoter, ID: "follower", Address: followerAddr},
	}}).Error())
	store.raft.Store(newNode("follower", store, followerTrans))

	require.True(t, tryNTimesWithWait(50, 100*time.Millisecond, func() bool {
		return store.Leader() == string(leaderAddr)
	}), "follower never learned the leader")
}

func TestGetDistributedTask(t *testing.T) {
	m := NewMockStore(t, "node-1", 0)
	attachFollowerWithLeader(t, m.store)
	cl := &queryErrClient{}
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, cl)

	// A pre-v1.40 leader has no case for TYPE_DISTRIBUTED_TASK_GET and prints
	// the type as a number. The text is what its rpc server sends; the join is
	// what this node's rpc client rebuilds for codes.Unimplemented.
	unknownCommand := errors.Join(
		status.Error(codes.Unimplemented, "unknown command type 301: consider upgrading to newer version"),
		types.ErrUnknownCommand)
	leaderFault := status.Error(codes.Internal, "leader fault")
	// The leader-local query error keeps the sentinel in its chain.
	notExistOnLeader := fmt.Errorf("could not get distributed task: %w", distributedtask.ErrTaskDoesNotExist)
	// The same error after the gRPC hop. The leader's rpc server sends only
	// the text of store_query.go's wrap around wrapPermanent. This node's rpc
	// client passes the FailedPrecondition status through, so the sentinel is
	// gone.
	notExistFlattened := status.Error(distributedtask.PermanentRejectionRPCCode,
		"could not get distributed task: [dtm-perm/task-not-exist] task ns/task-1 does not exist: task does not exist\npermanent FSM rejection")

	tests := []struct {
		name     string
		queryErr error
		wantErr  error
	}{
		{name: "a leader that does not know the query type propagates as an error", queryErr: unknownCommand, wantErr: types.ErrUnknownCommand},
		{name: "other query errors propagate", queryErr: leaderFault, wantErr: leaderFault},
		{name: "a missing task returns no task and no error", queryErr: notExistOnLeader},
		{name: "a missing task flattened by the gRPC hop returns no task and no error", queryErr: notExistFlattened},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl.err = tt.queryErr
			task, err := srv.GetDistributedTask(context.Background(), "ns", "task-1")
			assert.Nil(t, task)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
