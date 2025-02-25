//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rpc

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const raftGrpcMessageMaxSize = 1024 * 1024 * 1024

func TestServerNewError(t *testing.T) {
	sm := monitoring.NewGRPCServerMetrics("rpc_test", prometheus.NewPedanticRegistry())

	var (
		addr      = fmt.Sprintf("localhost:%v", utils.MustGetFreeTCPPort())
		members   = &MockMembers{leader: addr}
		executor  = &MockExecutor{}
		logger, _ = logrustest.NewNullLogger()
	)

	t.Run("Empty server address", func(t *testing.T) {
		srv := NewServer(members, executor, "", raftGrpcMessageMaxSize, false, sm, logger)
		assert.NotNil(t, srv.Open())
	})

	t.Run("Invalid IP", func(t *testing.T) {
		srv := NewServer(members, executor, "abc", raftGrpcMessageMaxSize, false, sm, logger)
		netErr := &net.OpError{}
		assert.ErrorAs(t, srv.Open(), &netErr)
	})
}

func TestRaftRelatedRPC(t *testing.T) {
	sm := monitoring.NewGRPCServerMetrics("rpc_test", prometheus.NewPedanticRegistry())

	tests := []struct {
		name     string
		members  *MockMembers
		executor *MockExecutor
		testFunc func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor)
	}{
		{
			name:     "Join leader not found",
			members:  &MockMembers{errJoin: types.ErrLeaderNotFound},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				ctx := context.Background()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Join(ctx, leaderAddr, &cmd.JoinPeerRequest{Id: "Node1", Address: leaderAddr, Voter: false})
				assert.NotNil(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, st.Code(), codes.ResourceExhausted)
				assert.ErrorContains(t, st.Err(), types.ErrLeaderNotFound.Error())
			},
		},
		{
			name:     "Join success",
			members:  &MockMembers{},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				ctx := context.Background()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Join(ctx, leaderAddr, &cmd.JoinPeerRequest{Id: "Node1", Address: leaderAddr, Voter: false})
				assert.Nil(t, err)
			},
		},
		{
			name:     "Notify members error",
			members:  &MockMembers{errNotify: types.ErrNotOpen},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				ctx := context.Background()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Notify(ctx, leaderAddr, &cmd.NotifyPeerRequest{Id: "Node1", Address: leaderAddr})
				assert.NotNil(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, st.Code(), codes.Unavailable)
				assert.ErrorContains(t, st.Err(), types.ErrNotOpen.Error())
			},
		},
		{
			name:     "Notify success",
			members:  &MockMembers{},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Notify(ctx, leaderAddr, &cmd.NotifyPeerRequest{Id: "Node1", Address: leaderAddr})
				assert.Nil(t, err)
			},
		},
		{
			name:     "Remove members error",
			members:  &MockMembers{errRemove: types.ErrNotLeader},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Remove(ctx, leaderAddr, &cmd.RemovePeerRequest{Id: "node1"})
				assert.NotNil(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, st.Code(), codes.ResourceExhausted)
				assert.ErrorContains(t, st.Err(), types.ErrNotLeader.Error())
			},
		},
		{
			name:     "Remove success",
			members:  &MockMembers{},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Remove(ctx, leaderAddr, &cmd.RemovePeerRequest{Id: "node1"})
				assert.Nil(t, err)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			leaderAddr := fmt.Sprintf("localhost:%v", utils.MustGetFreeTCPPort())
			test.members.leader = leaderAddr
			test.testFunc(t, leaderAddr, test.members, test.executor)
		})
	}
}

func TestQueryEndpoint(t *testing.T) {
	sm := monitoring.NewGRPCServerMetrics("rpc_test", prometheus.NewPedanticRegistry())

	tests := []struct {
		name     string
		testFunc func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor)
	}{
		{
			name: "Query success",
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Query(ctx, leaderAddr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
				assert.Nil(t, err)
			},
		},
		{
			name: "Query verify retry mechanism on leader not found",
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				n := 0
				executor.qf = func(*cmd.QueryRequest) (*cmd.QueryResponse, error) {
					n++
					if n < 2 {
						return &cmd.QueryResponse{}, types.ErrLeaderNotFound
					}
					return &cmd.QueryResponse{}, nil
				}

				_, err := client.Query(ctx, leaderAddr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
				assert.Nil(t, err)
				assert.Greater(t, n, 1)
			},
		},
		{
			name: "Query leader not found",
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				ctx := context.Background()
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				executor.qf = func(*cmd.QueryRequest) (*cmd.QueryResponse, error) {
					return &cmd.QueryResponse{}, types.ErrLeaderNotFound
				}
				_, err := client.Query(ctx, leaderAddr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
				assert.NotNil(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, st.Code(), codes.ResourceExhausted)
				assert.ErrorContains(t, st.Err(), types.ErrLeaderNotFound.Error())
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			leaderAddr := fmt.Sprintf("localhost:%v", utils.MustGetFreeTCPPort())
			members := &MockMembers{
				leader: leaderAddr,
			}
			executor := &MockExecutor{
				qf: func(qr *cmd.QueryRequest) (*cmd.QueryResponse, error) { return nil, nil },
				ef: func() error { return nil },
			}
			test.testFunc(t, leaderAddr, members, executor)
		})
	}
}

func TestApply(t *testing.T) {
	sm := monitoring.NewGRPCServerMetrics("rpc_test", prometheus.NewPedanticRegistry())

	tests := []struct {
		name     string
		members  *MockMembers
		executor *MockExecutor
		testFunc func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor)
	}{
		{
			name:    "Apply error on leader not found",
			members: &MockMembers{},
			executor: &MockExecutor{
				ef: func() error {
					return types.ErrLeaderNotFound
				},
			},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Apply(context.TODO(), leaderAddr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "C"})
				assert.NotNil(t, err)
				st, ok := status.FromError(err)
				assert.True(t, ok)
				assert.Equal(t, st.Code(), codes.ResourceExhausted)
				assert.ErrorContains(t, st.Err(), types.ErrLeaderNotFound.Error())
			},
		},
		{
			name:     "Apply verify retry",
			members:  &MockMembers{},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				n := 0
				executor.ef = func() error {
					n++
					if n < 2 {
						return types.ErrLeaderNotFound
					}
					return nil
				}

				_, err := client.Apply(context.TODO(), leaderAddr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "C"})
				assert.Nil(t, err)
				assert.Greater(t, n, 1)
			},
		},
		{
			name:     "Apply success",
			members:  &MockMembers{},
			executor: &MockExecutor{},
			testFunc: func(t *testing.T, leaderAddr string, members *MockMembers, executor *MockExecutor) {
				// Setup var, client and server
				logger, _ := logrustest.NewNullLogger()
				server := NewServer(members, executor, leaderAddr, raftGrpcMessageMaxSize, false, sm, logger)
				assert.Nil(t, server.Open())
				defer server.Close()
				client := NewClient(fakes.NewFakeRPCAddressResolver(leaderAddr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				defer client.Close()

				_, err := client.Apply(context.TODO(), leaderAddr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "C"})
				assert.Nil(t, err)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			leaderAddr := fmt.Sprintf("localhost:%v", utils.MustGetFreeTCPPort())
			test.members.leader = leaderAddr
			test.testFunc(t, leaderAddr, test.members, test.executor)
		})
	}
}

type MockMembers struct {
	leader    string
	errJoin   error
	errNotify error
	errRemove error
}

func (m *MockMembers) Join(id string, addr string, voter bool) error {
	return m.errJoin
}

func (m *MockMembers) Notify(id string, addr string) error {
	return m.errNotify
}

func (m *MockMembers) Remove(id string) error {
	return m.errRemove
}

func (m *MockMembers) Leader() string {
	return m.leader
}

type MockExecutor struct {
	ef func() error
	qf func(*cmd.QueryRequest) (*cmd.QueryResponse, error)
}

func (m *MockExecutor) Execute(_ context.Context, cmd *cmd.ApplyRequest) (uint64, error) {
	if m.ef != nil {
		return 0, m.ef()
	}
	return 0, nil
}

func (m *MockExecutor) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	if m.qf != nil {
		return m.qf(req)
	}
	return &cmd.QueryResponse{}, nil
}
