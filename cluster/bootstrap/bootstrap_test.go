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

package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	cmd "github.com/liutizhong/weaviate/cluster/proto/api"
	"github.com/liutizhong/weaviate/usecases/fakes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAny = errors.New("any error")

func TestBootstrapper(t *testing.T) {
	ctx := context.Background()
	anything := mock.Anything
	nodes := map[string]int{"S1": 1, "S2": 2}

	tests := []struct {
		name     string
		voter    bool
		nodes    map[string]int
		doBefore func(*MockNodeClient)
		isReady  func() bool
		success  bool
	}{
		{
			name:     "empty server list",
			voter:    true,
			nodes:    nil,
			doBefore: func(m *MockNodeClient) {},
			isReady:  func() bool { return false },
			success:  false,
		},
		{
			name:  "leader exist",
			voter: true,
			nodes: nodes,
			doBefore: func(m *MockNodeClient) {
				m.On("Join", anything, anything, anything).Return(&cmd.JoinPeerResponse{}, nil)
			},
			isReady: func() bool { return false },
			success: true,
		},
		{
			name:  "nodes not available",
			voter: true,
			nodes: nodes,
			doBefore: func(m *MockNodeClient) {
				m.On("Join", anything, "S1:1", anything).Return(&cmd.JoinPeerResponse{}, errAny)
				m.On("Join", anything, "S2:2", anything).Return(&cmd.JoinPeerResponse{}, errAny)

				m.On("Notify", anything, "S1:1", anything).Return(&cmd.NotifyPeerResponse{}, nil)
				m.On("Notify", anything, "S2:2", anything).Return(&cmd.NotifyPeerResponse{}, errAny)
			},
			isReady: func() bool { return false },
			success: false,
		},
		{
			name:  "follow the leader",
			voter: true,
			nodes: nodes,
			doBefore: func(m *MockNodeClient) {
				err := status.Error(codes.NotFound, "follow the leader")
				m.On("Join", anything, "S1:1", anything).Return(&cmd.JoinPeerResponse{}, errAny)
				m.On("Join", anything, "S2:2", anything).Return(&cmd.JoinPeerResponse{Leader: "S3"}, err)
				m.On("Join", anything, "S3", anything).Return(&cmd.JoinPeerResponse{}, nil)
			},
			isReady: func() bool { return false },
			success: true,
		},
		{
			name:     "exit early on cluster ready",
			voter:    true,
			nodes:    nodes,
			doBefore: func(m *MockNodeClient) {},
			isReady:  func() bool { return true },
			success:  true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			// Ensure the mocks are setup
			m := &MockNodeClient{}
			test.doBefore(m)

			// Configure the bootstrapper
			b := NewBootstrapper(m, "RID", "ADDR", test.voter, fakes.NewMockAddressResolver(func(id string) string { return id }), test.isReady)
			b.retryPeriod = time.Millisecond
			b.jitter = time.Millisecond
			ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
			logger, _ := logrustest.NewNullLogger()

			// Do the bootstrap
			err := b.Do(ctx, test.nodes, logger, make(chan struct{}))
			cancel()

			// Check all assertions
			if test.success && err != nil {
				t.Errorf("%s: %v", test.name, err)
			} else if !test.success && err == nil {
				t.Errorf("%s: test must fail", test.name)
			}
			m.AssertExpectations(t)
		})
	}
}

type MockNodeClient struct {
	mock.Mock
}

func (m *MockNodeClient) Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	args := m.Called(ctx, leaderAddr, req)
	return args.Get(0).(*cmd.JoinPeerResponse), args.Error(1)
}

func (m *MockNodeClient) Notify(ctx context.Context, leaderAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	args := m.Called(ctx, leaderAddr, req)
	return args.Get(0).(*cmd.NotifyPeerResponse), args.Error(1)
}
