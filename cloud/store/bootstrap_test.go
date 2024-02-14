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

package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBootStrapper(t *testing.T) {
	ctx := context.Background()
	anything := mock.Anything
	servers := []string{"S1", "S2"}

	tests := []struct {
		name     string
		voter    bool
		servers  []string
		doBefore func(*MockJoiner)
		success  bool
	}{
		{
			name:    "empty server list",
			voter:   true,
			servers: nil,
			doBefore: func(m *MockJoiner) {
				m.On("Join", anything, anything, anything).Return(&cmd.JoinPeerResponse{}, nil)
			},
			success: false,
		},
		{
			name:    "leader exist",
			voter:   true,
			servers: servers,
			doBefore: func(m *MockJoiner) {
				m.On("Join", anything, anything, anything).Return(&cmd.JoinPeerResponse{}, nil)
			},
			success: true,
		},
		{
			name:    "servers not available",
			voter:   true,
			servers: servers,
			doBefore: func(m *MockJoiner) {
				m.On("Join", anything, "S1", anything).Return(&cmd.JoinPeerResponse{}, errAny)
				m.On("Join", anything, "S2", anything).Return(&cmd.JoinPeerResponse{}, errAny)

				m.On("Notify", anything, "S1", anything).Return(&cmd.NotifyPeerResponse{}, nil)
				m.On("Notify", anything, "S2", anything).Return(&cmd.NotifyPeerResponse{}, errAny)
			},
			success: false,
		},
		{
			name:    "follow the leader",
			voter:   true,
			servers: servers,
			doBefore: func(m *MockJoiner) {
				err := status.Error(codes.NotFound, "follow the leader")
				m.On("Join", anything, "S1", anything).Return(&cmd.JoinPeerResponse{}, errAny)
				m.On("Join", anything, "S2", anything).Return(&cmd.JoinPeerResponse{Leader: "S3"}, err)
				m.On("Join", anything, "S3", anything).Return(&cmd.JoinPeerResponse{}, nil)
			},
			success: true,
		},
	}
	for _, test := range tests {
		m := &MockJoiner{}
		b := NewBootstrapper(m, "RID", "ADDR")
		b.retryPeriod = time.Millisecond
		b.jitter = time.Millisecond
		test.doBefore(m)
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		err := b.Do(ctx, test.servers, NewMockSLog(t).Logger, test.voter)
		cancel()
		if test.success && err != nil {
			t.Errorf("%s: %v", test.name, err)
		} else if !test.success && err == nil {
			t.Errorf("%s: test must fail", test.name)
		}
	}
}

type MockJoiner struct {
	mock.Mock
}

func (m *MockJoiner) Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	args := m.Called(ctx, leaderAddr, req)
	return args.Get(0).(*cmd.JoinPeerResponse), args.Error(1)
}

func (m *MockJoiner) Notify(ctx context.Context, leaderAddr string, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	args := m.Called(ctx, leaderAddr, req)
	return args.Get(0).(*cmd.NotifyPeerResponse), args.Error(1)
}
