//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rpc

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

const fourMiB = 4 * 1024 * 1024

type testResolver struct{}

func (r *testResolver) Address(raftAddress string) (string, error) { return raftAddress, nil }

type testServer struct {
	cmd.UnimplementedClusterServiceServer
	joinAttempts   int32
	notifyAttempts int32
	removeAttempts int32
	applyAttempts  int32
	queryAttempts  int32

	joinFailFor   int32
	notifyFailFor int32
	removeFailFor int32
	applyFailFor  int32
	queryFailFor  int32
}

func TestRPC_RetryPolicy_Succeeds(t *testing.T) {
	srv := &testServer{joinFailFor: 2, notifyFailFor: 2, removeFailFor: 2, applyFailFor: 2, queryFailFor: 2}
	addr, stop := startTestServer(t, srv)
	defer stop()

	// Sanity-check client policy by dialing directly as well
	raw := newRawConn(t, addr)
	defer raw.Close()

	// Use the rpc.Client which also applies serviceConfig via getConn
	cl := NewClient(&testResolver{}, fourMiB, false, nil)

	type testCase struct {
		name        string
		invoke      func(ctx context.Context) error
		got         func() int32
		wantAtLeast int32
	}

	cases := []testCase{
		{
			name: "JoinPeer retries and succeeds",
			invoke: func(ctx context.Context) error {
				_, err := cl.Join(ctx, addr, &cmd.JoinPeerRequest{})
				return err
			},
			got:         func() int32 { return atomic.LoadInt32(&srv.joinAttempts) },
			wantAtLeast: 3,
		},
		{
			name: "NotifyPeer retries and succeeds",
			invoke: func(ctx context.Context) error {
				_, err := cl.Notify(ctx, addr, &cmd.NotifyPeerRequest{})
				return err
			},
			got:         func() int32 { return atomic.LoadInt32(&srv.notifyAttempts) },
			wantAtLeast: 3,
		},
		{
			name: "RemovePeer retries and succeeds",
			invoke: func(ctx context.Context) error {
				_, err := cl.Remove(ctx, addr, &cmd.RemovePeerRequest{})
				return err
			},
			got:         func() int32 { return atomic.LoadInt32(&srv.removeAttempts) },
			wantAtLeast: 3,
		},
		{
			name: "Apply retries and succeeds",
			invoke: func(ctx context.Context) error {
				_, err := cl.Apply(ctx, addr, &cmd.ApplyRequest{})
				return err
			},
			got:         func() int32 { return atomic.LoadInt32(&srv.applyAttempts) },
			wantAtLeast: 3,
		},
		{
			name: "Query retries and succeeds",
			invoke: func(ctx context.Context) error {
				_, err := cl.Query(ctx, addr, &cmd.QueryRequest{})
				return err
			},
			got:         func() int32 { return atomic.LoadInt32(&srv.queryAttempts) },
			wantAtLeast: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := tc.invoke(ctx); err != nil {
				t.Fatalf("call failed: %v", err)
			}
			if got := tc.got(); got < tc.wantAtLeast {
				t.Fatalf("expected at least %d attempts, got %d", tc.wantAtLeast, got)
			}
		})
	}
}

func startTestServer(t *testing.T, svc cmd.ClusterServiceServer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	cmd.RegisterClusterServiceServer(s, svc)
	go func() { _ = s.Serve(lis) }()
	return lis.Addr().String(), func() {
		s.GracefulStop()
		_ = lis.Close()
	}
}

func newRawConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func TestJoinPeer_ExhaustsRetries(t *testing.T) {
	srv := &testServer{joinFailFor: 10}
	addr, stop := startTestServer(t, srv)
	defer stop()

	cl := NewClient(&testResolver{}, fourMiB, false, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	_, err := cl.Join(ctx, addr, &cmd.JoinPeerRequest{})

	// Should fail after exhausting retries
	assert.Error(t, err, "expected error after exhausting retries")

	// With MaxAttempts=3, we should see at least 3 attempts
	attempts := atomic.LoadInt32(&srv.joinAttempts)
	assert.GreaterOrEqual(t, attempts, int32(3), "expected at least 3 attempts, got %d", attempts)
}

func (s *testServer) JoinPeer(ctx context.Context, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	n := atomic.AddInt32(&s.joinAttempts, 1)
	if n <= s.joinFailFor {
		return nil, status.Error(codes.Unavailable, "temporary unavailable")
	}
	return &cmd.JoinPeerResponse{}, nil
}

func (s *testServer) NotifyPeer(ctx context.Context, req *cmd.NotifyPeerRequest) (*cmd.NotifyPeerResponse, error) {
	n := atomic.AddInt32(&s.notifyAttempts, 1)
	if n <= s.notifyFailFor {
		return nil, status.Error(codes.Unavailable, "temporary unavailable")
	}
	return &cmd.NotifyPeerResponse{}, nil
}

func (s *testServer) RemovePeer(ctx context.Context, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	n := atomic.AddInt32(&s.removeAttempts, 1)
	if n <= s.removeFailFor {
		return nil, status.Error(codes.Unavailable, "temporary unavailable")
	}
	return &cmd.RemovePeerResponse{}, nil
}

func (s *testServer) Apply(ctx context.Context, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	n := atomic.AddInt32(&s.applyAttempts, 1)
	if n <= s.applyFailFor {
		return nil, status.Error(codes.Unavailable, "temporary unavailable")
	}
	return &cmd.ApplyResponse{}, nil
}

func (s *testServer) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	n := atomic.AddInt32(&s.queryAttempts, 1)
	if n <= s.queryFailFor {
		return nil, status.Error(codes.Unavailable, "temporary unavailable")
	}
	return &cmd.QueryResponse{}, nil
}
