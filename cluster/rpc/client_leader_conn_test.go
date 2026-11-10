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

package rpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type blockingLeader struct {
	cmd.UnimplementedClusterServiceServer
	started chan struct{}
	release chan struct{}
}

func (b *blockingLeader) wait() {
	b.started <- struct{}{}
	<-b.release
}

func (b *blockingLeader) Query(context.Context, *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	b.wait()
	return &cmd.QueryResponse{}, nil
}

func (b *blockingLeader) Apply(context.Context, *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	b.wait()
	return &cmd.ApplyResponse{}, nil
}

// mapResolver resolves raft addresses from a fixed map; unknown addresses fail.
type mapResolver map[string]string

func (m mapResolver) Address(raftAddr string) (string, error) {
	if a, ok := m[raftAddr]; ok {
		return a, nil
	}
	return "", errors.New("unknown node")
}

func startBlockingLeader(t *testing.T) (string, *blockingLeader) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	svc := &blockingLeader{started: make(chan struct{}, 4), release: make(chan struct{})}
	cmd.RegisterClusterServiceServer(srv, svc)
	enterrors.GoWrapper(func() { _ = srv.Serve(lis) }, logrus.New())
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), svc
}

func awaitStarted(t *testing.T, svc *blockingLeader) {
	t.Helper()
	select {
	case <-svc.started:
	case <-time.After(10 * time.Second):
		t.Fatal("rpc never reached the server")
	}
}

func awaitErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("rpc never returned")
		return nil
	}
}

func TestLeaderChangeDoesNotAbortInFlightRPC(t *testing.T) {
	tests := []struct {
		name string
		call func(cl *Client, leader string) error
	}{
		{"query", func(cl *Client, leader string) error {
			_, err := cl.Query(context.Background(), leader, &cmd.QueryRequest{})
			return err
		}},
		{"apply", func(cl *Client, leader string) error {
			_, err := cl.Apply(context.Background(), leader, &cmd.ApplyRequest{})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, svc := startBlockingLeader(t)
			cl := NewClient(mapResolver{"old:8300": addr, "new:8300": addr}, 1<<20, false, logrus.New())
			t.Cleanup(cl.Close)

			oldErr := make(chan error, 1)
			enterrors.GoWrapper(func() { oldErr <- tt.call(cl, "old:8300") }, logrus.New())
			awaitStarted(t, svc)
			oldConn := cl.leader.conn

			newErr := make(chan error, 1)
			enterrors.GoWrapper(func() { newErr <- tt.call(cl, "new:8300") }, logrus.New())
			awaitStarted(t, svc)
			assert.NotEqual(t, connectivity.Shutdown, oldConn.GetState(), "old conn closed under an in-flight rpc")

			close(svc.release)
			require.NoError(t, awaitErr(t, oldErr))
			require.NoError(t, awaitErr(t, newErr))
			assert.Eventually(t, func() bool { return oldConn.GetState() == connectivity.Shutdown },
				10*time.Second, 10*time.Millisecond, "the old conn must be closed once its rpcs return")
		})
	}
}

func TestLeaderChangeClosesIdleConn(t *testing.T) {
	addr, svc := startBlockingLeader(t)
	close(svc.release)
	cl := NewClient(mapResolver{"old:8300": addr, "new:8300": addr}, 1<<20, false, logrus.New())
	t.Cleanup(cl.Close)

	_, err := cl.Query(context.Background(), "old:8300", &cmd.QueryRequest{})
	require.NoError(t, err)
	<-svc.started
	oldConn := cl.leader.conn

	_, err = cl.Query(context.Background(), "new:8300", &cmd.QueryRequest{})
	require.NoError(t, err)
	<-svc.started
	assert.Eventually(t, func() bool { return oldConn.GetState() == connectivity.Shutdown },
		10*time.Second, 10*time.Millisecond)
	assert.NotSame(t, oldConn, cl.leader.conn)
}

// A failed dial to a new leader used to leave the closed old conn cached under
// the old address, so every later call to that leader failed with "the client
// connection is closing".
func TestFailedDialDoesNotCacheClosedConn(t *testing.T) {
	addr, svc := startBlockingLeader(t)
	close(svc.release)
	cl := NewClient(mapResolver{"old:8300": addr}, 1<<20, false, logrus.New())
	t.Cleanup(cl.Close)

	_, err := cl.Query(context.Background(), "old:8300", &cmd.QueryRequest{})
	require.NoError(t, err)
	<-svc.started

	_, err = cl.Query(context.Background(), "unresolvable:8300", &cmd.QueryRequest{})
	require.ErrorContains(t, err, "resolve address")

	_, err = cl.Query(context.Background(), "old:8300", &cmd.QueryRequest{})
	require.NoError(t, err)
	<-svc.started
}

func TestCloseWithInFlightRPC(t *testing.T) {
	addr, svc := startBlockingLeader(t)
	cl := NewClient(mapResolver{"leader:8300": addr}, 1<<20, false, logrus.New())

	errCh := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := cl.Query(context.Background(), "leader:8300", &cmd.QueryRequest{})
		errCh <- err
	}, logrus.New())
	awaitStarted(t, svc)
	conn := cl.leader.conn

	cl.Close()
	assert.Equal(t, connectivity.Shutdown, conn.GetState())
	require.Error(t, awaitErr(t, errCh), "close aborts in-flight rpcs")
	assert.Nil(t, cl.leader)
	close(svc.release)
}
