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

package resolver

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

const (
	transportTimeout = 10 * time.Second
	promptly         = 2 * time.Second
)

func newTestTransport(t *testing.T) *raftImpl.NetworkTransport {
	t.Helper()
	logger, _ := test.NewNullLogger()
	advertise := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	r := &raft{ClusterStateReader: ipv6NodeSelector{}}
	trans, err := r.NewTCPTransport("127.0.0.1:0", advertise, 2, transportTimeout, logger)
	require.NoError(t, err)
	t.Cleanup(func() { trans.Close() })
	return trans
}

// silentPeer accepts connections and never answers, like a peer that is
// partitioned after the connection was established.
func silentPeer(t *testing.T) (raftImpl.ServerAddress, <-chan struct{}) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	accepted := make(chan struct{}, 16)
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
			accepted <- struct{}{}
		}
	}()
	t.Cleanup(func() {
		l.Close()
		<-done
		for _, c := range conns {
			c.Close()
		}
	})
	return raftImpl.ServerAddress(l.Addr().String()), accepted
}

func TestTransportCloseUnblocksRPCs(t *testing.T) {
	tests := []struct {
		name string
		rpc  func(trans *raftImpl.NetworkTransport, target raftImpl.ServerAddress) error
	}{
		{
			name: "request vote",
			rpc: func(trans *raftImpl.NetworkTransport, target raftImpl.ServerAddress) error {
				return trans.RequestVote("peer", target, &raftImpl.RequestVoteRequest{}, &raftImpl.RequestVoteResponse{})
			},
		},
		{
			name: "request pre-vote",
			rpc: func(trans *raftImpl.NetworkTransport, target raftImpl.ServerAddress) error {
				return trans.RequestPreVote("peer", target, &raftImpl.RequestPreVoteRequest{}, &raftImpl.RequestPreVoteResponse{})
			},
		},
		{
			name: "append entries",
			rpc: func(trans *raftImpl.NetworkTransport, target raftImpl.ServerAddress) error {
				return trans.AppendEntries("peer", target, &raftImpl.AppendEntriesRequest{}, &raftImpl.AppendEntriesResponse{})
			},
		},
		{
			name: "timeout now",
			rpc: func(trans *raftImpl.NetworkTransport, target raftImpl.ServerAddress) error {
				return trans.TimeoutNow("peer", target, &raftImpl.TimeoutNowRequest{}, &raftImpl.TimeoutNowResponse{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trans := newTestTransport(t)
			target, accepted := silentPeer(t)

			errCh := make(chan error, 1)
			go func() { errCh <- tt.rpc(trans, target) }()

			select {
			case <-accepted:
			case err := <-errCh:
				t.Fatalf("rpc returned before reaching the peer: %v", err)
			case <-time.After(promptly):
				t.Fatal("peer never accepted the connection")
			}

			start := time.Now()
			require.NoError(t, trans.Close())
			select {
			case err := <-errCh:
				require.Error(t, err)
				require.Less(t, time.Since(start), promptly)
			case <-time.After(promptly):
				t.Fatal("rpc still blocked after the transport was closed")
			}
		})
	}
}

func TestTransportCloseAbortsInFlightDial(t *testing.T) {
	trans := newTestTransport(t)
	// TEST-NET-1 is not routable, so a dial normally hangs until the timeout.
	target := raftImpl.ServerAddress("192.0.2.1:8300")

	errCh := make(chan error, 1)
	go func() {
		errCh <- trans.RequestVote("peer", target, &raftImpl.RequestVoteRequest{}, &raftImpl.RequestVoteResponse{})
	}()

	select {
	case err := <-errCh:
		t.Skipf("network rejected the dial before close: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	start := time.Now()
	require.NoError(t, trans.Close())
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(start), promptly)
	case <-time.After(promptly):
		t.Fatal("dial still in flight after the transport was closed")
	}
}

func TestStreamLayerDialAfterClose(t *testing.T) {
	target, _ := silentPeer(t)
	s, err := newTCPStreamLayer("127.0.0.1:0", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	require.NoError(t, s.Close())

	_, err = s.Dial(target, transportTimeout)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed), "got %v", err)
}

func TestStreamLayerForgetsClosedConns(t *testing.T) {
	target, _ := silentPeer(t)
	s, err := newTCPStreamLayer("127.0.0.1:0", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	for range 3 {
		c, err := s.Dial(target, transportTimeout)
		require.NoError(t, err)
		require.NoError(t, c.Close())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Empty(t, s.conns)
}

func TestNewTCPStreamLayerRejectsUnadvertisableAddress(t *testing.T) {
	tests := []struct {
		name      string
		advertise net.Addr
		wantErr   error
	}{
		{name: "unspecified bind without advertise", advertise: nil, wantErr: errNotAdvertisable},
		{name: "unspecified advertise", advertise: &net.TCPAddr{IP: net.IPv4zero}, wantErr: errNotAdvertisable},
		{name: "non-tcp advertise", advertise: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, wantErr: errNotTCP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTCPStreamLayer("0.0.0.0:0", tt.advertise)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
