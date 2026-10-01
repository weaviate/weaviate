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
	"sync"
	"time"

	raftImpl "github.com/hashicorp/raft"
)

var (
	errNotTCP          = errors.New("local address is not a TCP address")
	errNotAdvertisable = errors.New("local bind address is not advertisable")
)

// tcpStreamLayer is raft's plain TCP stream layer, except that Close also
// aborts in-flight dials and closes every outbound connection. raft.Shutdown
// waits for its RPC goroutines, and without this an RPC to an unreachable
// peer holds shutdown for up to the transport timeout.
type tcpStreamLayer struct {
	advertise net.Addr
	listener  net.Listener

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	conns  map[*trackedConn]struct{}
}

func newTCPStreamLayer(bindAddr string, advertise net.Addr) (*tcpStreamLayer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &tcpStreamLayer{
		advertise: advertise,
		listener:  listener,
		ctx:       ctx,
		cancel:    cancel,
		conns:     map[*trackedConn]struct{}{},
	}

	addr, ok := s.Addr().(*net.TCPAddr)
	if !ok {
		s.Close()
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		s.Close()
		return nil, errNotAdvertisable
	}
	return s, nil
}

func (s *tcpStreamLayer) Dial(address raftImpl.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(s.ctx, "tcp", string(address))
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		conn.Close()
		return nil, net.ErrClosed
	}
	tc := &trackedConn{Conn: conn, layer: s}
	s.conns[tc] = struct{}{}
	return tc, nil
}

func (s *tcpStreamLayer) Accept() (net.Conn, error) {
	return s.listener.Accept()
}

func (s *tcpStreamLayer) Close() error {
	s.cancel()

	s.mu.Lock()
	s.closed = true
	conns := s.conns
	s.conns = nil
	s.mu.Unlock()

	for c := range conns {
		c.Conn.Close()
	}
	return s.listener.Close()
}

func (s *tcpStreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.listener.Addr()
}

type trackedConn struct {
	net.Conn
	layer *tcpStreamLayer
}

func (c *trackedConn) Close() error {
	c.layer.mu.Lock()
	delete(c.layer.conns, c)
	c.layer.mu.Unlock()
	return c.Conn.Close()
}
