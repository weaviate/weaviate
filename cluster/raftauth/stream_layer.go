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

package raftauth

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/cluster"
)

const (
	// MaxHandshakeTimeout caps how long a peer may take to complete the handshake.
	MaxHandshakeTimeout = 10 * time.Second
	// LegacyTTL is how long a peer that does not speak the handshake is dialed without it before it is probed again.
	LegacyTTL = time.Minute

	serverReadBufferSize = 128
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

// Config configures a StreamLayer.
type Config struct {
	BindAddr  string
	Advertise net.Addr
	Auth      cluster.BasicAuth
	// HandshakeTimeout bounds the handshake on both sides; zero or above MaxHandshakeTimeout means MaxHandshakeTimeout.
	HandshakeTimeout time.Duration
	Logger           logrus.FieldLogger
}

// StreamLayer is a raft.StreamLayer over TCP that authenticates every
// connection with the cluster basic auth credentials (see the package doc).
//
// Accept never blocks on a peer: the server side of the handshake runs on the
// first Read or Write of the returned conn, which is raft's per-connection
// goroutine. Once the handshake is done reads and writes pass straight through.
type StreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
	mode      Mode
	key       []byte
	timeout   time.Duration
	logger    logrus.FieldLogger
	now       func() time.Time

	legacyMu sync.Mutex
	legacy   map[string]time.Time
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// NewStreamLayer listens on cfg.BindAddr and applies the same advertise
// address checks as raft's TCP transport.
func NewStreamLayer(cfg Config) (*StreamLayer, error) {
	timeout := cfg.HandshakeTimeout
	if timeout <= 0 || timeout > MaxHandshakeTimeout {
		timeout = MaxHandshakeTimeout
	}
	list, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		return nil, err
	}
	s := &StreamLayer{
		advertise: cfg.Advertise,
		listener:  list.(*net.TCPListener),
		mode:      ModeFrom(cfg.Auth),
		key:       deriveKey(cfg.Auth),
		timeout:   timeout,
		logger:    cfg.Logger,
		now:       time.Now,
		legacy:    map[string]time.Time{},
	}
	addr, ok := s.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errors.Join(errNotTCP, list.Close())
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		return nil, errors.Join(errNotAdvertisable, list.Close())
	}
	return s, nil
}

// Mode returns the effective authentication mode.
func (s *StreamLayer) Mode() Mode {
	return s.mode
}

// Accept implements net.Listener.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &serverConn{Conn: conn, layer: s}, nil
}

// Close implements net.Listener.
func (s *StreamLayer) Close() error {
	return s.listener.Close()
}

// Addr implements net.Listener, returning the advertise address when set.
func (s *StreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.listener.Addr()
}

// Dial implements raft.StreamLayer. With credentials it runs the handshake
// before returning; it falls back to plain raft for a legacy peer only when
// this node does not verify, so a verifying node never sends the log to an
// unauthenticated server.
func (s *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	addr := string(address)
	if !s.mode.Sends() || (!s.mode.Verifies() && s.isLegacy(addr)) {
		return net.DialTimeout("tcp", addr, timeout)
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	hsTimeout := timeout
	if hsTimeout <= 0 || hsTimeout > s.timeout {
		hsTimeout = s.timeout
	}
	mismatch, err := s.runClientHandshake(conn, hsTimeout)
	if err == nil {
		if mismatch {
			s.logger.WithField("peer", addr).Warn("raft auth: peer credentials mismatch, check CLUSTER_BASIC_AUTH_* on every node before turning verification on")
		}
		return conn, nil
	}
	if closeErr := conn.Close(); closeErr != nil {
		s.logger.WithField("peer", addr).Debugf("raft auth: close after failed handshake: %v", closeErr)
	}
	if s.mode.Verifies() || !errors.Is(err, errLegacyPeer) {
		return nil, err
	}
	s.markLegacy(addr)
	s.logger.WithField("peer", addr).Infof("raft auth: peer does not support the raft auth handshake, using plain raft for %s", LegacyTTL)
	return net.DialTimeout("tcp", addr, timeout)
}

func (s *StreamLayer) runClientHandshake(conn net.Conn, timeout time.Duration) (bool, error) {
	if err := conn.SetDeadline(s.now().Add(timeout)); err != nil {
		return false, err
	}
	mismatch, err := clientHandshake(conn, s.key, s.mode.Verifies())
	if err != nil {
		return false, err
	}
	return mismatch, conn.SetDeadline(time.Time{})
}

func (s *StreamLayer) isLegacy(addr string) bool {
	s.legacyMu.Lock()
	defer s.legacyMu.Unlock()
	exp, ok := s.legacy[addr]
	if !ok {
		return false
	}
	if s.now().After(exp) {
		delete(s.legacy, addr)
		return false
	}
	return true
}

func (s *StreamLayer) markLegacy(addr string) {
	s.legacyMu.Lock()
	defer s.legacyMu.Unlock()
	now := s.now()
	for a, exp := range s.legacy {
		if now.After(exp) {
			delete(s.legacy, a)
		}
	}
	s.legacy[addr] = now.Add(LegacyTTL)
}

// serverConn runs the server side of the handshake on its first Read or Write.
// The handshake replaces any earlier deadline; raft sets none on inbound conns.
type serverConn struct {
	net.Conn
	layer *StreamLayer

	once sync.Once
	err  error
	// br holds bytes read past the handshake; nil once drained.
	br *bufio.Reader
}

func (c *serverConn) Read(p []byte) (int, error) {
	c.once.Do(c.handshake)
	if c.err != nil {
		return 0, c.err
	}
	if c.br != nil {
		if c.br.Buffered() > 0 {
			return c.br.Read(p)
		}
		c.br = nil
	}
	return c.Conn.Read(p)
}

func (c *serverConn) Write(p []byte) (int, error) {
	c.once.Do(c.handshake)
	if c.err != nil {
		return 0, c.err
	}
	return c.Conn.Write(p)
}

func (c *serverConn) handshake() {
	s := c.layer
	if err := c.SetDeadline(s.now().Add(s.timeout)); err != nil {
		c.fail("set handshake deadline", err)
		return
	}
	c.br = bufio.NewReaderSize(c.Conn, serverReadBufferSize)
	first, err := c.br.Peek(1)
	if err != nil {
		if errors.Is(err, io.EOF) {
			c.err = io.EOF
			return
		}
		c.fail("read first byte", err)
		return
	}
	if first[0] != magic {
		if s.mode.Verifies() {
			c.fail("rejected peer", errUnauthenticated)
			return
		}
	} else {
		mismatch, err := serverHandshake(c.br, c.Conn, s.key, s.mode.Verifies())
		if err != nil {
			c.fail("handshake", err)
			return
		}
		if mismatch {
			s.logger.WithField("remote_addr", c.Conn.RemoteAddr().String()).
				Warn("raft auth: peer credentials mismatch, check CLUSTER_BASIC_AUTH_* on every node before turning verification on")
		}
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		c.fail("clear handshake deadline", err)
	}
}

// fail logs the reason and makes raft drop the connection quietly with io.EOF.
func (c *serverConn) fail(msg string, err error) {
	c.layer.logger.WithField("remote_addr", c.Conn.RemoteAddr().String()).
		Warnf("raft auth: %s: %v", msg, err)
	c.err = io.EOF
}
