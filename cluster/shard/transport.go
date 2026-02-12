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

package shard

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/log"
)

// ShardAddressProvider implements raft.ServerAddressProvider.
// It resolves a RAFT server ID (node name) to a host:port address
// for the shard RAFT transport layer.
type ShardAddressProvider struct {
	resolver         addressResolver
	raftPort         int
	isLocalCluster   bool
	nodeNameToPortMap map[string]int
}

// ServerAddr resolves a server ID to a RAFT transport address.
func (p *ShardAddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	addr := p.resolver.NodeAddress(string(id))
	if addr == "" {
		return "", fmt.Errorf("could not resolve node %s", id)
	}
	if !p.isLocalCluster {
		return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, p.raftPort)), nil
	}
	port, exists := p.nodeNameToPortMap[string(id)]
	if !exists {
		port = p.raftPort
	}
	return raft.ServerAddress(fmt.Sprintf("%s:%d", addr, port)), nil
}

// MuxTransport is a per-node singleton that manages a shared TCP listener
// and yamux session pool for multiplexing per-shard RAFT traffic.
// All shard RAFT clusters on a node share a bounded number of TCP connections
// (at most 2 per peer pair) via yamux stream multiplexing.
type MuxTransport struct {
	listener     net.Listener
	advertise    net.Addr
	addrProvider *ShardAddressProvider
	logger       logrus.FieldLogger
	yamuxCfg     *yamux.Config

	sessions   map[string]*yamux.Session // peerAddr -> outbound session
	sessionsMu sync.RWMutex

	shardLayers   map[string]*ShardStreamLayer // shardKey -> layer
	shardLayersMu sync.RWMutex

	shutdownCh chan struct{}
}

// NewMuxTransport creates a new multiplexed transport. It binds a TCP listener
// on bindAddr and starts an accept loop for incoming connections.
func NewMuxTransport(
	bindAddr string,
	advertise net.Addr,
	provider *ShardAddressProvider,
	logger logrus.FieldLogger,
) (*MuxTransport, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("bind shard raft transport on %s: %w", bindAddr, err)
	}

	yamuxCfg := yamux.DefaultConfig()
	yamuxCfg.AcceptBacklog = 1024
	yamuxCfg.ConnectionWriteTimeout = 10 * time.Second
	yamuxCfg.KeepAliveInterval = 15 * time.Second
	yamuxCfg.LogOutput = io.Discard

	m := &MuxTransport{
		listener:     ln,
		advertise:    advertise,
		addrProvider: provider,
		logger:       logger,
		yamuxCfg:     yamuxCfg,
		sessions:     make(map[string]*yamux.Session),
		shardLayers:  make(map[string]*ShardStreamLayer),
		shutdownCh:   make(chan struct{}),
	}

	go m.acceptLoop()

	logger.WithFields(logrus.Fields{
		"bind":      bindAddr,
		"advertise": advertise.String(),
	}).Info("shard RAFT mux transport started")

	return m, nil
}

// acceptLoop accepts incoming TCP connections and wraps each in a yamux
// server session, then dispatches streams to the appropriate shard layers.
func (m *MuxTransport) acceptLoop() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			select {
			case <-m.shutdownCh:
				return
			default:
			}
			m.logger.WithError(err).Warn("shard mux transport: accept error")
			continue
		}

		session, err := yamux.Server(conn, m.yamuxCfg)
		if err != nil {
			m.logger.WithError(err).Warn("shard mux transport: yamux server error")
			conn.Close()
			continue
		}

		go m.handleSession(session)
	}
}

// handleSession accepts yamux streams from a session and dispatches each
// to the appropriate ShardStreamLayer based on the shard key header.
func (m *MuxTransport) handleSession(session *yamux.Session) {
	for {
		stream, err := session.Accept()
		if err != nil {
			select {
			case <-m.shutdownCh:
				return
			default:
			}
			if !session.IsClosed() {
				m.logger.WithError(err).Debug("shard mux transport: stream accept error")
			}
			return
		}

		key, err := readShardKeyHeader(stream)
		if err != nil {
			m.logger.WithError(err).Warn("shard mux transport: read shard key header")
			stream.Close()
			continue
		}

		m.shardLayersMu.RLock()
		layer, ok := m.shardLayers[key]
		m.shardLayersMu.RUnlock()

		if !ok {
			m.logger.WithField("shard_key", key).Warn("shard mux transport: unknown shard key, closing stream")
			stream.Close()
			continue
		}

		// Non-blocking send to the shard layer's incoming channel.
		// If the buffer is full, close the stream — remote RAFT will retry.
		select {
		case layer.incomingCh <- stream:
		default:
			m.logger.WithField("shard_key", key).Warn("shard mux transport: incoming buffer full, dropping stream")
			stream.Close()
		}
	}
}

// getOrDialSession returns an existing outbound yamux session for the peer,
// or dials a new TCP connection and creates a yamux client session.
func (m *MuxTransport) getOrDialSession(address raft.ServerAddress) (*yamux.Session, error) {
	addr := string(address)

	m.sessionsMu.RLock()
	session, ok := m.sessions[addr]
	m.sessionsMu.RUnlock()

	if ok && !session.IsClosed() {
		return session, nil
	}

	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	// Double-check after acquiring write lock
	if session, ok = m.sessions[addr]; ok && !session.IsClosed() {
		return session, nil
	}

	// Dial new TCP connection
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", addr, err)
	}

	session, err = yamux.Client(conn, m.yamuxCfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("yamux client for %s: %w", addr, err)
	}

	m.sessions[addr] = session
	return session, nil
}

// CreateShardTransport creates a per-shard RAFT transport. It registers a
// ShardStreamLayer and wraps it in a raft.NetworkTransport.
func (m *MuxTransport) CreateShardTransport(
	className, shardName string,
	logger *logrus.Logger,
) (raft.Transport, error) {
	key := shardKey(className, shardName)

	layer := &ShardStreamLayer{
		shardKey:   key,
		mux:        m,
		advertise:  m.advertise,
		incomingCh: make(chan net.Conn, 64),
		closeCh:    make(chan struct{}),
	}

	m.shardLayersMu.Lock()
	m.shardLayers[key] = layer
	m.shardLayersMu.Unlock()

	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:                layer,
		MaxPool:               3,
		Timeout:               10 * time.Second,
		ServerAddressProvider: m.addrProvider,
		Logger:                log.NewHCLogrusLogger("shard-raft-net", logger),
	})

	return transport, nil
}

// DestroyShardTransport unregisters a shard's stream layer and closes it.
func (m *MuxTransport) DestroyShardTransport(className, shardName string) {
	key := shardKey(className, shardName)

	m.shardLayersMu.Lock()
	layer, ok := m.shardLayers[key]
	if ok {
		delete(m.shardLayers, key)
	}
	m.shardLayersMu.Unlock()

	if ok {
		layer.Close()
	}
}

// Close shuts down the mux transport: closes the listener, all yamux sessions,
// and signals all goroutines to stop.
func (m *MuxTransport) Close() error {
	close(m.shutdownCh)

	if err := m.listener.Close(); err != nil {
		m.logger.WithError(err).Warn("shard mux transport: error closing listener")
	}

	m.sessionsMu.Lock()
	for addr, session := range m.sessions {
		if err := session.Close(); err != nil {
			m.logger.WithError(err).WithField("peer", addr).Debug("shard mux transport: error closing session")
		}
		delete(m.sessions, addr)
	}
	m.sessionsMu.Unlock()

	m.shardLayersMu.Lock()
	for key, layer := range m.shardLayers {
		layer.Close()
		delete(m.shardLayers, key)
	}
	m.shardLayersMu.Unlock()

	m.logger.Info("shard RAFT mux transport closed")
	return nil
}

// ShardStreamLayer implements raft.StreamLayer for a single shard.
// It provides virtual per-shard Accept/Dial over shared yamux sessions.
type ShardStreamLayer struct {
	shardKey   string
	mux        *MuxTransport
	advertise  net.Addr
	incomingCh chan net.Conn // buffered, populated by MuxTransport.handleSession
	closeCh    chan struct{}
}

// Accept waits for and returns the next incoming connection for this shard.
func (s *ShardStreamLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-s.incomingCh:
		return conn, nil
	case <-s.closeCh:
		return nil, fmt.Errorf("shard stream layer closed")
	}
}

// Close closes the stream layer, causing Accept to return an error.
func (s *ShardStreamLayer) Close() error {
	select {
	case <-s.closeCh:
		// Already closed
	default:
		close(s.closeCh)
		// Drain and close any pending connections
		for {
			select {
			case conn := <-s.incomingCh:
				conn.Close()
			default:
				return nil
			}
		}
	}
	return nil
}

// Addr returns the advertise address for this stream layer.
func (s *ShardStreamLayer) Addr() net.Addr {
	return s.advertise
}

// Dial opens a new yamux stream to the target address and writes the shard
// key header so the remote MuxTransport can dispatch it correctly.
func (s *ShardStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	session, err := s.mux.getOrDialSession(address)
	if err != nil {
		return nil, err
	}

	stream, err := session.Open()
	if err != nil {
		return nil, fmt.Errorf("open yamux stream to %s: %w", address, err)
	}

	if err := writeShardKeyHeader(stream, s.shardKey); err != nil {
		stream.Close()
		return nil, fmt.Errorf("write shard key header to %s: %w", address, err)
	}

	return stream, nil
}

// writeShardKeyHeader writes a shard key header: [uint16 BE length][key bytes].
func writeShardKeyHeader(w io.Writer, key string) error {
	keyBytes := []byte(key)
	if len(keyBytes) > 65535 {
		return fmt.Errorf("shard key too long: %d bytes", len(keyBytes))
	}
	var hdr [2]byte
	binary.BigEndian.PutUint16(hdr[:], uint16(len(keyBytes)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(keyBytes)
	return err
}

// readShardKeyHeader reads a shard key header: [uint16 BE length][key bytes].
func readShardKeyHeader(r io.Reader) (string, error) {
	var hdr [2]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint16(hdr[:])
	if length == 0 {
		return "", fmt.Errorf("empty shard key")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// shardKey creates a composite key for identifying a shard's RAFT cluster.
func shardKey(className, shardName string) string {
	return className + "/" + shardName
}
