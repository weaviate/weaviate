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

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// maxRaftFrameSize caps an inbound raft-message frame. Generous headroom over
// the 2MB MaxSizePerMsg; an oversized length is treated as a corrupt stream.
const maxRaftFrameSize = 64 * 1024 * 1024

// Transport sends raft messages to peer nodes for any group. Send is
// fire-and-forget: raft tolerates message loss and retries on the next tick.
// Each raftpb.Message carries its own To/From uint64 node IDs.
type Transport interface {
	Send(groupID uint64, msgs []raftpb.Message)
	Close() error
}

// MessageRouter hands an inbound raft message to the Store that owns the
// group. A transport is node-scoped and multiplexes every group, so it needs
// this indirection to fan messages out to per-group Stores. Implemented by
// the Registry.
type MessageRouter interface {
	RouteMessage(groupID uint64, msg raftpb.Message) error
}

// ShardAddressProvider resolves a string node ID to a host:port address for
// the shard RAFT transport layer.
type ShardAddressProvider struct {
	resolver          addressResolver
	raftPort          int
	isLocalCluster    bool
	nodeNameToPortMap map[string]int
}

// Resolve returns the host:port RAFT transport address for a node ID.
func (p *ShardAddressProvider) Resolve(nodeID string) (string, error) {
	addr := p.resolver.NodeAddress(nodeID)
	if addr == "" {
		return "", fmt.Errorf("could not resolve node %s", nodeID)
	}
	if !p.isLocalCluster {
		return fmt.Sprintf("%s:%d", addr, p.raftPort), nil
	}
	port, exists := p.nodeNameToPortMap[nodeID]
	if !exists {
		port = p.raftPort
	}
	return fmt.Sprintf("%s:%d", addr, port), nil
}

// peerConn is a single long-lived outbound yamux stream to one peer, carrying
// framed raft messages for every group. The mutex serialises concurrent Sends
// from multiple per-shard Ready loops.
type peerConn struct {
	mu     sync.Mutex
	stream net.Conn
}

// MuxTransport is a per-node singleton that manages a shared TCP listener and
// yamux session pool, multiplexing every shard's RAFT traffic. It implements
// Transport: outbound messages are framed (groupID, raftpb.Message) over a
// per-peer stream; inbound frames are demultiplexed to per-group Stores via
// the MessageRouter.
type MuxTransport struct {
	listener     net.Listener
	advertise    net.Addr
	addrProvider *ShardAddressProvider
	nodeIDs      *nodeIDMap
	router       MessageRouter
	logger       logrus.FieldLogger
	yamuxCfg     *yamux.Config

	sessions   map[string]*yamux.Session // peerAddr -> outbound session
	inbound    []*yamux.Session          // accepted server sessions
	sessionsMu sync.RWMutex              // guards sessions + inbound

	peers   map[uint64]*peerConn // peer uint64 nodeID -> outbound raft stream
	peersMu sync.RWMutex

	shutdownCh chan struct{}
	acceptDone chan struct{}  // closed when acceptLoop exits
	wg         sync.WaitGroup // handleSession + readStream goroutines
}

// NewMuxTransport creates a new multiplexed transport. It binds a TCP listener
// on bindAddr and starts an accept loop for incoming connections. router
// receives every inbound raft message; nodeIDs translates raft uint64 IDs back
// to string node IDs for address resolution.
func NewMuxTransport(
	bindAddr string,
	advertise net.Addr,
	provider *ShardAddressProvider,
	nodeIDs *nodeIDMap,
	router MessageRouter,
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
		nodeIDs:      nodeIDs,
		router:       router,
		logger:       logger,
		yamuxCfg:     yamuxCfg,
		sessions:     make(map[string]*yamux.Session),
		peers:        make(map[uint64]*peerConn),
		shutdownCh:   make(chan struct{}),
		acceptDone:   make(chan struct{}),
	}

	enterrors.GoWrapper(m.acceptLoop, logger)

	logger.WithFields(logrus.Fields{
		"bind":      bindAddr,
		"advertise": advertise.String(),
	}).Info("shard RAFT mux transport started")

	return m, nil
}

// acceptLoop accepts incoming TCP connections and wraps each in a yamux
// server session. It is not tracked by m.wg; Close waits on m.acceptDone so
// that no inbound session is registered after Close starts closing them.
func (m *MuxTransport) acceptLoop() {
	defer close(m.acceptDone)
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

		m.sessionsMu.Lock()
		m.inbound = append(m.inbound, session)
		m.sessionsMu.Unlock()

		m.wg.Add(1)
		enterrors.GoWrapper(func() {
			m.handleSession(session)
		}, m.logger)
	}
}

// handleSession accepts yamux streams from a session and spawns a reader for
// each. Every stream carries framed raft messages.
func (m *MuxTransport) handleSession(session *yamux.Session) {
	defer m.wg.Done()
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

		m.wg.Add(1)
		enterrors.GoWrapper(func() {
			m.readStream(stream)
		}, m.logger)
	}
}

// readStream decodes framed raft messages off one stream and routes each to
// the owning Store until the stream errors or closes.
func (m *MuxTransport) readStream(stream net.Conn) {
	defer m.wg.Done()
	defer stream.Close()

	var hdr [12]byte
	for {
		if _, err := io.ReadFull(stream, hdr[:]); err != nil {
			if err != io.EOF {
				m.logger.WithError(err).Debug("shard mux transport: read frame header")
			}
			return
		}
		groupID := binary.BigEndian.Uint64(hdr[:8])
		msgLen := binary.BigEndian.Uint32(hdr[8:12])
		if msgLen == 0 || msgLen > maxRaftFrameSize {
			m.logger.WithField("len", msgLen).Warn("shard mux transport: invalid frame length, closing stream")
			return
		}

		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(stream, buf); err != nil {
			m.logger.WithError(err).Debug("shard mux transport: read frame payload")
			return
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(buf); err != nil {
			m.logger.WithError(err).Warn("shard mux transport: unmarshal raft message")
			continue
		}
		if err := m.router.RouteMessage(groupID, msg); err != nil {
			m.logger.WithField("group", groupID).WithError(err).Warn("shard mux transport: route message")
		}
	}
}

// Send frames each raft message and writes it on the destination peer's
// stream. Fire-and-forget: unresolvable peers and write failures are dropped
// (raft re-sends on the next tick).
func (m *MuxTransport) Send(groupID uint64, msgs []raftpb.Message) {
	for i := range msgs {
		msg := msgs[i]
		pc := m.peerStream(msg.To)
		if pc == nil {
			continue
		}
		frame, err := encodeFrame(groupID, &msg)
		if err != nil {
			m.logger.WithError(err).Warn("shard mux transport: encode frame")
			continue
		}
		pc.mu.Lock()
		_, werr := pc.stream.Write(frame)
		pc.mu.Unlock()
		if werr != nil {
			m.logger.WithError(werr).WithField("to", msg.To).Debug("shard mux transport: write failed, dropping peer")
			m.dropPeer(msg.To)
		}
	}
}

// peerStream returns the outbound stream for a peer, dialing one if needed.
// Returns nil if the peer cannot be resolved or dialed.
func (m *MuxTransport) peerStream(to uint64) *peerConn {
	m.peersMu.RLock()
	pc, ok := m.peers[to]
	m.peersMu.RUnlock()
	if ok {
		return pc
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	if pc, ok := m.peers[to]; ok {
		return pc
	}

	nodeID, ok := m.nodeIDs.stringID(to)
	if !ok {
		m.logger.WithField("to", to).Warn("shard mux transport: unknown destination node ID")
		return nil
	}
	addr, err := m.addrProvider.Resolve(nodeID)
	if err != nil {
		m.logger.WithError(err).WithField("node", nodeID).Warn("shard mux transport: resolve peer address")
		return nil
	}
	session, err := m.getOrDialSession(addr)
	if err != nil {
		m.logger.WithError(err).WithField("addr", addr).Debug("shard mux transport: dial peer")
		return nil
	}
	stream, err := session.Open()
	if err != nil {
		m.logger.WithError(err).WithField("addr", addr).Debug("shard mux transport: open stream")
		return nil
	}
	pc = &peerConn{stream: stream}
	m.peers[to] = pc
	return pc
}

// dropPeer closes and forgets a peer's stream so the next Send re-dials.
func (m *MuxTransport) dropPeer(to uint64) {
	m.peersMu.Lock()
	pc, ok := m.peers[to]
	if ok {
		delete(m.peers, to)
	}
	m.peersMu.Unlock()
	if ok {
		pc.stream.Close()
	}
}

// getOrDialSession returns an existing outbound yamux session for the peer,
// or dials a new TCP connection and creates a yamux client session.
func (m *MuxTransport) getOrDialSession(addr string) (*yamux.Session, error) {
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

// Close shuts down the mux transport: stops the accept loop, closes every
// yamux session (inbound and outbound) and peer stream — which unblocks the
// handleSession/readStream goroutines — then waits for them to stop.
func (m *MuxTransport) Close() error {
	close(m.shutdownCh)

	if err := m.listener.Close(); err != nil {
		m.logger.WithError(err).Warn("shard mux transport: error closing listener")
	}

	// Wait for the accept loop to exit before closing inbound sessions, so no
	// session is registered after this point.
	<-m.acceptDone

	m.peersMu.Lock()
	for to, pc := range m.peers {
		pc.stream.Close()
		delete(m.peers, to)
	}
	m.peersMu.Unlock()

	m.sessionsMu.Lock()
	for _, session := range m.inbound {
		session.Close()
	}
	m.inbound = nil
	for addr, session := range m.sessions {
		if err := session.Close(); err != nil {
			m.logger.WithError(err).WithField("peer", addr).Debug("shard mux transport: error closing session")
		}
		delete(m.sessions, addr)
	}
	m.sessionsMu.Unlock()

	m.wg.Wait()

	m.logger.Info("shard RAFT mux transport closed")
	return nil
}

// encodeFrame builds a wire frame: [uint64 groupID BE][uint32 msgLen BE][msg].
func encodeFrame(groupID uint64, msg *raftpb.Message) ([]byte, error) {
	body, err := msg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal raft message: %w", err)
	}
	frame := make([]byte, 12+len(body))
	binary.BigEndian.PutUint64(frame[:8], groupID)
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(body)))
	copy(frame[12:], body)
	return frame, nil
}
