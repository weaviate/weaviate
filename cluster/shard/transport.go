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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// frameHeaderLen is the wire-frame prefix: [uint64 groupID BE][uint32 msgLen BE].
const frameHeaderLen = 12

// maxRaftFrameSize caps an inbound raft-message frame. Generous headroom over
// the 2MB MaxSizePerMsg; an oversized length is treated as a corrupt stream.
const maxRaftFrameSize = 64 * 1024 * 1024

// defaultHeartbeatFlushInterval is the coalescer flush cadence used when
// NewMuxTransport is given no explicit interval; it matches the Store tick rate.
const defaultHeartbeatFlushInterval = 100 * time.Millisecond

// Transport sends raft messages to peer nodes for any group. Send is
// fire-and-forget AND non-blocking: it enqueues onto per-peer sender lanes
// and returns; a slow or dead peer surfaces as counted drops, never as a
// blocked caller — raft tolerates message loss and retries on the next tick.
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

// Sender-lane bounds. Named constants on purpose — the values derive from
// raft's own flow-control configuration, not from tuning.
const (
	// bulkStripeMaxBytes bounds the bytes queued on one bulk stripe — one
	// (group, peer) pair. It equals defaultMaxInflightBytes (32MB), raft's
	// per-follower per-group cap on unacked append bytes, so the bound is
	// exact: the stripe's one group can never overflow it on its own (raft
	// throttles it first). Overflow drops indicate raft misbehaving or the
	// peer wedged — recovered by raft's probe/retry machinery.
	bulkStripeMaxBytes = defaultMaxInflightBytes

	// bulkStripeMaxMsgs caps queue slice growth when the traffic is tiny
	// messages (MsgAppResp is ~100 bytes and never nears the byte bound):
	// 4x defaultMaxInflightMsgs (256), headroom for response bursts and
	// probe churn — and more per-group capacity than the pre-stripe 4096
	// shared across every group on the peer.
	bulkStripeMaxMsgs = 1024

	// prioLaneMaxMsgs bounds the priority lane. Its traffic is tiny and
	// low-rate — the coalescer contributes at most one frame per peer per
	// flush interval, votes are rare — so 1024 queued frames is ~100s of
	// backlog, far past usefulness; a byte bound would never be the binding
	// constraint.
	prioLaneMaxMsgs = 1024
)

// shardRaftStreamWindow is the yamux per-stream receive-window ceiling
// (MaxStreamWindowSize) for both sides of every transport session — the
// receiver grants the window, so dial and accept must both set it. Windows
// size to the network, not the message (plans/oversized-objects.md §2): on
// the inter-node LAN the bandwidth-delay product is ~1–3MB, and 4MiB covers
// it with slack for receiver scheduling jitter — the 256KB default left zero
// slack, stalling ~2MB MsgApp frames on every half-window refill (measured
// 35–43ms per write). It also covers the common-path ~2MB frame (replicator
// chunk bound = MaxSizePerMsg) in a single steady-state grant; larger frames
// (up to maxRaftCommandBytes) stream through as a rolling pipeline of
// window-sized slices at full rate.
//
// Deliberately at the bottom of the ratified 4–8MiB range, for two reasons
// that scale with stripe count (one bulk stream per (group, peer)):
//   - Memory: receive buffering may lazily grow to ~2x the window per
//     actively-flowing stream (hashicorp/yamux PR #50), multiplied by
//     stripe count.
//   - Backpressure: the window is how a slow receiver throttles a fast
//     sender; oversizing it converts visible sender-side queueing into
//     invisible receiver memory growth.
//
// Raft timing is NOT a sizing input: the priority lane rides its own TCP
// connection per peer (see laneClass), so no amount of windowed bulk data
// can queue ahead of a heartbeat in a shared socket.
const shardRaftStreamWindow = 4 * 1024 * 1024

// laneClass splits a peer's transport traffic into two classes — priority
// (heartbeats, vote-class messages, MsgTimeoutNow) and bulk (everything
// else) — and each class dials its OWN TCP connection + yamux session per
// peer (sessions and dial locks are keyed by sessionKey). Above the NIC the
// classes share nothing: no session send loop, no socket buffer, no
// flow-control interaction. A bulk connection hard-saturated at the TCP
// level therefore cannot delay a heartbeat or an election, structurally —
// stream windows are pure throughput knobs, never a raft-timing bound.
// Priority and bulk frames to one peer may reorder across the two
// connections; that was already true across streams within one session, and
// raft tolerates it (priority types are idempotent state probes; per-group
// MsgApp FIFO lives entirely inside one bulk stripe).
type laneClass uint8

const (
	laneClassPriority laneClass = iota
	laneClassBulk
)

// sessionKey keys outbound sessions and dial locks by (peer address, class):
// each class gets its own connection and its own dial serialisation.
type sessionKey struct {
	addr  string
	class laneClass
}

// isPriorityMsg reports whether a message rides the priority lane: traffic
// that must reach a peer even while its bulk connection is stalled on data.
// Heartbeats keep leadership leases alive; vote-class messages (and
// MsgTimeoutNow, which triggers an immediate election on the target) decide
// elections, which coincide with exactly the overload that stalls bulk lanes
// — queued behind a saturated bulk connection they would extend
// leaderlessness by the drain time. All priority types are tiny and
// low-rate, so they cannot starve the heartbeat cadence. MsgAppResp
// deliberately stays on the bulk lane: it is high-rate under load, and a
// response flood on the priority lane could delay heartbeats — the exact
// inversion this split exists to prevent; behind bulk it degrades to
// commit-latency throttling, the permitted symptom.
func isPriorityMsg(t raftpb.MessageType) bool {
	switch t {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp,
		raftpb.MsgVote, raftpb.MsgPreVote,
		raftpb.MsgVoteResp, raftpb.MsgPreVoteResp,
		raftpb.MsgTimeoutNow:
		return true
	default:
		return false
	}
}

// outFrame is one encoded wire frame queued on a sender lane, stamped at
// enqueue time so queue_wait can be observed at pickup.
type outFrame struct {
	frame      []byte
	enqueuedAt time.Time
}

// sendLane is one bounded FIFO feeding one writer goroutine that owns one
// yamux stream to the peer. Each peer has one priority lane plus one bulk
// stripe per raft group (see peerSender), each with its own stream — opened
// on the lane's class connection: the priority lane's stream lives on the
// peer's priority connection, bulk stripes share the peer's bulk connection
// (see laneClass), so priority traffic shares nothing with bulk data above
// the NIC. Within the bulk connection yamux flow control is per-stream (a
// stalled stream exhausts only its own send window), so one group's stalled
// stripe no longer head-of-line-blocks another group's — while each group's
// own traffic keeps strict FIFO order through its single stripe. Enqueue
// never blocks: on overflow the incoming frame is dropped and counted at
// dropSite.
type sendLane struct {
	// class picks the peer connection this lane's stream is opened on.
	class laneClass

	mu     sync.Mutex
	q      []outFrame
	qBytes int
	// closed is set (exactly once, by whichever of shutdown or group removal
	// gets there first) when the queue is discarded; enqueues after that
	// point are counted at closedSite instead of accumulating unserved.
	closed     bool
	closedSite string

	// notify wakes the writer; cap 1 so enqueue never blocks on it.
	notify chan struct{}

	// done is closed by removeGroup to retire a bulk stripe whose group left
	// this node; the writer exits on it. Never closed for priority lanes.
	done chan struct{}

	// maxBytes <= 0 means no byte bound (the priority lane is count-bound
	// only; its frames are tiny).
	maxBytes int
	maxMsgs  int
	dropSite string

	// stream is the lane's outbound yamux stream (opened lazily, closed and
	// re-opened on write errors). The writer goroutine owns opening and
	// writing, but group removal closes the stream from outside to unpark a
	// writer blocked mid-frame — so the field is accessed only via
	// setStream/clearStream/takeStream under mu, and net.Conn.Close always
	// runs OUTSIDE the lock (closing on a wedged session can block up to
	// ConnectionWriteTimeout). yamux streams tolerate concurrent and double
	// Close, so the writer's error path and a racing removal may both close
	// the same stream safely. Transport Close unblocks parked writers by
	// closing sessions, not via this field.
	stream net.Conn
}

func newSendLane(class laneClass, maxBytes, maxMsgs int, dropSite string) *sendLane {
	return &sendLane{
		class:    class,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
		maxBytes: maxBytes,
		maxMsgs:  maxMsgs,
		dropSite: dropSite,
	}
}

// setStream installs the writer's freshly opened stream.
func (l *sendLane) setStream(s net.Conn) {
	l.mu.Lock()
	l.stream = s
	l.mu.Unlock()
}

// getStream returns the current stream (nil if none).
func (l *sendLane) getStream() net.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stream
}

// clearStream detaches s if it is still the lane's current stream — the
// writer's error path, which must not clobber a replacement. The caller
// closes s itself (safe even if a racing removal already did).
func (l *sendLane) clearStream(s net.Conn) {
	l.mu.Lock()
	if l.stream == s {
		l.stream = nil
	}
	l.mu.Unlock()
}

// takeStream detaches and returns the current stream for the caller to close
// outside the lock; nil if none.
func (l *sendLane) takeStream() net.Conn {
	l.mu.Lock()
	s := l.stream
	l.stream = nil
	l.mu.Unlock()
	return s
}

// peerSender owns the outbound path to one peer: one priority lane plus one
// bulk stripe per raft group, each with its own writer goroutine and stream.
// Raft needs per-group FIFO delivery (reordering MsgApp within a group
// degrades into rejection/re-send churn) but nothing across groups — pinning
// each group to one stripe preserves the former while removing cross-group
// head-of-line blocking through a single shared lane. label is the peer's
// node-ID string, resolved once for metric labels and address resolution.
// bulk is guarded by MuxTransport.peersMu (never by a lane mutex): lookups
// under RLock, stripe creation/removal under Lock, so stripe-writer spawns
// stay covered by Close's peersMu barrier exactly like peerSender creation.
type peerSender struct {
	label string
	bulk  map[uint64]*sendLane // groupID -> bulk stripe
	prio  *sendLane
}

// heartbeatCoalescer batches heartbeat frames per destination so the flush
// loop enqueues once per peer per tick rather than once per group. Delaying a
// heartbeat is safe: it carries only a commit index and raft tolerates loss.
// take transfers each buffer to the peer's priority sender lane, so the
// heartbeat path costs one buffer allocation per peer per flush; buf is
// accessed only under mu.
type heartbeatCoalescer struct {
	mu  sync.Mutex
	buf map[uint64][]byte // dest nodeID -> accumulated encoded frames
}

func newHeartbeatCoalescer() *heartbeatCoalescer {
	return &heartbeatCoalescer{buf: make(map[uint64][]byte)}
}

// enqueue marshals a heartbeat frame straight onto the destination buffer — no
// intermediate allocation; slices.Grow stops allocating once the buffer reaches
// steady-state size.
func (c *heartbeatCoalescer) enqueue(to, groupID uint64, msg *raftpb.Message) error {
	sz := msg.Size()
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.buf[to]
	off := len(b)
	b = slices.Grow(b, frameHeaderLen+sz)
	b = b[:off+frameHeaderLen+sz]
	putFrameHeader(b[off:], groupID, sz)
	if _, err := msg.MarshalTo(b[off+frameHeaderLen:]); err != nil {
		c.buf[to] = b[:off] // discard the partial frame
		return fmt.Errorf("marshal heartbeat: %w", err)
	}
	c.buf[to] = b
	return nil
}

// peers returns the destinations with buffered heartbeats, appended to dst so
// the caller can reuse its backing array.
func (c *heartbeatCoalescer) peers(dst []uint64) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	for to, b := range c.buf {
		if len(b) > 0 {
			dst = append(dst, to)
		}
	}
	return dst
}

// take removes and returns a destination's accumulated frames, transferring
// ownership to the caller: the buffer is handed to a sender lane, which holds
// it until written, so the coalescer starts a fresh buffer for the
// destination on its next enqueue.
func (c *heartbeatCoalescer) take(to uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.buf[to]
	if len(b) == 0 {
		return nil
	}
	delete(c.buf, to)
	return b
}

// MuxTransport is a per-node singleton that manages a shared TCP listener and
// yamux session pool, multiplexing every shard's RAFT traffic. It implements
// Transport: outbound messages are framed (groupID, raftpb.Message) and
// queued onto sender lanes — per peer, one priority lane plus one bulk
// stripe per raft group, each with its own writer goroutine and yamux stream
// (see sendLane and peerSender) — so Send never blocks on a peer and one
// group's stalled bulk traffic never delays another's. Outbound sessions are
// keyed by (peer address, class): the priority lane owns a separate TCP
// connection per peer, bulk stripes share the other (see laneClass), and the
// two fail and redial independently. Inbound frames are demultiplexed to
// per-group Stores via the MessageRouter; the accept path is class-blind
// (sessions and streams carry no identity, frames carry their groupID).
type MuxTransport struct {
	listener     net.Listener
	advertise    net.Addr
	addrProvider *ShardAddressProvider
	nodeIDs      *nodeIDMap
	router       MessageRouter
	logger       logrus.FieldLogger
	yamuxCfg     *yamux.Config

	sessions   map[sessionKey]*yamux.Session // (peerAddr, class) -> outbound session
	inbound    []*yamux.Session              // accepted server sessions
	sessionsMu sync.RWMutex                  // guards sessions + inbound

	// dialLocks serialises dials per session key (sessionKey -> *sync.Mutex)
	// so stripes of one peer share their class's dial while dials to distinct
	// peers — and the two classes of ONE peer — proceed independently: a dead
	// peer's hanging dial must not delay anyone else, and a bulk dial hanging
	// on a half-dead peer (e.g. SYN blackhole) must not delay a priority dial
	// to the same peer.
	dialLocks sync.Map

	peers   map[uint64]*peerSender // peer uint64 nodeID -> sender lanes
	peersMu sync.RWMutex

	coalescer     *heartbeatCoalescer
	flushInterval time.Duration

	// dropLog rate-limits the WARN lines for message loss so a drop storm
	// stays diagnosable without flooding the log.
	dropLog *logLimiter

	// dialFn dials outbound peer TCP connections; defaults to a plain
	// net.Dialer. Injectable so tests can model unreachable or hung peers.
	// dialCtx is cancelled by Close, aborting any in-flight dial.
	dialFn     func(ctx context.Context, network, addr string) (net.Conn, error)
	dialCtx    context.Context
	dialCancel context.CancelFunc

	shutdownCh chan struct{}
	acceptDone chan struct{}  // closed when acceptLoop exits
	flushDone  chan struct{}  // closed when flushLoop exits
	wg         sync.WaitGroup // handleSession + readStream + lane writer goroutines
}

// errTransportClosed reports an operation racing the transport's shutdown.
var errTransportClosed = fmt.Errorf("shard mux transport is closed")

// errGroupRemoved reports a stripe operation racing the stripe's retirement
// by removeGroup.
var errGroupRemoved = fmt.Errorf("shard mux transport: raft group removed")

// NewMuxTransport creates a new multiplexed transport. It binds a TCP listener
// on bindAddr and starts an accept loop for incoming connections. router
// receives every inbound raft message; nodeIDs translates raft uint64 IDs back
// to string node IDs for address resolution. flushInterval sets the heartbeat
// coalescer's flush cadence; a non-positive value takes the default.
func NewMuxTransport(
	bindAddr string,
	advertise net.Addr,
	provider *ShardAddressProvider,
	nodeIDs *nodeIDMap,
	router MessageRouter,
	logger logrus.FieldLogger,
	flushInterval time.Duration,
) (*MuxTransport, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("bind shard raft transport on %s: %w", bindAddr, err)
	}

	if flushInterval <= 0 {
		flushInterval = defaultHeartbeatFlushInterval
	}

	yamuxCfg := yamux.DefaultConfig()
	yamuxCfg.AcceptBacklog = 1024
	yamuxCfg.ConnectionWriteTimeout = 10 * time.Second
	yamuxCfg.KeepAliveInterval = 15 * time.Second
	yamuxCfg.MaxStreamWindowSize = shardRaftStreamWindow
	yamuxCfg.LogOutput = io.Discard

	dialCtx, dialCancel := context.WithCancel(context.Background())
	m := &MuxTransport{
		listener:      ln,
		advertise:     advertise,
		addrProvider:  provider,
		nodeIDs:       nodeIDs,
		router:        router,
		logger:        logger,
		yamuxCfg:      yamuxCfg,
		sessions:      make(map[sessionKey]*yamux.Session),
		peers:         make(map[uint64]*peerSender),
		coalescer:     newHeartbeatCoalescer(),
		flushInterval: flushInterval,
		dropLog:       newLogLimiter(time.Second),
		dialFn:        (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		dialCtx:       dialCtx,
		dialCancel:    dialCancel,
		shutdownCh:    make(chan struct{}),
		acceptDone:    make(chan struct{}),
		flushDone:     make(chan struct{}),
	}

	enterrors.GoWrapper(m.acceptLoop, logger)
	enterrors.GoWrapper(m.flushLoop, logger)

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
			m.logger.Warnf("shard mux transport: accept error: %v", err)
			continue
		}

		session, err := yamux.Server(conn, m.yamuxCfg)
		if err != nil {
			m.logger.Warnf("shard mux transport: yamux server error: %v", err)
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
// each. Every stream carries framed raft messages. On exit — the session is
// dead (Accept only errors on session teardown) or the transport is closing
// — the session is closed (idempotent; releases the conn if the peer died
// without one) and removed from the inbound list, so peer reconnect churn
// cannot accumulate dead sessions until transport Close.
func (m *MuxTransport) handleSession(session *yamux.Session) {
	defer m.wg.Done()
	defer func() {
		session.Close()
		m.forgetInbound(session)
	}()
	for {
		stream, err := session.Accept()
		if err != nil {
			select {
			case <-m.shutdownCh:
				return
			default:
			}
			if !session.IsClosed() {
				m.logger.Debugf("shard mux transport: stream accept error: %v", err)
			}
			return
		}

		m.wg.Add(1)
		enterrors.GoWrapper(func() {
			m.readStream(stream)
		}, m.logger)
	}
}

// forgetInbound drops one session from the inbound list. No-op after Close's
// sweep has nilled the slice (or when a racing handler already removed it).
func (m *MuxTransport) forgetInbound(session *yamux.Session) {
	m.sessionsMu.Lock()
	for i, s := range m.inbound {
		if s == session {
			m.inbound = append(m.inbound[:i], m.inbound[i+1:]...)
			break
		}
	}
	m.sessionsMu.Unlock()
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
				m.logger.Debugf("shard mux transport: read frame header: %v", err)
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
			m.logger.Debugf("shard mux transport: read frame payload: %v", err)
			return
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(buf); err != nil {
			m.logger.Warnf("shard mux transport: unmarshal raft message: %v", err)
			continue
		}
		if err := m.router.RouteMessage(groupID, msg); err != nil {
			m.logger.WithField("group", groupID).Warnf("shard mux transport: route message: %v", err)
		}
	}
}

// Send frames each raft message and enqueues it onto the destination peer's
// sender lane; the wire write happens on the lane's writer goroutine.
// Non-blocking and fire-and-forget: unresolvable peers and lane overflow are
// counted drops (raft re-sends on the next tick), never a blocked caller.
// Heartbeats are buffered into the coalescer and enqueued as one priority
// frame per peer per flush tick; vote-class traffic rides the priority lane
// directly; everything else keeps per-group FIFO order on the group's bulk
// stripe.
func (m *MuxTransport) Send(groupID uint64, msgs []raftpb.Message) {
	countMessages("send", groupID, msgs)
	for i := range msgs {
		msg := msgs[i]
		if isCoalescableHeartbeat(msg.Type) {
			if err := m.coalescer.enqueue(msg.To, groupID, &msg); err != nil {
				shardRaftDropped.WithLabelValues(dropSiteHeartbeatEncode).Inc()
				m.logger.Warnf("shard mux transport: enqueue heartbeat: %v", err)
			}
			continue
		}
		frame, err := encodeFrame(groupID, &msg)
		if err != nil {
			shardRaftDropped.WithLabelValues(dropSiteEncodeFrame).Inc()
			m.logger.Warnf("shard mux transport: encode frame: %v", err)
			continue
		}
		if isPriorityMsg(msg.Type) {
			ps := m.peerSender(msg.To)
			if ps == nil {
				continue // counted at the specific drop site inside peerSender
			}
			m.enqueueFrame(ps, ps.prio, frame)
		} else {
			ps, lane := m.bulkLane(msg.To, groupID)
			if ps == nil {
				continue // counted at the specific drop site inside bulkLane
			}
			m.enqueueFrame(ps, lane, frame)
		}
	}
}

func isCoalescableHeartbeat(t raftpb.MessageType) bool {
	return t == raftpb.MsgHeartbeat || t == raftpb.MsgHeartbeatResp
}

// enqueueFrame appends one encoded frame to a sender lane and wakes its
// writer. On overflow the INCOMING frame is dropped (drop-newest): dropping
// the head instead would gap the peer's append stream in front of everything
// already queued, degrading the whole backlog into rejection/re-send churn,
// while dropping the tail leaves the queued prefix intact and raft's
// probe/retry machinery re-sends from the loss point. Every drop is counted
// and (rate-limited) WARN-logged.
func (m *MuxTransport) enqueueFrame(ps *peerSender, lane *sendLane, frame []byte) {
	lane.mu.Lock()
	if lane.closed {
		site := lane.closedSite
		lane.mu.Unlock()
		shardRaftDropped.WithLabelValues(site).Inc()
		return
	}
	if (lane.maxBytes > 0 && lane.qBytes+len(frame) > lane.maxBytes) || len(lane.q) >= lane.maxMsgs {
		lane.mu.Unlock()
		shardRaftDropped.WithLabelValues(lane.dropSite).Inc()
		if m.dropLog.Allow(lane.dropSite + ps.label) {
			m.logger.WithField("to", ps.label).Warnf("shard mux transport: sender lane full, dropping frame (%s)", lane.dropSite)
		}
		return
	}
	lane.q = append(lane.q, outFrame{frame: frame, enqueuedAt: time.Now()})
	lane.qBytes += len(frame)
	lane.mu.Unlock()
	select {
	case lane.notify <- struct{}{}:
	default:
	}
}

// popAll moves the lane's queued frames into dst (retaining dst's capacity)
// and resets the queue, zeroing handed-off slots so the backing array drops
// its frame references.
func (l *sendLane) popAll(dst []outFrame) []outFrame {
	l.mu.Lock()
	dst = append(dst, l.q...)
	clear(l.q)
	l.q = l.q[:0]
	l.qBytes = 0
	l.mu.Unlock()
	return dst
}

// closeAndDiscard empties the lane and marks it closed so any racing enqueue
// is counted rather than silently stranded; every discarded frame lands in
// site's ledger (send_shutdown at transport close, send_group_removed at
// stripe retirement). Idempotent: the first close wins — a later call (e.g.
// the writer's exit path after removeGroup already discarded) counts nothing
// and preserves the original site.
func (l *sendLane) closeAndDiscard(site string) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.closedSite = site
	n := len(l.q)
	clear(l.q)
	l.q = l.q[:0]
	l.qBytes = 0
	l.mu.Unlock()
	if n > 0 {
		shardRaftDropped.WithLabelValues(site).Add(float64(n))
	}
}

// runLane is a lane's writer goroutine: it drains the queue into stream
// writes until transport shutdown or (for bulk stripes) group removal.
func (m *MuxTransport) runLane(ps *peerSender, lane *sendLane) {
	defer m.wg.Done()
	defer func() {
		if s := lane.takeStream(); s != nil {
			s.Close()
		}
	}()
	var batch []outFrame
	for {
		select {
		case <-m.shutdownCh:
			lane.closeAndDiscard(dropSiteSendShutdown)
			return
		case <-lane.done:
			// removeGroup already discarded the queue and closed the stream;
			// this call is the no-op second close of the idempotent pair.
			lane.closeAndDiscard(dropSiteSendGroupRemoved)
			return
		case <-lane.notify:
		}
		for {
			batch = lane.popAll(batch[:0])
			if len(batch) == 0 {
				break
			}
			m.writeBatch(ps, lane, batch)
			clear(batch) // release frame references promptly
		}
	}
}

// writeBatch writes a popped run of frames in FIFO order. A write error
// drops the failed frame and the lane's stream (the next frame re-opens,
// re-dialing if the session died); a failure to obtain a stream drops the
// remainder of the batch, counted per frame at the failing site — the next
// enqueue triggers a fresh attempt, mirroring the pre-lane per-Send retry
// behavior.
func (m *MuxTransport) writeBatch(ps *peerSender, lane *sendLane, batch []outFrame) {
	for i := range batch {
		stream := lane.getStream()
		if stream == nil {
			var site string
			var err error
			stream, site, err = m.openLaneStream(ps, lane)
			if err != nil {
				n := len(batch) - i
				shardRaftDropped.WithLabelValues(site).Add(float64(n))
				if site != dropSiteSendShutdown && site != dropSiteSendGroupRemoved && m.dropLog.Allow(site+ps.label) {
					m.logger.WithField("to", ps.label).Warnf("shard mux transport: no stream (%s), dropping %d frames: %v", site, n, err)
				}
				return
			}
			lane.setStream(stream)
		}
		item := batch[i]
		pickup := time.Now()
		shardRaftSendPeer.WithLabelValues(ps.label, "queue_wait").Observe(pickup.Sub(item.enqueuedAt).Seconds())
		_, werr := stream.Write(item.frame)
		shardRaftSendPeer.WithLabelValues(ps.label, "write").Observe(time.Since(pickup).Seconds())
		if werr != nil {
			shardRaftDropped.WithLabelValues(dropSiteSendWriteError).Inc()
			if m.dropLog.Allow(dropSiteSendWriteError + ps.label) {
				m.logger.WithField("to", ps.label).Warnf("shard mux transport: write failed, dropping frame and stream: %v", werr)
			}
			lane.clearStream(stream)
			stream.Close() // safe if a racing removal closed it first
		}
	}
}

// openLaneStream resolves the peer's address and opens a fresh yamux stream
// for one lane on the lane's class connection, dialing a session if none is
// alive. On failure it returns the drop site attributing the loss. A retired
// stripe (group removed while frames were mid-batch) must not re-open a
// stream for a dead group.
func (m *MuxTransport) openLaneStream(ps *peerSender, lane *sendLane) (net.Conn, string, error) {
	select {
	case <-m.shutdownCh:
		return nil, dropSiteSendShutdown, errTransportClosed
	case <-lane.done:
		return nil, dropSiteSendGroupRemoved, errGroupRemoved
	default:
	}
	addr, err := m.addrProvider.Resolve(ps.label)
	if err != nil {
		return nil, dropSitePeerResolve, err
	}
	session, err := m.getOrDialSession(addr, lane.class)
	if err != nil {
		if errors.Is(err, errTransportClosed) {
			return nil, dropSiteSendShutdown, err
		}
		return nil, dropSitePeerDial, err
	}
	stream, err := session.Open()
	if err != nil {
		return nil, dropSitePeerOpenStream, err
	}
	return stream, "", nil
}

// flushLoop flushes the heartbeat coalescer on a fixed cadence until shutdown.
// peers is reused across ticks; the flush loop is the only goroutine that
// touches it.
func (m *MuxTransport) flushLoop() {
	defer close(m.flushDone)
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	var peers []uint64
	for {
		select {
		case <-m.shutdownCh:
			return
		case <-ticker.C:
			peers = m.flushHeartbeats(peers)
		}
	}
}

// flushHeartbeats hands each peer's buffered heartbeats to that peer's
// priority lane as a single concatenated frame (one stream write per peer per
// tick). peers is a reused buffer, returned so the caller retains its grown
// capacity for the next call.
func (m *MuxTransport) flushHeartbeats(peers []uint64) []uint64 {
	peers = m.coalescer.peers(peers[:0])
	for _, to := range peers {
		frames := m.coalescer.take(to)
		if len(frames) == 0 {
			continue
		}
		ps := m.peerSender(to)
		if ps == nil {
			continue // counted at the specific drop site inside peerSender
		}
		m.enqueueFrame(ps, ps.prio, frames)
	}
	return peers
}

// peerSender returns the destination's sender, creating it — and spawning
// its priority-lane writer goroutine — on first use. Creation is cheap (map
// insert + spawn; no I/O): address resolution and dialing happen on the
// writer goroutines, so a dead peer can never delay an enqueue. Returns nil
// (with the drop counted) if the uint64 ID is unknown or the transport is
// shutting down.
func (m *MuxTransport) peerSender(to uint64) *peerSender {
	m.peersMu.RLock()
	ps, ok := m.peers[to]
	m.peersMu.RUnlock()
	if ok {
		return ps
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	return m.peerSenderLocked(to)
}

// peerSenderLocked is peerSender's slow path; the caller holds peersMu.
func (m *MuxTransport) peerSenderLocked(to uint64) *peerSender {
	if ps, ok := m.peers[to]; ok {
		return ps
	}

	// Checked under peersMu so Close's barrier (which acquires peersMu after
	// closing shutdownCh) guarantees no writer spawns after its wg.Wait began.
	select {
	case <-m.shutdownCh:
		shardRaftDropped.WithLabelValues(dropSiteSendShutdown).Inc()
		return nil
	default:
	}

	nodeID, ok := m.nodeIDs.stringID(to)
	if !ok {
		shardRaftDropped.WithLabelValues(dropSitePeerResolve).Inc()
		m.logger.WithField("to", to).Warn("shard mux transport: unknown destination node ID")
		return nil
	}
	ps := &peerSender{
		label: nodeID,
		bulk:  make(map[uint64]*sendLane),
		prio:  newSendLane(laneClassPriority, 0, prioLaneMaxMsgs, dropSiteSendPrioQueueFull),
	}
	m.peers[to] = ps
	m.wg.Add(1)
	enterrors.GoWrapper(func() { m.runLane(ps, ps.prio) }, m.logger)
	return ps
}

// bulkLane returns the destination's bulk stripe for a group, creating the
// stripe (and the peer sender) — and spawning the stripe's writer goroutine —
// on first use. Same barrier discipline as peerSender: creation re-checks
// shutdownCh under peersMu, so no stripe writer spawns after Close's wg.Wait
// began. Returns nils (with the drop counted) if the uint64 ID is unknown or
// the transport is shutting down. A Send for a group whose stripe was just
// removed legitimately re-creates it (groups restart on tenant re-load); the
// no-Send-after-teardown ordering is owned by the Store lifecycle
// (Stop-before-unregister).
func (m *MuxTransport) bulkLane(to, groupID uint64) (*peerSender, *sendLane) {
	m.peersMu.RLock()
	ps, ok := m.peers[to]
	var lane *sendLane
	if ok {
		lane = ps.bulk[groupID]
	}
	m.peersMu.RUnlock()
	if lane != nil {
		return ps, lane
	}

	m.peersMu.Lock()
	defer m.peersMu.Unlock()
	ps = m.peerSenderLocked(to)
	if ps == nil {
		return nil, nil // counted at the specific drop site
	}
	if lane, ok := ps.bulk[groupID]; ok {
		return ps, lane
	}
	select {
	case <-m.shutdownCh:
		shardRaftDropped.WithLabelValues(dropSiteSendShutdown).Inc()
		return nil, nil
	default:
	}
	lane = newSendLane(laneClassBulk, bulkStripeMaxBytes, bulkStripeMaxMsgs, dropSiteSendBulkQueueFull)
	ps.bulk[groupID] = lane
	m.wg.Add(1)
	enterrors.GoWrapper(func() { m.runLane(ps, lane) }, m.logger)
	return ps, lane
}

// removeGroup retires every bulk stripe of a departed raft group across all
// peers: queued frames are discarded (counted once at send_group_removed),
// the writer is told to exit, and the stripe's stream is closed — unparking
// a writer blocked mid-frame against a stalled peer, so a departed group can
// never leak its goroutine or stream. Idempotent (only the caller that
// deletes a stripe under peersMu reaps it). Called by the Registry after the
// group's Store has stopped (its Ready loop — the only Send source — has
// exited). Stream closes run outside all locks: closing on a wedged session
// can block up to ConnectionWriteTimeout, the same bounded exposure as the
// writer's own error-path close.
func (m *MuxTransport) removeGroup(groupID uint64) {
	var reaped []*sendLane
	m.peersMu.Lock()
	for _, ps := range m.peers {
		if lane, ok := ps.bulk[groupID]; ok {
			delete(ps.bulk, groupID)
			reaped = append(reaped, lane)
		}
	}
	m.peersMu.Unlock()

	for _, lane := range reaped {
		lane.closeAndDiscard(dropSiteSendGroupRemoved)
		close(lane.done)
		if s := lane.takeStream(); s != nil {
			s.Close()
		}
	}
}

// getOrDialSession returns an existing outbound yamux session for the peer's
// class connection, or dials a new TCP connection and creates a yamux client
// session. Dials are serialised per (address, class) (never under
// sessionsMu), so a hanging dial to one peer cannot delay sessions to any
// other — and a hanging bulk dial cannot delay a priority dial to the same
// peer. The insert re-checks shutdown under sessionsMu so a dial racing
// Close cannot resurrect a session after Close's sweep.
func (m *MuxTransport) getOrDialSession(addr string, class laneClass) (*yamux.Session, error) {
	key := sessionKey{addr: addr, class: class}
	if s := m.lookupSession(key); s != nil {
		return s, nil
	}

	lockAny, _ := m.dialLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockAny.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	// Re-check: a concurrent holder of this key's lock may have dialed.
	if s := m.lookupSession(key); s != nil {
		return s, nil
	}
	select {
	case <-m.shutdownCh:
		return nil, errTransportClosed
	default:
	}

	conn, err := m.dialFn(m.dialCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", addr, err)
	}

	session, err := yamux.Client(conn, m.yamuxCfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("yamux client for %s: %w", addr, err)
	}

	m.sessionsMu.Lock()
	select {
	case <-m.shutdownCh:
		m.sessionsMu.Unlock()
		session.Close()
		return nil, errTransportClosed
	default:
	}
	m.sessions[key] = session
	m.sessionsMu.Unlock()
	return session, nil
}

// lookupSession returns the live outbound session for a key, or nil.
func (m *MuxTransport) lookupSession(key sessionKey) *yamux.Session {
	m.sessionsMu.RLock()
	session, ok := m.sessions[key]
	m.sessionsMu.RUnlock()
	if ok && !session.IsClosed() {
		return session
	}
	return nil
}

// Close shuts down the mux transport: signals shutdown (aborting any
// in-flight dial), stops the accept and flush loops, closes every yamux
// session (inbound and outbound) — force-closing all streams, which unblocks
// readStream goroutines and any lane writer parked in a stream write — then
// waits for every goroutine to stop. Frames still queued on sender lanes are
// discarded and counted as send_shutdown drops; the Registry stops all
// Stores before closing the transport, so nothing enqueues after this.
func (m *MuxTransport) Close() error {
	close(m.shutdownCh)
	m.dialCancel()

	if err := m.listener.Close(); err != nil {
		m.logger.Warnf("shard mux transport: error closing listener: %v", err)
	}

	// Wait for the accept loop to exit before closing inbound sessions, and
	// for the flush loop to exit so it cannot enqueue after the peers barrier
	// below.
	<-m.acceptDone
	<-m.flushDone

	m.sessionsMu.Lock()
	for _, session := range m.inbound {
		session.Close()
	}
	m.inbound = nil
	for key, session := range m.sessions {
		if err := session.Close(); err != nil {
			m.logger.WithField("peer", key.addr).Debugf("shard mux transport: error closing session: %v", err)
		}
		delete(m.sessions, key)
	}
	m.sessionsMu.Unlock()

	// Barrier: peerSender creation checks shutdownCh under peersMu, so after
	// this acquisition no new lane writer can spawn — wg.Wait covers every
	// goroutine ever started.
	m.peersMu.Lock()
	m.peers = make(map[uint64]*peerSender)
	m.peersMu.Unlock()

	m.wg.Wait()

	m.logger.Info("shard RAFT mux transport closed")
	return nil
}

// putFrameHeader writes the frameHeaderLen-byte wire-frame prefix into dst.
func putFrameHeader(dst []byte, groupID uint64, msgLen int) {
	binary.BigEndian.PutUint64(dst[:8], groupID)
	binary.BigEndian.PutUint32(dst[8:12], uint32(msgLen))
}

// encodeFrame builds a wire frame: [uint64 groupID BE][uint32 msgLen BE][msg].
func encodeFrame(groupID uint64, msg *raftpb.Message) ([]byte, error) {
	body, err := msg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal raft message: %w", err)
	}
	frame := make([]byte, frameHeaderLen+len(body))
	putFrameHeader(frame, groupID, len(body))
	copy(frame[frameHeaderLen:], body)
	return frame, nil
}
