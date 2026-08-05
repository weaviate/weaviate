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
	"io"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// The tests in this file pin the transport's liveness contract: Send never
// blocks its caller, heartbeat-class traffic reaches a peer whose bulk lane
// is stalled, overflow drops are accounted and recovered from, and Close
// stays bounded with stalled peers.

// laneMux bundles a MuxTransport built for lane tests with its ID map, the
// capture router receiving its inbound traffic, and an idempotent close.
type laneMux struct {
	mux     *MuxTransport
	nodeIDs *nodeIDMap
	router  *captureRouter
	close   func()
}

// newLaneMux binds a MuxTransport on loopback whose resolver maps each given
// peer node ID to a fixed host:port. Close is registered as a cleanup but is
// also returned (idempotent) so tests can close explicitly.
func newLaneMux(t *testing.T, flushInterval time.Duration, peerAddrs map[string]string) *laneMux {
	t.Helper()
	logger, _ := test.NewNullLogger()

	addresses := map[string]string{}
	portMap := map[string]int{}
	for id, addr := range peerAddrs {
		host, portStr, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		port, err := strconv.Atoi(portStr)
		require.NoError(t, err)
		addresses[id] = host
		portMap[id] = port
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	self := ln.Addr().String()
	require.NoError(t, ln.Close())
	selfHost, selfPortStr, err := net.SplitHostPort(self)
	require.NoError(t, err)
	selfPort, err := strconv.Atoi(selfPortStr)
	require.NoError(t, err)
	addresses["lane-self"] = selfHost
	portMap["lane-self"] = selfPort

	provider := &ShardAddressProvider{
		resolver:          &mockResolver{addresses: addresses},
		raftPort:          selfPort,
		isLocalCluster:    true,
		nodeNameToPortMap: portMap,
	}
	advertise, err := net.ResolveTCPAddr("tcp", self)
	require.NoError(t, err)

	nodeIDs := newNodeIDMap()
	nodeIDs.register("lane-self")
	for id := range peerAddrs {
		nodeIDs.register(id)
	}
	router := &captureRouter{}

	mux, err := NewMuxTransport(self, advertise, provider, nodeIDs, router, logger, flushInterval)
	require.NoError(t, err)

	var once sync.Once
	closeFn := func() { once.Do(func() { mux.Close() }) }
	t.Cleanup(closeFn)
	return &laneMux{mux: mux, nodeIDs: nodeIDs, router: router, close: closeFn}
}

// silentTCPPeer accepts TCP connections and never reads from them: a yamux
// client can dial and open streams (small control frames are absorbed by
// kernel buffers) but any bulk write parks on the yamux send window forever.
// The returned stop func closes the listener and every accepted conn,
// unblocking parked writers; tests MUST register it via t.Cleanup AFTER
// building the mux so it runs before the mux's own cleanup Close.
func silentTCPPeer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var mu sync.Mutex
	var conns []net.Conn
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			ln.Close()
			<-acceptDone
			mu.Lock()
			for _, c := range conns {
				c.Close()
			}
			mu.Unlock()
		})
	}
	return ln.Addr().String(), stop
}

// testYamuxPeer is a yamux server endpoint with controllable read behavior:
// with stallOnApp, each stream is read normally until its first MsgApp frame,
// then parked (its send window fills and the sender's writes stall) while
// other streams keep being read — modelling a peer whose TCP connection
// accepts writes but whose bulk stream is not drained. With
// stallConnMinAppBytes > 0, the first MsgApp frame of at least that many
// payload bytes parks the whole CONNECTION it arrived on (reads stop at the
// TCP level, kernel buffers fill, the sender's session send loop wedges at a
// socket write) while other connections keep being read. With a shut gate,
// no stream is read at all until release().
type testYamuxPeer struct {
	addr                 string
	ln                   net.Listener
	stallOnApp           bool
	stallConnMinAppBytes int
	readBuf              int
	gate                 chan struct{}
	stopCh               chan struct{}
	wg                   sync.WaitGroup
	stopOnce             sync.Once

	mu     sync.Mutex
	pconns []*peerConn

	heartbeats atomic.Int64
	votes      atomic.Int64
	appMu      sync.Mutex
	appIdx     []uint64
}

// peerConn is one accepted connection, its yamux session, and per-connection
// traffic counts. Tests identify a connection's class behaviorally — bulk has
// carried a MsgApp, priority has carried heartbeats and no MsgApp — so the
// production wire needs no class marking.
type peerConn struct {
	conn    *gatedConn
	session *yamux.Session

	heartbeats atomic.Int64
	apps       atomic.Int64
}

// gatedConn wraps an accepted TCP conn so trip() can park its reads,
// modelling a peer that stops draining one connection at the TCP level.
type gatedConn struct {
	net.Conn
	tripped chan struct{}
	stopCh  chan struct{}
	once    sync.Once
}

func (c *gatedConn) trip() { c.once.Do(func() { close(c.tripped) }) }

func (c *gatedConn) Read(p []byte) (int, error) {
	select {
	case <-c.tripped:
		<-c.stopCh
		return 0, io.ErrClosedPipe
	default:
	}
	return c.Conn.Read(p)
}

type yamuxPeerOpts struct {
	stallOnApp           bool // park the stream that carries a MsgApp
	gated                bool // read nothing until release()
	stallConnMinAppBytes int  // park the connection carrying a MsgApp at least this large
	readBuf              int  // kernel receive-buffer cap per accepted conn
}

func startTestYamuxPeer(t *testing.T, stallOnApp, gated bool) *testYamuxPeer {
	return startTestYamuxPeerOpts(t, yamuxPeerOpts{stallOnApp: stallOnApp, gated: gated})
}

func startTestYamuxPeerOpts(t *testing.T, opts yamuxPeerOpts) *testYamuxPeer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &testYamuxPeer{
		addr:                 ln.Addr().String(),
		ln:                   ln,
		stallOnApp:           opts.stallOnApp,
		stallConnMinAppBytes: opts.stallConnMinAppBytes,
		readBuf:              opts.readBuf,
		gate:                 make(chan struct{}),
		stopCh:               make(chan struct{}),
	}
	if !opts.gated {
		close(p.gate)
	}
	p.wg.Add(1)
	go p.acceptLoop()
	return p
}

func (p *testYamuxPeer) acceptLoop() {
	defer p.wg.Done()
	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.readBuf > 0 {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(p.readBuf)
			}
		}
		gc := &gatedConn{Conn: conn, tripped: make(chan struct{}), stopCh: p.stopCh}
		session, err := yamux.Server(gc, cfg)
		if err != nil {
			conn.Close()
			continue
		}
		pc := &peerConn{conn: gc, session: session}
		p.mu.Lock()
		p.pconns = append(p.pconns, pc)
		p.mu.Unlock()
		p.wg.Add(1)
		go p.acceptStreams(pc)
	}
}

func (p *testYamuxPeer) acceptStreams(pc *peerConn) {
	defer p.wg.Done()
	for {
		stream, err := pc.session.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.readFrames(pc, stream)
	}
}

func (p *testYamuxPeer) readFrames(pc *peerConn, stream net.Conn) {
	defer p.wg.Done()
	select {
	case <-p.gate:
	case <-p.stopCh:
		return
	}
	var hdr [12]byte
	for {
		if _, err := io.ReadFull(stream, hdr[:]); err != nil {
			return
		}
		msgLen := binary.BigEndian.Uint32(hdr[8:12])
		buf := make([]byte, msgLen)
		if _, err := io.ReadFull(stream, buf); err != nil {
			return
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(buf); err != nil {
			return
		}
		switch msg.Type {
		case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
			pc.heartbeats.Add(1)
			p.heartbeats.Add(1)
		case raftpb.MsgVote, raftpb.MsgPreVote:
			p.votes.Add(1)
		case raftpb.MsgApp:
			p.appMu.Lock()
			p.appIdx = append(p.appIdx, msg.Index)
			p.appMu.Unlock()
			pc.apps.Add(1)
			if p.stallConnMinAppBytes > 0 && len(buf) >= p.stallConnMinAppBytes {
				// Park the whole connection: its reads stop at the TCP level.
				pc.conn.trip()
			}
			if p.stallOnApp {
				// Park this stream: stop reading so its send window fills.
				<-p.stopCh
				return
			}
		default:
		}
	}
}

// connWhere returns the first accepted connection matching pred, or nil.
func (p *testYamuxPeer) connWhere(pred func(*peerConn) bool) *peerConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.pconns {
		if pred(pc) {
			return pc
		}
	}
	return nil
}

// bulkConn is the accepted connection that has carried MsgApp traffic;
// prioConn the one that has carried heartbeats and no MsgApp. Nil until
// identifiable.
func (p *testYamuxPeer) bulkConn() *peerConn {
	return p.connWhere(func(pc *peerConn) bool { return pc.apps.Load() > 0 })
}

func (p *testYamuxPeer) prioConn() *peerConn {
	return p.connWhere(func(pc *peerConn) bool {
		return pc.heartbeats.Load() > 0 && pc.apps.Load() == 0
	})
}

func (p *testYamuxPeer) release() { close(p.gate) }

func (p *testYamuxPeer) appCount() int {
	p.appMu.Lock()
	defer p.appMu.Unlock()
	return len(p.appIdx)
}

func (p *testYamuxPeer) appIndexes() []uint64 {
	p.appMu.Lock()
	defer p.appMu.Unlock()
	return append([]uint64(nil), p.appIdx...)
}

// stop closes the listener, sessions, and conns, unblocking any sender-side
// writer parked on a window this peer will never open. Idempotent.
func (p *testYamuxPeer) stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.ln.Close()
		p.mu.Lock()
		for _, pc := range p.pconns {
			pc.session.Close()
			pc.conn.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
}

// bulkMsgApp builds a MsgApp with one entry of the given payload size; Index
// doubles as a sequence number so receivers can assert delivery order.
func bulkMsgApp(to, index uint64, size int) raftpb.Message {
	return raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      to,
		From:    1,
		Index:   index,
		Entries: []raftpb.Entry{{Index: index, Term: 1, Data: make([]byte, size)}},
	}
}

// TestMuxTransport_Send_StalledPeerDoesNotBlock pins the core liveness
// contract: a peer that is connected but not reading (send window exhausted)
// must not block Send callers — the Ready loop and the append/apply workers
// call Send inline, so a blocked Send stalls raft timing.
func TestMuxTransport_Send_StalledPeerDoesNotBlock(t *testing.T) {
	peerAddr, stopPeer := silentTCPPeer(t)
	lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peerAddr})
	t.Cleanup(stopPeer) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Larger than the 256KB yamux stream window: the wire write cannot
		// complete against a peer that never reads.
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 1<<20)})
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 2, 1024)})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a stalled peer; the transmit path must be enqueue-only")
	}
}

// TestMuxTransport_Send_HungDialDoesNotBlockOtherPeers pins cross-peer
// isolation of the dial path: a peer whose TCP dial hangs must not delay
// sends to a healthy peer (the old code dialed while holding the global
// peers write lock, convoying every other Send behind a dead peer's dial).
func TestMuxTransport_Send_HungDialDoesNotBlockOtherPeers(t *testing.T) {
	// Healthy peer C is a real transport so arrival is observable.
	peerC := newLaneMux(t, time.Hour, nil)
	cAddr := peerC.mux.listener.Addr().String()

	// Peer B resolves to an address whose dial hangs until the dial ctx dies.
	bAddr := "127.0.0.1:9" // never actually dialed; the hook intercepts it
	sender := newLaneMux(t, time.Hour, map[string]string{"peer-b": bAddr, "peer-c": cAddr})
	realDial := sender.mux.dialFn
	sender.mux.dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == bAddr {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return realDial(ctx, network, addr)
	}
	toB := sender.nodeIDs.register("peer-b")
	toC := sender.nodeIDs.register("peer-c")

	go func() {
		sender.mux.Send(1, []raftpb.Message{bulkMsgApp(toB, 1, 1024)})
	}()
	// Let the B send reach its hung dial before sending to C.
	time.Sleep(100 * time.Millisecond)

	go func() {
		sender.mux.Send(1, []raftpb.Message{bulkMsgApp(toC, 2, 1024)})
	}()

	require.Eventually(t, func() bool { return peerC.router.count() >= 1 },
		2*time.Second, 10*time.Millisecond,
		"send to a healthy peer must not wait behind another peer's hung dial")
}

// TestMuxTransport_HeartbeatsFlowWhileBulkStalled pins the per-lane stream
// isolation: with peer B's bulk stream stalled mid-MsgApp (window full, TCP
// still accepting writes), heartbeat-class traffic must keep reaching B.
func TestMuxTransport_HeartbeatsFlowWhileBulkStalled(t *testing.T) {
	peer := startTestYamuxPeer(t, true, false)
	lm := newLaneMux(t, 20*time.Millisecond, map[string]string{"peer-b": peer.addr})
	t.Cleanup(peer.stop) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")
	from := lm.nodeIDs.register("lane-self")

	// Three window-sized MsgApps: the peer consumes the first, then parks the
	// stream, so a later write stalls mid-frame.
	go func() {
		for i := 1; i <= 3; i++ {
			lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, uint64(i), 300<<10)})
		}
	}()
	require.Eventually(t, func() bool { return peer.appCount() >= 1 },
		2*time.Second, 10*time.Millisecond, "first MsgApp should arrive before the stall")
	// Give the follow-up write time to park on the stalled stream.
	time.Sleep(100 * time.Millisecond)

	before := peer.heartbeats.Load()
	require.Eventually(t, func() bool {
		lm.mux.Send(1, []raftpb.Message{{Type: raftpb.MsgHeartbeat, To: to, From: from, Term: 1}})
		return peer.heartbeats.Load() >= before+3
	}, 3*time.Second, 20*time.Millisecond,
		"heartbeats must keep flowing to a peer whose bulk stream is stalled")
}

// TestMuxTransport_HeartbeatsFlowWhileBulkConnectionStalled pins the
// connection-level inversion bound: the priority lane rides its OWN TCP
// connection per peer, so a bulk connection hard-stalled at the TCP level —
// the peer stops reading the whole connection, kernel buffers fill, and the
// bulk session's send loop wedges at a socket write — cannot delay
// heartbeat-class traffic or votes to the same peer. Against a
// shared-session transport this fails: one wedged send loop starves every
// stream, priority included.
//
// The peer caps its kernel receive buffer and the sender pushes two stripes'
// worth of window-sized frames after the stall, so the stalled socket is
// provably saturated. All assertions complete well inside the 10s
// ConnectionWriteTimeout: past it, yamux tears down the wedged session and
// the ensuing redial could mask a shared-connection regression as recovery.
func TestMuxTransport_HeartbeatsFlowWhileBulkConnectionStalled(t *testing.T) {
	peer := startTestYamuxPeerOpts(t, yamuxPeerOpts{stallConnMinAppBytes: 1 << 20, readBuf: 64 << 10})
	lm := newLaneMux(t, 20*time.Millisecond, map[string]string{"peer-b": peer.addr})
	t.Cleanup(peer.stop) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")
	from := lm.nodeIDs.register("lane-self")

	const (
		groupA = uint64(1)
		groupB = uint64(2)
	)

	// Warm both stripes: small MsgApps are consumed normally, growing each
	// stream's send window toward the 4MiB max.
	lm.mux.Send(groupA, []raftpb.Message{bulkMsgApp(to, 1, 64<<10)})
	lm.mux.Send(groupB, []raftpb.Message{bulkMsgApp(to, 2, 64<<10)})
	require.Eventually(t, func() bool { return peer.appCount() >= 2 },
		2*time.Second, 10*time.Millisecond, "warm-up MsgApps should arrive")

	// A large MsgApp trips the peer: it stops reading the bulk connection.
	lm.mux.Send(groupA, []raftpb.Message{bulkMsgApp(to, 3, 2<<20)})
	require.Eventually(t, func() bool { return slices.Contains(peer.appIndexes(), uint64(3)) },
		2*time.Second, 10*time.Millisecond, "trip MsgApp should arrive before the stall")

	// Saturate the stalled connection: two window-sized frames far exceed
	// kernel buffering, wedging the bulk session's send loop mid-write.
	lm.mux.Send(groupA, []raftpb.Message{bulkMsgApp(to, 4, 4<<20)})
	lm.mux.Send(groupB, []raftpb.Message{bulkMsgApp(to, 5, 4<<20)})
	time.Sleep(200 * time.Millisecond) // let the writes park

	before := peer.heartbeats.Load()
	require.Eventually(t, func() bool {
		lm.mux.Send(groupA, []raftpb.Message{{Type: raftpb.MsgHeartbeat, To: to, From: from, Term: 1}})
		return peer.heartbeats.Load() >= before+3
	}, 3*time.Second, 20*time.Millisecond,
		"heartbeats must keep flowing while the bulk connection is TCP-stalled")

	// Elections proceed: vote-class traffic rides the priority connection.
	lm.mux.Send(groupA, []raftpb.Message{{Type: raftpb.MsgVote, To: to, From: from, Term: 2}})
	require.Eventually(t, func() bool { return peer.votes.Load() >= 1 },
		3*time.Second, 20*time.Millisecond,
		"votes must reach a peer whose bulk connection is TCP-stalled")
}

// TestMuxTransport_IndependentReconnect pins per-class connection failure
// independence: killing one class's TCP connection must not disturb the other
// class — its traffic keeps flowing on the SAME accepted connection — while
// the killed class redials and resumes on a NEW connection. The
// identification step is itself the structural pin: a shared-session
// transport never presents distinct bulk and priority connections.
func TestMuxTransport_IndependentReconnect(t *testing.T) {
	tests := []struct {
		name     string
		killBulk bool
	}{
		{name: "bulk connection killed, priority uninterrupted", killBulk: true},
		{name: "priority connection killed, bulk uninterrupted", killBulk: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			peer := startTestYamuxPeer(t, false, false) // reads everything
			lm := newLaneMux(t, 20*time.Millisecond, map[string]string{"peer-b": peer.addr})
			t.Cleanup(peer.stop)
			to := lm.nodeIDs.register("peer-b")
			from := lm.nodeIDs.register("lane-self")

			sendHB := func() {
				lm.mux.Send(1, []raftpb.Message{{Type: raftpb.MsgHeartbeat, To: to, From: from, Term: 1}})
			}

			// Establish traffic on both classes and identify the connections.
			lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 1024)})
			require.Eventually(t, func() bool {
				sendHB()
				return peer.bulkConn() != nil && peer.prioConn() != nil
			}, 3*time.Second, 20*time.Millisecond,
				"transport must present distinct bulk and priority connections")
			bulkPC, prioPC := peer.bulkConn(), peer.prioConn()

			if tc.killBulk {
				require.NoError(t, bulkPC.conn.Close())

				// Priority uninterrupted, on the same accepted connection.
				before := prioPC.heartbeats.Load()
				require.Eventually(t, func() bool {
					sendHB()
					return prioPC.heartbeats.Load() >= before+3
				}, 3*time.Second, 20*time.Millisecond,
					"heartbeats must keep flowing on the surviving priority connection")

				// Bulk redials and resumes on a new connection; the re-send
				// loop stands in for raft's probe/retry after the drop window.
				idx := uint64(100)
				require.Eventually(t, func() bool {
					idx++
					lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, idx, 1024)})
					return peer.connWhere(func(pc *peerConn) bool {
						return pc != bulkPC && pc.apps.Load() > 0
					}) != nil
				}, 5*time.Second, 50*time.Millisecond,
					"bulk traffic must resume over a fresh connection")
			} else {
				require.NoError(t, prioPC.conn.Close())

				// Bulk unaffected, on the same accepted connection.
				appsBefore := bulkPC.apps.Load()
				idx := uint64(100)
				require.Eventually(t, func() bool {
					idx++
					lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, idx, 1024)})
					return bulkPC.apps.Load() > appsBefore
				}, 3*time.Second, 50*time.Millisecond,
					"bulk traffic must keep flowing on the surviving bulk connection")

				// Priority redials and resumes on a new connection.
				require.Eventually(t, func() bool {
					sendHB()
					return peer.connWhere(func(pc *peerConn) bool {
						return pc != prioPC && pc.heartbeats.Load() > 0
					}) != nil
				}, 5*time.Second, 50*time.Millisecond,
					"heartbeats must resume over a fresh priority connection")
			}
		})
	}
}

// TestMuxTransport_BulkStripes_CrossGroupIsolation pins per-group stripe
// isolation on the bulk path: with group A's stripe stalled mid-frame against
// a peer that stopped draining it, group B's traffic to the SAME peer must
// still arrive — and heartbeats must keep flowing throughout. Before
// striping, one bulk lane per peer serialized every group through a single
// stream: A's parked frame head-of-line-blocked B (the measured 211–349ms
// queue waits under 3-group import load).
func TestMuxTransport_BulkStripes_CrossGroupIsolation(t *testing.T) {
	peer := startTestYamuxPeer(t, true, false) // parks each stream at its first MsgApp
	lm := newLaneMux(t, 20*time.Millisecond, map[string]string{"peer-b": peer.addr})
	t.Cleanup(peer.stop) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")
	from := lm.nodeIDs.register("lane-self")

	const (
		groupA = uint64(1)
		groupB = uint64(2)
	)

	// Group A: the first MsgApp is consumed (parking A's stream); the second —
	// larger than any configured stream window — parks A's writer mid-frame.
	lm.mux.Send(groupA, []raftpb.Message{bulkMsgApp(to, 1, 64<<10)})
	require.Eventually(t, func() bool { return peer.appCount() >= 1 },
		2*time.Second, 10*time.Millisecond, "group A's first MsgApp should arrive before the stall")
	lm.mux.Send(groupA, []raftpb.Message{bulkMsgApp(to, 2, 6<<20)})
	time.Sleep(100 * time.Millisecond) // let A's follow-up write park on the stalled stream

	// Group B to the same peer must not queue behind group A's parked frame.
	lm.mux.Send(groupB, []raftpb.Message{bulkMsgApp(to, 100, 1024)})
	require.Eventually(t, func() bool {
		return slices.Contains(peer.appIndexes(), uint64(100))
	}, 2*time.Second, 10*time.Millisecond,
		"group B's MsgApp must not head-of-line-block behind group A's stalled stripe")

	// Priority traffic stays unaffected while bulk stripes are congested.
	before := peer.heartbeats.Load()
	require.Eventually(t, func() bool {
		lm.mux.Send(groupA, []raftpb.Message{{Type: raftpb.MsgHeartbeat, To: to, From: from, Term: 1}})
		return peer.heartbeats.Load() >= before+3
	}, 3*time.Second, 20*time.Millisecond,
		"heartbeats must keep flowing while bulk stripes are stalled")
}

// TestMuxTransport_BulkStripes_PerGroupFIFO pins the ordering contract
// striping must preserve: within one group, accepted bulk traffic reaches
// the peer in send order (one stripe = one FIFO = one stream), even with two
// groups' sends interleaved and initially blocked against an unread window.
func TestMuxTransport_BulkStripes_PerGroupFIFO(t *testing.T) {
	peer := startTestYamuxPeer(t, false, true) // gated: reads nothing until release
	lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peer.addr})
	t.Cleanup(peer.stop) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")

	// Interleave two groups' ordered MsgApps; the index encodes (group, seq)
	// so per-group order is assertable after arbitrary cross-group
	// interleaving. Volumes stay far under the stripe bounds — nothing drops.
	const perGroup = 10
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= perGroup; i++ {
			lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, uint64(1000+i), 128<<10)})
			lm.mux.Send(2, []raftpb.Message{bulkMsgApp(to, uint64(2000+i), 128<<10)})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked against an unread peer; enqueue must stay non-blocking")
	}

	peer.release()
	require.Eventually(t, func() bool { return peer.appCount() == 2*perGroup },
		10*time.Second, 20*time.Millisecond, "every frame must be delivered once the peer reads")

	var groupA, groupB []uint64
	for _, idx := range peer.appIndexes() {
		if idx >= 2000 {
			groupB = append(groupB, idx)
		} else {
			groupA = append(groupA, idx)
		}
	}
	require.Len(t, groupA, perGroup)
	require.Len(t, groupB, perGroup)
	require.True(t, slices.IsSorted(groupA), "group A's MsgApps reordered: %v", groupA)
	require.True(t, slices.IsSorted(groupB), "group B's MsgApps reordered: %v", groupB)
}

// bulkStripeCount reports how many peers currently hold a bulk stripe for
// the group.
func bulkStripeCount(m *MuxTransport, groupID uint64) int {
	m.peersMu.RLock()
	defer m.peersMu.RUnlock()
	n := 0
	for _, ps := range m.peers {
		if _, ok := ps.bulk[groupID]; ok {
			n++
		}
	}
	return n
}

// TestMuxTransport_RemoveGroup pins stripe retirement: a departed group's
// stripes are reaped (map entry, queued frames, writer, stream), other
// groups are untouched, a later Send legitimately re-creates the stripe, and
// a writer parked mid-frame against a stalled peer is unparked — with every
// frame accounted exactly once across the removal and write-error sites.
func TestMuxTransport_RemoveGroup(t *testing.T) {
	t.Run("retires the group's stripe, preserves others, re-creates on demand", func(t *testing.T) {
		peer := startTestYamuxPeer(t, false, false) // reads everything
		lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peer.addr})
		t.Cleanup(peer.stop)
		to := lm.nodeIDs.register("peer-b")

		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 1024)})
		lm.mux.Send(2, []raftpb.Message{bulkMsgApp(to, 2, 1024)})
		require.Eventually(t, func() bool { return peer.appCount() == 2 },
			2*time.Second, 10*time.Millisecond)
		require.Equal(t, 1, bulkStripeCount(lm.mux, 1))
		require.Equal(t, 1, bulkStripeCount(lm.mux, 2))

		lm.mux.removeGroup(1)
		require.Equal(t, 0, bulkStripeCount(lm.mux, 1), "removed group's stripe must be reaped")
		require.Equal(t, 1, bulkStripeCount(lm.mux, 2), "other groups' stripes must survive")

		// Idempotent second removal.
		lm.mux.removeGroup(1)

		// The surviving group keeps flowing; the removed group re-creates its
		// stripe on the next Send (groups restart on tenant re-load).
		lm.mux.Send(2, []raftpb.Message{bulkMsgApp(to, 3, 1024)})
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 4, 1024)})
		require.Eventually(t, func() bool {
			idx := peer.appIndexes()
			return slices.Contains(idx, uint64(3)) && slices.Contains(idx, uint64(4))
		}, 2*time.Second, 10*time.Millisecond)
		require.Equal(t, 1, bulkStripeCount(lm.mux, 1), "a fresh Send must re-create the stripe")
	})

	t.Run("unparks a writer stalled mid-frame; frames accounted exactly once", func(t *testing.T) {
		peer := startTestYamuxPeer(t, true, false) // parks the stream at its first MsgApp
		lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peer.addr})
		t.Cleanup(peer.stop)
		to := lm.nodeIDs.register("peer-b")

		// First frame is consumed (parking the stream); the second — larger
		// than any window — parks the writer mid-Write; two more queue behind
		// it.
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 64<<10)})
		require.Eventually(t, func() bool { return peer.appCount() >= 1 },
			2*time.Second, 10*time.Millisecond)
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 2, 6<<20)})
		time.Sleep(100 * time.Millisecond) // let the writer park mid-frame
		removed := dropDelta("send_group_removed")
		writeErr := dropDelta("send_write_error")
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 3, 1024)})
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 4, 1024)})

		// Removal discards the queued frames (send_group_removed) and closes
		// the stream out from under the parked writer, whose in-flight frame
		// then fails its write (send_write_error) — the double-close race the
		// stream helpers exist for. Every undelivered frame is accounted at
		// exactly one of the two sites.
		lm.mux.removeGroup(1)
		require.Equal(t, 0, bulkStripeCount(lm.mux, 1))
		require.Eventually(t, func() bool { return removed()+writeErr() == 3 },
			3*time.Second, 10*time.Millisecond,
			"3 undelivered frames must be accounted exactly once across send_group_removed and send_write_error")

		// The unparked writer must be fully reaped: Close stays bounded.
		closed := make(chan struct{})
		go func() {
			lm.close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return after removeGroup unparked the stalled writer")
		}
	})
}

// TestMuxTransport_BulkOverflowDropsAndRecovers pins the overflow contract:
// with a peer reading nothing, pushing more than the bulk lane holds must
// (a) never block the Send caller, (b) count drops at the bulk overflow
// site, and (c) deliver everything that was not dropped — in order — once
// the peer resumes reading, plus fresh traffic afterwards.
func TestMuxTransport_BulkOverflowDropsAndRecovers(t *testing.T) {
	peer := startTestYamuxPeer(t, false, true) // gated: reads nothing until release
	lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peer.addr})
	t.Cleanup(peer.stop) // LIFO: unblock parked writers before the mux closes
	to := lm.nodeIDs.register("peer-b")

	const sent = 20
	drops := dropDelta("send_bulk_queue_full")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= sent; i++ {
			lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, uint64(i), 2<<20)})
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Send blocked while the peer stalled; enqueue must stay non-blocking")
	}
	dropped := drops()
	require.Greater(t, dropped, float64(0), "overflow must be counted at the bulk drop site")

	peer.release()
	want := sent - int(dropped)
	require.Eventually(t, func() bool { return peer.appCount() == want },
		10*time.Second, 20*time.Millisecond,
		"every frame not dropped must be delivered once the peer resumes")

	// Undropped traffic must arrive in send order (drop-newest never reorders).
	idx := peer.appIndexes()
	for i := 1; i < len(idx); i++ {
		require.Greater(t, idx[i], idx[i-1], "delivered MsgApps reordered")
	}

	// The lane must accept and deliver fresh traffic after recovery.
	lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 9999, 1024)})
	require.Eventually(t, func() bool {
		idx := peer.appIndexes()
		return len(idx) > 0 && idx[len(idx)-1] == 9999
	}, 5*time.Second, 20*time.Millisecond, "post-recovery send must be delivered")
}

// TestMuxTransport_CloseWithStalledPeer pins bounded shutdown: Close must
// return promptly (no leak, no panic) while a peer write is parked on a full
// window and while a dial is hung.
func TestMuxTransport_CloseWithStalledPeer(t *testing.T) {
	t.Run("writer parked on a stalled stream", func(t *testing.T) {
		peerAddr, stopPeer := silentTCPPeer(t)
		lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": peerAddr})
		t.Cleanup(stopPeer)
		to := lm.nodeIDs.register("peer-b")

		go func() {
			for i := 1; i <= 4; i++ {
				lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, uint64(i), 1<<20)})
			}
		}()
		time.Sleep(200 * time.Millisecond) // let the write park

		closed := make(chan struct{})
		go func() {
			lm.close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return with a writer parked on a stalled stream")
		}
	})

	t.Run("bulk connection stalled at the TCP level, priority healthy", func(t *testing.T) {
		peer := startTestYamuxPeerOpts(t, yamuxPeerOpts{stallConnMinAppBytes: 1 << 20, readBuf: 64 << 10})
		lm := newLaneMux(t, 20*time.Millisecond, map[string]string{"peer-b": peer.addr})
		t.Cleanup(peer.stop)
		to := lm.nodeIDs.register("peer-b")

		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 64<<10)})
		require.Eventually(t, func() bool { return peer.appCount() >= 1 },
			2*time.Second, 10*time.Millisecond)
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 2, 2<<20)}) // trips the connection
		require.Eventually(t, func() bool { return peer.appCount() >= 2 },
			2*time.Second, 10*time.Millisecond)
		lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 3, 4<<20)}) // parks the writer mid-frame
		time.Sleep(200 * time.Millisecond)                         // let the write park

		closed := make(chan struct{})
		go func() {
			lm.close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return with the bulk connection TCP-stalled")
		}
	})

	t.Run("writer parked in a hung dial", func(t *testing.T) {
		bAddr := "127.0.0.1:9"
		lm := newLaneMux(t, time.Hour, map[string]string{"peer-b": bAddr})
		lm.mux.dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		to := lm.nodeIDs.register("peer-b")

		go func() {
			lm.mux.Send(1, []raftpb.Message{bulkMsgApp(to, 1, 1024)})
		}()
		time.Sleep(100 * time.Millisecond) // let the send reach the dial

		closed := make(chan struct{})
		go func() {
			lm.close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return with a hung dial in flight")
		}
	})
}

// TestIsPriorityMsg pins the lane classification: heartbeat- and vote-class
// traffic (plus MsgTimeoutNow) rides the priority lane; everything else —
// notably high-rate MsgAppResp and bulky MsgApp/MsgSnap — stays on bulk.
func TestIsPriorityMsg(t *testing.T) {
	tests := []struct {
		typ  raftpb.MessageType
		prio bool
	}{
		{raftpb.MsgHeartbeat, true},
		{raftpb.MsgHeartbeatResp, true},
		{raftpb.MsgVote, true},
		{raftpb.MsgPreVote, true},
		{raftpb.MsgVoteResp, true},
		{raftpb.MsgPreVoteResp, true},
		{raftpb.MsgTimeoutNow, true},
		{raftpb.MsgApp, false},
		{raftpb.MsgAppResp, false},
		{raftpb.MsgSnap, false},
		{raftpb.MsgReadIndex, false},
		{raftpb.MsgReadIndexResp, false},
		{raftpb.MsgTransferLeader, false},
	}
	for _, tc := range tests {
		require.Equalf(t, tc.prio, isPriorityMsg(tc.typ), "classification for %s", tc.typ)
	}
}

// laneHarness builds the minimal transport wiring enqueueFrame needs.
func laneHarness(maxBytes, maxMsgs int, dropSite string) (*MuxTransport, *peerSender, *sendLane) {
	logger, _ := test.NewNullLogger()
	m := &MuxTransport{logger: logger, dropLog: newLogLimiter(time.Second)}
	ps := &peerSender{label: "peer-x"}
	return m, ps, newSendLane(laneClassBulk, maxBytes, maxMsgs, dropSite)
}

// TestSendLane_BoundsAndOrder pins the lane queue contract: strict FIFO for
// everything accepted, byte and count bounds enforced at enqueue, and
// drop-newest on overflow (the queued prefix survives intact).
func TestSendLane_BoundsAndOrder(t *testing.T) {
	frame := func(n int) []byte { return make([]byte, n) }
	tests := []struct {
		name      string
		maxBytes  int
		maxMsgs   int
		enqueue   []int // frame sizes, in order
		wantSizes []int // sizes left queued, in order
		wantDrops float64
	}{
		{
			name:      "all frames fit and keep order",
			maxBytes:  100,
			maxMsgs:   10,
			enqueue:   []int{10, 20, 30},
			wantSizes: []int{10, 20, 30},
			wantDrops: 0,
		},
		{
			name:      "byte bound drops the newest, prefix intact",
			maxBytes:  50,
			maxMsgs:   10,
			enqueue:   []int{20, 20, 20, 5},
			wantSizes: []int{20, 20, 5}, // third frame dropped; smaller later frame still fits
			wantDrops: 1,
		},
		{
			name:      "count bound drops the newest, prefix intact",
			maxBytes:  0, // unbounded bytes (priority-lane shape)
			maxMsgs:   2,
			enqueue:   []int{1, 2, 3, 4},
			wantSizes: []int{1, 2},
			wantDrops: 2,
		},
		{
			// The wedge mechanism behind the Store.Apply size guard: a frame
			// larger than the byte bound can never enqueue, even into an empty
			// lane — upstream raft would re-send it forever. The guard exists
			// because of this permanent-drop shape.
			name:      "over-bound frame dropped even from an empty lane",
			maxBytes:  50,
			maxMsgs:   10,
			enqueue:   []int{60},
			wantSizes: nil,
			wantDrops: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const site = "send_bulk_queue_full"
			m, ps, lane := laneHarness(tc.maxBytes, tc.maxMsgs, site)
			drops := dropDelta(site)
			for _, n := range tc.enqueue {
				m.enqueueFrame(ps, lane, frame(n))
			}
			require.Equal(t, tc.wantDrops, drops(), "overflow drops")

			got := lane.popAll(nil)
			var sizes []int
			for _, f := range got {
				sizes = append(sizes, len(f.frame))
			}
			require.Equal(t, tc.wantSizes, sizes, "queued frames (FIFO, drop-newest)")

			// popAll must fully reset the queue and its byte accounting.
			lane.mu.Lock()
			require.Empty(t, lane.q)
			require.Zero(t, lane.qBytes)
			lane.mu.Unlock()
		})
	}
}

// TestSendLane_ClosedLaneCountsShutdownDrops pins the shutdown ledger: a
// frame enqueued after the lane writer discarded its queue is counted at
// send_shutdown, never silently stranded.
func TestSendLane_ClosedLaneCountsShutdownDrops(t *testing.T) {
	m, ps, lane := laneHarness(100, 10, "send_bulk_queue_full")
	m.enqueueFrame(ps, lane, make([]byte, 10))

	discarded := dropDelta("send_shutdown")
	lane.closeAndDiscard(dropSiteSendShutdown)
	require.Equal(t, float64(1), discarded(), "queued frame must be counted at discard")

	m.enqueueFrame(ps, lane, make([]byte, 10))
	require.Equal(t, float64(2), discarded(), "post-close enqueue must be counted, not stranded")

	lane.mu.Lock()
	require.Empty(t, lane.q, "closed lane must not accumulate frames")
	lane.mu.Unlock()
}
