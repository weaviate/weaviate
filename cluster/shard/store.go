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
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// nRetainedSnapshots is the number of snapshots to retain per shard.
	nRetainedSnapshots = 3

	// defaultTickInterval is how often the Ready loop ticks the RawNode.
	defaultTickInterval = 100 * time.Millisecond

	// defaultHeartbeatTicks / defaultElectionTicks are the etcd/raft tick
	// counts used when no duration is configured (≈200ms heartbeat, ≈1s
	// election at the default 100ms tick).
	defaultHeartbeatTicks = 2
	defaultElectionTicks  = 10

	// defaultSnapshotThreshold is the applied-index delta that triggers a
	// snapshot when StoreConfig.SnapshotThreshold is unset.
	defaultSnapshotThreshold = 8192

	// defaultMaxSizePerMsg / defaultMaxInflightMsgs size etcd/raft replication
	// batches — tuned against the replicator's 2MB chunk size.
	defaultMaxSizePerMsg   = 2 * 1024 * 1024
	defaultMaxInflightMsgs = 256

	// proposeChanSize / incomingMsgChanSize buffer the Ready loop's inbound
	// channels. raft tolerates message loss, so overflow simply drops.
	proposeChanSize     = 64
	incomingMsgChanSize = 256
)

var (
	// ErrNotLeader is returned when an operation is attempted on a non-leader node.
	ErrNotLeader = errors.New("not leader")

	// ErrLeadershipLost is returned to a pending Apply when this node loses
	// leadership before the proposed command commits.
	ErrLeadershipLost = errors.New("leadership lost")

	// ErrNotStarted is returned when an operation is attempted before the cluster is started.
	ErrNotStarted = errors.New("raft cluster not started")

	// ErrAlreadyClosed is returned when an operation is attempted on a closed cluster.
	ErrAlreadyClosed = errors.New("raft cluster already closed")

	// ErrLeaderElectionTimeout is returned when the store cannot observe a
	// leader before the caller's context deadline expires.
	ErrLeaderElectionTimeout = errors.New("timed out waiting for shard raft leader election")
)

// ShardRaftState is this node's role in a shard's RAFT cluster. It replaces the
// leaked hashicorp raft.RaftState in the public Store API.
type ShardRaftState uint32

const (
	ShardStateFollower ShardRaftState = iota
	ShardStateCandidate
	ShardStateLeader
	ShardStateShutdown
)

func (s ShardRaftState) String() string {
	switch s {
	case ShardStateFollower:
		return "Follower"
	case ShardStateCandidate:
		return "Candidate"
	case ShardStateLeader:
		return "Leader"
	case ShardStateShutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// StoreConfig holds configuration for a shard's RAFT cluster.
type StoreConfig struct {
	// ClassName is the name of the class this shard belongs to.
	ClassName string
	// ShardName is the name of the shard.
	ShardName string
	// NodeID is the local node's identifier.
	NodeID string
	// Members is the list of node IDs that are members of this shard's RAFT cluster.
	Members []string
	// Logger is the logger to use.
	Logger *logrus.Logger
	// TickInterval is how often the Ready loop ticks the RawNode.
	TickInterval time.Duration

	// Transport delivers raft messages to peer nodes. In production this is
	// the node's MuxTransport; in tests it is a MemTransport.
	Transport Transport
	// SharedLog is the node-wide multi-group raft log.
	SharedLog *sharedlog.Store
	// Snapshotter is the node-wide bounded snapshot worker pool.
	Snapshotter *Snapshotter
	// NodeIDs translates between string node IDs and etcd/raft uint64 IDs.
	// If nil, a private map is created (single-node use only).
	NodeIDs *nodeIDMap
	// Resolver resolves node IDs to host addresses (for Leader()).
	Resolver addressResolver

	// RAFT timing configuration.
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	// SnapshotThreshold is the applied-index delta that triggers a snapshot.
	SnapshotThreshold uint64
}

// Response is the result of applying a command to the FSM.
type Response struct {
	Error   error
	Version uint64
}

// applyResult carries a committed command's outcome back to a pending Apply.
type applyResult struct {
	idx uint64
	err error
}

// pendingApply correlates one in-flight Apply with its committed entry.
type pendingApply struct {
	done chan applyResult
}

// proposal is one command queued from Apply onto the Ready loop.
type proposal struct {
	reqID uint64
	data  []byte
}

// Store manages a RAFT cluster for a single physical shard. Each shard has its
// own etcd/raft group; membership equals the shard's replica nodes
// (Physical.BelongsToNodes). The public API is library-agnostic.
type Store struct {
	config  StoreConfig
	log     logrus.FieldLogger
	groupID uint64

	fsm         *FSM
	transport   Transport
	sharedLog   *sharedlog.Store
	snapshotter *Snapshotter
	nodeIDs     *nodeIDMap
	resolver    addressResolver

	// raftStorage is this group's view of the shared log; it is the
	// RawNode's raft.Storage.
	raftStorage  raft.Storage
	tickInterval time.Duration

	// rawNode and the Ready-loop channels are owned by the run() goroutine
	// after Start; only run() touches rawNode (it is not thread-safe).
	rawNode       *raft.RawNode
	proposeCh     chan proposal
	incomingMsgCh chan raftpb.Message
	snapResultCh  chan SnapshotResult
	loopCtx       context.Context
	loopCancel    context.CancelFunc
	loopDone      chan struct{}

	// confState / lastSnapshotIndex / snapshotPending are Ready-loop-local.
	confState         raftpb.ConfState
	lastSnapshotIndex uint64
	snapshotPending   bool

	// leadership snapshots, written by the Ready loop, read by accessors.
	state    atomic.Uint32 // ShardRaftState
	leaderID atomic.Uint64
	leaderCh chan struct{}

	// pending correlates Apply reqIDs with their committed entries.
	pending   map[uint64]*pendingApply
	pendingMu sync.Mutex
	nextReqID atomic.Uint64

	mu      sync.RWMutex
	started bool
	closed  bool
}

// NewStore creates a new RAFT cluster for a shard. The cluster is not started
// until Start() is called.
func NewStore(config StoreConfig) (*Store, error) {
	if config.TickInterval <= 0 {
		config.TickInterval = defaultTickInterval
	}
	if config.SharedLog == nil {
		return nil, fmt.Errorf("shard store: SharedLog is required")
	}
	if config.Snapshotter == nil {
		return nil, fmt.Errorf("shard store: Snapshotter is required")
	}
	if config.Transport == nil {
		return nil, fmt.Errorf("shard store: Transport is required")
	}
	nodeIDs := config.NodeIDs
	if nodeIDs == nil {
		nodeIDs = newNodeIDMap()
	}

	groupID := hashGroupID(config.ClassName, config.ShardName)

	log := config.Logger.WithFields(logrus.Fields{
		"component": "shard_raft_store",
		"class":     config.ClassName,
		"shard":     config.ShardName,
		"group":     groupID,
	})

	return &Store{
		config:       config,
		log:          log,
		groupID:      groupID,
		fsm:          NewFSM(config.ClassName, config.ShardName, config.NodeID, config.Logger),
		transport:    config.Transport,
		sharedLog:    config.SharedLog,
		snapshotter:  config.Snapshotter,
		nodeIDs:      nodeIDs,
		resolver:     config.Resolver,
		raftStorage:  config.SharedLog.Storage(groupID),
		tickInterval: config.TickInterval,
		leaderCh:     make(chan struct{}, 1),
		pending:      make(map[uint64]*pendingApply),
	}, nil
}

// GroupID returns this shard's etcd/raft group identifier.
func (s *Store) GroupID() uint64 { return s.groupID }

// SetShard sets the shard operator that will process commands.
// This must be called before Start().
func (s *Store) SetShard(shard shard) {
	s.fsm.SetShard(shard)
}

// CreateTransferSnapshot delegates to the underlying shard to create a
// hardlink snapshot for out-of-band state transfer.
func (s *Store) CreateTransferSnapshot(ctx context.Context) (TransferSnapshot, error) {
	sh := s.fsm.getShard()
	if sh == nil {
		return TransferSnapshot{}, fmt.Errorf("shard not set")
	}
	return sh.CreateTransferSnapshot(ctx)
}

// ReleaseTransferSnapshot delegates to the underlying shard to clean up a
// transfer snapshot's staging directory.
func (s *Store) ReleaseTransferSnapshot(snapshotID string) error {
	sh := s.fsm.getShard()
	if sh == nil {
		return fmt.Errorf("shard not set")
	}
	return sh.ReleaseTransferSnapshot(snapshotID)
}

// SetStateTransferer sets the state transferrer on the FSM.
func (s *Store) SetStateTransferer(st StateTransferer) {
	s.fsm.SetStateTransferer(st)
}

// Start initializes and starts the RAFT cluster.
func (s *Store) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}
	if s.closed {
		return ErrAlreadyClosed
	}

	s.log.Info("starting shard RAFT store")

	// Seed the nodeID map's reverse table for every member before driving the
	// RawNode, so any uint64 etcd hands back (Status().Lead, ConfState.Voters,
	// Message.From) can be un-hashed to a string node ID.
	for _, m := range s.config.Members {
		s.nodeIDs.register(m)
	}
	localID := s.nodeIDs.register(s.config.NodeID)

	hasGroup, err := s.sharedLog.HasGroup(s.groupID)
	if err != nil {
		return fmt.Errorf("check existing group: %w", err)
	}

	rn, err := raft.NewRawNode(s.raftConfig(localID))
	if err != nil {
		return fmt.Errorf("new raw node: %w", err)
	}

	if !hasGroup {
		peers := make([]raft.Peer, 0, len(s.config.Members))
		for _, m := range s.config.Members {
			peers = append(peers, raft.Peer{ID: s.nodeIDs.register(m)})
		}
		if err := rn.Bootstrap(peers); err != nil {
			return fmt.Errorf("bootstrap raft group: %w", err)
		}
		s.log.WithField("peers", len(peers)).Info("bootstrapped RAFT group")
	} else if snap, err := s.raftStorage.Snapshot(); err == nil && !raft.IsEmptySnap(snap) {
		// Restart from a local snapshot: re-seed the FSM's applied index so
		// WaitForAppliedIndex stays correct across restarts.
		var meta shardSnapshotData
		if len(snap.Data) > 0 && json.Unmarshal(snap.Data, &meta) == nil {
			if err := s.fsm.RestoreFromSnapshot(meta); err != nil {
				s.log.WithError(err).Warn("restore FSM from local snapshot")
			}
		}
		s.confState = snap.Metadata.ConfState
		s.lastSnapshotIndex = snap.Metadata.Index
	}

	s.rawNode = rn
	s.proposeCh = make(chan proposal, proposeChanSize)
	s.incomingMsgCh = make(chan raftpb.Message, incomingMsgChanSize)
	s.snapResultCh = make(chan SnapshotResult, 1)
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	s.loopDone = make(chan struct{})

	enterrors.GoWrapper(s.run, s.log)

	s.started = true
	s.log.Info("shard RAFT store started")
	return nil
}

// raftConfig builds the etcd/raft configuration for this group.
func (s *Store) raftConfig(localID uint64) *raft.Config {
	hb := ticksFromDuration(s.config.HeartbeatTimeout, s.tickInterval, defaultHeartbeatTicks)
	el := ticksFromDuration(s.config.ElectionTimeout, s.tickInterval, defaultElectionTicks)
	if el <= hb {
		el = hb + 1
	}

	var applied uint64
	if snap, err := s.raftStorage.Snapshot(); err == nil {
		applied = snap.Metadata.Index
	}

	return &raft.Config{
		ID:              localID,
		ElectionTick:    el,
		HeartbeatTick:   hb,
		Storage:         s.raftStorage,
		Applied:         applied,
		MaxSizePerMsg:   defaultMaxSizePerMsg,
		MaxInflightMsgs: defaultMaxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         true,
		Logger:          raftLogger{s.log},
	}
}

// ticksFromDuration converts a timeout duration to a tick count, falling back
// to def when the duration is unset.
func ticksFromDuration(d, tick time.Duration, def int) int {
	if d <= 0 || tick <= 0 {
		return def
	}
	n := int(d / tick)
	if n < 1 {
		n = 1
	}
	return n
}

// run is the Ready loop: the single goroutine that owns the RawNode. It ticks,
// drains Ready(), persists, transmits, applies committed entries, and Advances.
func (s *Store) run() {
	defer close(s.loopDone)

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.loopCtx.Done():
			return
		case <-ticker.C:
			s.rawNode.Tick()
		case m := <-s.incomingMsgCh:
			if err := s.rawNode.Step(m); err != nil {
				s.log.WithError(err).Debug("raft step")
			}
		case p := <-s.proposeCh:
			s.handlePropose(p)
		case r := <-s.snapResultCh:
			s.onSnapshotResult(r)
		}

		for s.rawNode.HasReady() {
			s.processReady()
		}
	}
}

// processReady drains one Ready: persist durably, install snapshots, send
// messages, apply committed entries, track leadership, Advance, then maybe
// trigger a new snapshot.
func (s *Store) processReady() {
	rd := s.rawNode.Ready()

	// 1. Persist HardState + Entries (+ Snapshot) durably before sending.
	if len(rd.Entries) > 0 || !raft.IsEmptyHardState(rd.HardState) || !raft.IsEmptySnap(rd.Snapshot) {
		gw := sharedlog.GroupWrite{GroupID: s.groupID, Entries: rd.Entries}
		if !raft.IsEmptyHardState(rd.HardState) {
			hs := rd.HardState
			gw.HardState = &hs
		}
		if !raft.IsEmptySnap(rd.Snapshot) {
			sn := rd.Snapshot
			gw.Snapshot = &sn
		}
		if err := s.sharedLog.Append(context.Background(), gw); err != nil {
			panic(fmt.Sprintf("shard raft %s/%s: durability invariant violated persisting raft state: %v",
				s.config.ClassName, s.config.ShardName, err))
		}
	}

	// 2. Install a received snapshot into the FSM.
	if !raft.IsEmptySnap(rd.Snapshot) {
		s.applySnapshot(rd.Snapshot)
	}

	// 3. Transmit outbound messages.
	if len(rd.Messages) > 0 {
		s.transport.Send(s.groupID, rd.Messages)
	}

	// 4. Apply committed entries to the FSM, wake pending Applies.
	s.applyEntries(rd.CommittedEntries)

	// 5. Track leadership changes.
	if rd.SoftState != nil {
		s.handleSoftState(rd.SoftState)
	}

	// 6. Advance and maybe snapshot.
	s.rawNode.Advance(rd)
	s.maybeSnapshot()
}

// applySnapshot installs a snapshot received from the leader: restore the FSM
// from its metadata and discard the now-stale log prefix.
func (s *Store) applySnapshot(snap raftpb.Snapshot) {
	if len(snap.Data) > 0 {
		var meta shardSnapshotData
		if err := json.Unmarshal(snap.Data, &meta); err != nil {
			s.log.WithError(err).Error("decode snapshot data")
		} else if err := s.fsm.RestoreFromSnapshot(meta); err != nil {
			s.log.WithError(err).Error("restore from snapshot")
		}
	}
	s.confState = snap.Metadata.ConfState
	if err := s.sharedLog.Compact(s.groupID, snap.Metadata.Index+1); err != nil {
		s.log.WithError(err).Warn("compact log after snapshot install")
	}
	s.lastSnapshotIndex = snap.Metadata.Index
}

// applyEntries dispatches committed entries to the FSM and wakes pending Applies.
func (s *Store) applyEntries(entries []raftpb.Entry) {
	for i := range entries {
		ent := entries[i]
		switch ent.Type {
		case raftpb.EntryNormal:
			if len(ent.Data) == 0 {
				// Empty leader entry (no-op on election) — advance index only.
				s.fsm.setApplied(ent.Index)
				continue
			}
			reqID, payload, ok := decodeCmd(ent.Data)
			if !ok {
				s.log.WithField("index", ent.Index).Error("malformed command entry, skipping")
				s.fsm.setApplied(ent.Index)
				continue
			}
			resp := s.fsm.Dispatch(payload, ent.Index)
			s.wakePending(reqID, applyResult{idx: ent.Index, err: resp.Error})
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ent.Data); err != nil {
				s.log.WithError(err).Error("unmarshal conf change")
			} else if cs := s.rawNode.ApplyConfChange(cc); cs != nil {
				s.confState = *cs
			}
			s.fsm.setApplied(ent.Index)
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(ent.Data); err != nil {
				s.log.WithError(err).Error("unmarshal conf change v2")
			} else if cs := s.rawNode.ApplyConfChange(cc); cs != nil {
				s.confState = *cs
			}
			s.fsm.setApplied(ent.Index)
		default:
			s.fsm.setApplied(ent.Index)
		}
	}
}

// handleSoftState records a leadership change and wakes waiters; on losing
// leadership it fails all pending Applies.
func (s *Store) handleSoftState(ss *raft.SoftState) {
	prev := ShardRaftState(s.state.Load())
	next := mapRaftState(ss.RaftState)
	s.state.Store(uint32(next))

	prevLeader := s.leaderID.Load()
	s.leaderID.Store(ss.Lead)
	if ss.Lead != prevLeader {
		select {
		case s.leaderCh <- struct{}{}:
		default:
		}
	}

	if prev == ShardStateLeader && next != ShardStateLeader {
		s.drainPending(ErrLeadershipLost)
	}
}

// handlePropose proposes a queued command, or fails it fast if not leader.
func (s *Store) handlePropose(p proposal) {
	if ShardRaftState(s.state.Load()) != ShardStateLeader {
		s.wakePending(p.reqID, applyResult{err: ErrNotLeader})
		return
	}
	if err := s.rawNode.Propose(p.data); err != nil {
		s.wakePending(p.reqID, applyResult{err: ErrNotLeader})
	}
}

// maybeSnapshot dispatches a snapshot job once the applied index has advanced
// far enough past the last snapshot. Snapshots are advisory: if the pool is
// busy the attempt simply retries on a later round.
func (s *Store) maybeSnapshot() {
	if s.snapshotPending {
		return
	}
	threshold := s.config.SnapshotThreshold
	if threshold == 0 {
		threshold = defaultSnapshotThreshold
	}
	applied := s.fsm.LastAppliedIndex()
	if applied < s.lastSnapshotIndex+threshold {
		return
	}
	sh := s.fsm.getShard()
	if sh == nil {
		return
	}
	err := s.snapshotter.Submit(SnapshotRequest{
		GroupID:      s.groupID,
		ClassName:    s.config.ClassName,
		ShardName:    s.config.ShardName,
		NodeID:       s.config.NodeID,
		AppliedIndex: applied,
		Flusher:      sh,
		Result:       s.snapResultCh,
	})
	if err != nil {
		return // busy/closed — retry next round
	}
	s.snapshotPending = true
}

// onSnapshotResult records a completed snapshot in raft storage and truncates
// the log. It runs single-threaded on the Ready loop.
func (s *Store) onSnapshotResult(r SnapshotResult) {
	s.snapshotPending = false
	if r.Err != nil {
		s.log.WithError(r.Err).WithField("index", r.Index).Warn("snapshot job failed")
		return
	}
	term, err := s.raftStorage.Term(r.Index)
	if err != nil {
		s.log.WithError(err).WithField("index", r.Index).Warn("snapshot index already compacted, skipping")
		return
	}
	snap := raftpb.Snapshot{
		Data: r.Metadata,
		Metadata: raftpb.SnapshotMetadata{
			Index:     r.Index,
			Term:      term,
			ConfState: s.confState,
		},
	}
	if err := s.sharedLog.Append(context.Background(), sharedlog.GroupWrite{
		GroupID:  s.groupID,
		Snapshot: &snap,
	}); err != nil {
		panic(fmt.Sprintf("shard raft %s/%s: durability invariant violated persisting snapshot: %v",
			s.config.ClassName, s.config.ShardName, err))
	}
	if err := s.sharedLog.Compact(s.groupID, r.Index+1); err != nil {
		s.log.WithError(err).Warn("compact log after snapshot")
	}
	s.lastSnapshotIndex = r.Index
}

// wakePending delivers a result to a waiting Apply, if one is registered.
// The send is non-blocking: a result may already have been delivered (e.g. a
// leadership-loss drain racing a commit), in which case this is a no-op.
func (s *Store) wakePending(reqID uint64, res applyResult) {
	s.pendingMu.Lock()
	p, ok := s.pending[reqID]
	s.pendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case p.done <- res:
	default:
	}
}

// drainPending fails every in-flight Apply with err.
func (s *Store) drainPending(err error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for _, p := range s.pending {
		select {
		case p.done <- applyResult{err: err}:
		default:
		}
	}
}

// step hands an inbound raft message to the Ready loop. Called by the
// Registry's MessageRouter. Non-blocking: a message that arrives before Start
// or after Stop, or when the queue is full, is dropped — raft re-sends.
func (s *Store) step(msg raftpb.Message) {
	s.mu.RLock()
	ch := s.incomingMsgCh
	live := s.started && !s.closed
	s.mu.RUnlock()
	if !live || ch == nil {
		return
	}
	select {
	case ch <- msg:
	default:
		s.log.WithField("type", msg.Type).Warn("dropping inbound raft message: queue full")
	}
}

// Stop gracefully stops the RAFT cluster.
func (s *Store) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if !s.started {
		s.closed = true
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	loopCancel := s.loopCancel
	loopDone := s.loopDone
	s.mu.Unlock()

	s.log.Info("stopping shard RAFT store")

	loopCancel()
	<-loopDone

	// Fail any Apply still waiting on a commit the (now stopped) loop will
	// never deliver.
	s.drainPending(ErrAlreadyClosed)

	s.mu.Lock()
	s.started = false
	s.mu.Unlock()

	s.log.Info("shard RAFT store stopped")
	return nil
}

// Apply applies a command to the RAFT cluster. It blocks until the command is
// committed and applied locally, or the context is cancelled.
func (s *Store) Apply(ctx context.Context, req *shardproto.ApplyRequest) (uint64, error) {
	s.mu.RLock()
	started, closed := s.started, s.closed
	s.mu.RUnlock()
	if !started {
		return 0, ErrNotStarted
	}
	if closed {
		return 0, ErrAlreadyClosed
	}

	body, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal command: %w", err)
	}

	reqID := s.nextReqID.Add(1)
	p := &pendingApply{done: make(chan applyResult, 1)}
	s.pendingMu.Lock()
	s.pending[reqID] = p
	s.pendingMu.Unlock()
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, reqID)
		s.pendingMu.Unlock()
	}()

	select {
	case s.proposeCh <- proposal{reqID: reqID, data: encodeCmd(reqID, body)}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.loopDone:
		return 0, ErrAlreadyClosed
	}

	select {
	case r := <-p.done:
		return r.idx, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.loopDone:
		return 0, ErrAlreadyClosed
	}
}

// IsLeader returns true if this node is the leader of the shard's RAFT cluster.
func (s *Store) IsLeader() bool {
	s.mu.RLock()
	started, closed := s.started, s.closed
	s.mu.RUnlock()
	if !started || closed {
		return false
	}
	return ShardRaftState(s.state.Load()) == ShardStateLeader
}

// Leader returns the current leader's address, or empty string if unknown.
func (s *Store) Leader() string {
	id := s.LeaderID()
	if id == "" {
		return ""
	}
	if s.resolver == nil {
		return id
	}
	if addr := s.resolver.NodeAddress(id); addr != "" {
		return addr
	}
	return id
}

// LeaderID returns the current leader's node ID, or empty string if unknown.
func (s *Store) LeaderID() string {
	s.mu.RLock()
	started, closed := s.started, s.closed
	s.mu.RUnlock()
	if !started || closed {
		return ""
	}
	id := s.leaderID.Load()
	if id == 0 {
		return ""
	}
	str, ok := s.nodeIDs.stringID(id)
	if !ok {
		return ""
	}
	return str
}

// VerifyLeader ensures this node is still the leader. Used for linearizable
// reads. Correctness rests on CheckQuorum: a leader that has lost contact with
// a majority steps itself down within ~election-timeout.
func (s *Store) VerifyLeader() error {
	s.mu.RLock()
	started, closed := s.started, s.closed
	s.mu.RUnlock()
	if closed {
		return ErrAlreadyClosed
	}
	if !started {
		return ErrNotStarted
	}
	if ShardRaftState(s.state.Load()) != ShardStateLeader {
		return ErrNotLeader
	}
	return nil
}

// State returns the current RAFT state of this node.
func (s *Store) State() ShardRaftState {
	s.mu.RLock()
	started, closed := s.started, s.closed
	s.mu.RUnlock()
	if !started || closed {
		return ShardStateShutdown
	}
	return ShardRaftState(s.state.Load())
}

// LastAppliedIndex returns the last applied RAFT log index.
func (s *Store) LastAppliedIndex() uint64 {
	return s.fsm.LastAppliedIndex()
}

// WaitForAppliedIndex blocks until the local FSM has applied at least
// targetIndex, or the context is cancelled.
func (s *Store) WaitForAppliedIndex(ctx context.Context, targetIndex uint64) error {
	return s.fsm.WaitForIndex(ctx, targetIndex)
}

// WaitForLeader blocks until this node observes a leader for the shard's RAFT
// cluster (either local or remote), or the context is cancelled.
func (s *Store) WaitForLeader(ctx context.Context) error {
	s.mu.RLock()
	started, closed := s.started, s.closed
	loopDone := s.loopDone
	s.mu.RUnlock()
	if closed {
		return ErrAlreadyClosed
	}
	if !started {
		return ErrNotStarted
	}

	if s.leaderID.Load() != 0 {
		return nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrLeaderElectionTimeout
			}
			return ctx.Err()
		case <-s.leaderCh:
			if s.leaderID.Load() != 0 {
				return nil
			}
		case <-ticker.C:
			if s.leaderID.Load() != 0 {
				return nil
			}
		case <-loopDone:
			return ErrAlreadyClosed
		}
	}
}

// mapRaftState maps an etcd/raft StateType to a ShardRaftState.
func mapRaftState(st raft.StateType) ShardRaftState {
	switch st {
	case raft.StateLeader:
		return ShardStateLeader
	case raft.StateCandidate, raft.StatePreCandidate:
		return ShardStateCandidate
	default:
		return ShardStateFollower
	}
}

// encodeCmd prefixes a marshalled command with its 8-byte request ID so the
// Ready loop can correlate the committed entry back to the waiting Apply.
func encodeCmd(reqID uint64, body []byte) []byte {
	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint64(out[:8], reqID)
	copy(out[8:], body)
	return out
}

// decodeCmd splits an entry into its request ID and command payload.
func decodeCmd(data []byte) (reqID uint64, payload []byte, ok bool) {
	if len(data) < 8 {
		return 0, nil, false
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], true
}

// hashGroupID derives a deterministic uint64 group ID from a class/shard pair,
// mirroring hashNodeID (0 is bumped to 1 since etcd/raft has no group 0 reserved
// but 0 is a convenient "unset" sentinel for the Registry's router table).
func hashGroupID(className, shardName string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(className))
	_, _ = h.Write([]byte{'/'})
	_, _ = h.Write([]byte(shardName))
	v := h.Sum64()
	if v == 0 {
		return 1
	}
	return v
}

// raftLogger adapts a logrus.FieldLogger to etcd/raft's Logger interface.
type raftLogger struct{ l logrus.FieldLogger }

func (r raftLogger) Debug(v ...interface{})              { r.l.Debug(v...) }
func (r raftLogger) Debugf(f string, v ...interface{})   { r.l.Debugf(f, v...) }
func (r raftLogger) Info(v ...interface{})               { r.l.Info(v...) }
func (r raftLogger) Infof(f string, v ...interface{})    { r.l.Infof(f, v...) }
func (r raftLogger) Warning(v ...interface{})            { r.l.Warn(v...) }
func (r raftLogger) Warningf(f string, v ...interface{}) { r.l.Warnf(f, v...) }
func (r raftLogger) Error(v ...interface{})              { r.l.Error(v...) }
func (r raftLogger) Errorf(f string, v ...interface{})   { r.l.Errorf(f, v...) }
func (r raftLogger) Fatal(v ...interface{})              { r.l.Fatal(v...) }
func (r raftLogger) Fatalf(f string, v ...interface{})   { r.l.Fatalf(f, v...) }
func (r raftLogger) Panic(v ...interface{})              { r.l.Panic(v...) }
func (r raftLogger) Panicf(f string, v ...interface{})   { r.l.Panicf(f, v...) }

// memTransportInboxSize is generous so that, in practice, tests never hit the
// drop path. Dropping is still correct (raft re-sends) — the buffer just
// keeps unrelated tests deterministic.
const memTransportInboxSize = 1024

// MemNetwork connects a set of MemTransports in-process, with no sockets.
// Tests build one network, then one MemTransport per node.
type MemNetwork struct {
	mu    sync.RWMutex
	nodes map[uint64]*MemTransport
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: make(map[uint64]*MemTransport)}
}

// NewTransport registers nodeID on the network and starts its delivery
// goroutine. router receives every message addressed to nodeID.
func (n *MemNetwork) NewTransport(nodeID uint64, router MessageRouter, logger logrus.FieldLogger) *MemTransport {
	t := &MemTransport{
		net:    n,
		nodeID: nodeID,
		router: router,
		log: logger.WithFields(logrus.Fields{
			"component": "shard_mem_transport",
			"node":      nodeID,
		}),
		inbox: make(chan inboundMsg, memTransportInboxSize),
		done:  make(chan struct{}),
	}

	n.mu.Lock()
	n.nodes[nodeID] = t
	n.mu.Unlock()

	t.wg.Add(1)
	enterrors.GoWrapper(t.deliverLoop, t.log)
	return t
}

func (n *MemNetwork) transport(nodeID uint64) (*MemTransport, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	t, ok := n.nodes[nodeID]
	return t, ok
}

func (n *MemNetwork) remove(nodeID uint64) {
	n.mu.Lock()
	delete(n.nodes, nodeID)
	n.mu.Unlock()
}

type inboundMsg struct {
	groupID uint64
	msg     raftpb.Message
}

// MemTransport is an in-process Transport — the etcd/raft equivalent of
// hashicorp's NewInmemTransport. Messages reach the destination node's
// router over a channel rather than a socket.
type MemTransport struct {
	net    *MemNetwork
	nodeID uint64
	router MessageRouter
	log    logrus.FieldLogger

	inbox chan inboundMsg

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

func (t *MemTransport) Send(groupID uint64, msgs []raftpb.Message) {
	for _, msg := range msgs {
		dst, ok := t.net.transport(msg.To)
		if !ok {
			t.log.WithField("to", msg.To).Warn("dropping message: unknown destination")
			continue
		}
		select {
		case dst.inbox <- inboundMsg{groupID: groupID, msg: msg}:
		case <-dst.done:
			// destination is shutting down; drop — raft tolerates loss.
		default:
			t.log.WithField("to", msg.To).Warn("dropping message: destination inbox full")
		}
	}
}

func (t *MemTransport) deliverLoop() {
	defer t.wg.Done()
	for {
		select {
		case <-t.done:
			return
		case in := <-t.inbox:
			if err := t.router.RouteMessage(in.groupID, in.msg); err != nil {
				t.log.WithField("group", in.groupID).WithError(err).Warn("route inbound message")
			}
		}
	}
}

// Close stops the delivery goroutine and unregisters the node. Idempotent.
// Undelivered inbox messages are discarded — raft re-sends on the next tick.
func (t *MemTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.done)
		t.net.remove(t.nodeID)
	})
	t.wg.Wait()
	return nil
}

// nodeIDMap translates between Weaviate's string node IDs and the uint64 IDs
// etcd/raft requires. The string -> uint64 direction is a deterministic
// FNV-1a hash, so it is stable across restarts and needs no persistence; only
// the uint64 -> string reverse lookup needs state, filled lazily as IDs are
// registered.
type nodeIDMap struct {
	mu       sync.RWMutex
	toString map[uint64]string
}

func newNodeIDMap() *nodeIDMap {
	return &nodeIDMap{toString: make(map[uint64]string)}
}

// hashNodeID maps a node-ID string to a uint64. etcd/raft reserves 0 as
// raft.None (no node), so a 0 hash is bumped to 1 — a genuine string that
// also hashes to 1 is then caught by collision detection in register.
func hashNodeID(nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	v := h.Sum64()
	if v == 0 {
		return 1
	}
	return v
}

// register returns the uint64 ID for nodeID, recording the reverse mapping.
// Idempotent. Panics on hash collision (two distinct strings -> same uint64):
// the 2^64 space makes that astronomically unlikely, and it would be an
// unrecoverable cluster-config bug rather than a runtime condition to handle.
func (m *nodeIDMap) register(nodeID string) uint64 {
	id := hashNodeID(nodeID)

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.toString[id]; ok {
		if existing != nodeID {
			panic(fmt.Sprintf("shard: node ID hash collision: %q and %q both hash to %d",
				existing, nodeID, id))
		}
		return id
	}
	m.toString[id] = nodeID
	return id
}

// stringID returns the node-ID string for a uint64, or false if no such ID
// has been registered.
func (m *nodeIDMap) stringID(id uint64) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.toString[id]
	return s, ok
}
