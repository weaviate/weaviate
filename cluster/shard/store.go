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
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/log"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	// raftDBName is the name of the BoltDB file for RAFT log storage.
	raftDBName = "raft.db"

	// logCacheCapacity is the maximum number of logs to cache in-memory.
	logCacheCapacity = 512

	// nRetainedSnapshots is the number of snapshots to retain.
	nRetainedSnapshots = 3

	// defaultApplyTimeout is the default timeout for RAFT Apply operations.
	defaultApplyTimeout = 10 * time.Second
)

var (
	// ErrNotLeader is returned when an operation is attempted on a non-leader node.
	ErrNotLeader = errors.New("not leader")

	// ErrNotStarted is returned when an operation is attempted before the cluster is started.
	ErrNotStarted = errors.New("raft cluster not started")

	// ErrAlreadyClosed is returned when an operation is attempted on a closed cluster.
	ErrAlreadyClosed = errors.New("raft cluster already closed")

	// ErrLeaderElectionTimeout is returned when the store cannot observe a
	// leader before the caller's context deadline expires.
	ErrLeaderElectionTimeout = errors.New("timed out waiting for shard raft leader election")
)

// StoreConfig holds configuration for a shard's RAFT cluster.
type StoreConfig struct {
	// ClassName is the name of the class this shard belongs to.
	ClassName string
	// ShardName is the name of the shard.
	ShardName string
	// NodeID is the local node's identifier.
	NodeID string
	// DataPath is the root path where RAFT data will be stored.
	DataPath string
	// Members is the list of node IDs that are members of this shard's RAFT cluster.
	Members []string
	// Logger is the logger to use.
	Logger *logrus.Logger
	// ApplyTimeout is the timeout for RAFT Apply operations.
	ApplyTimeout time.Duration

	// Transport is the RAFT transport to use. In production this is a TCP transport,
	// in tests it can be an in-memory transport.
	Transport raft.Transport

	// RAFT timing configuration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
}

// Store manages a RAFT cluster for a single physical shard.
// Each shard has its own RAFT cluster where membership equals the shard's
// replica nodes (Physical.BelongsToNodes).
type Store struct {
	config StoreConfig
	log    logrus.FieldLogger

	// RAFT components
	raft          *raft.Raft
	fsm           *FSM
	logStore      *raftbolt.BoltStore
	logCache      *raft.LogCache
	snapshotStore raft.SnapshotStore
	transport     raft.Transport

	// State
	mu       sync.RWMutex
	started  bool
	closed   bool
	dataPath string
}

// NewStore creates a new RAFT cluster for a shard.
// The cluster is not started until Start() is called.
func NewStore(config StoreConfig) (*Store, error) {
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = defaultApplyTimeout
	}

	// Calculate the data path for this shard's RAFT state
	dataPath := filepath.Join(config.DataPath, "raft")

	log := config.Logger.WithFields(logrus.Fields{
		"component": "shard_raft_store",
		"class":     config.ClassName,
		"shard":     config.ShardName,
	})

	return &Store{
		config:    config,
		log:       log,
		fsm:       NewFSM(config.ClassName, config.ShardName, config.NodeID, config.Logger),
		transport: config.Transport,
		dataPath:  dataPath,
	}, nil
}

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

	// Initialize storage
	if err := s.initStorage(); err != nil {
		return fmt.Errorf("init storage: %w", err)
	}

	// Create RAFT instance
	if err := s.initRaft(); err != nil {
		s.cleanupStorage()
		return fmt.Errorf("init raft: %w", err)
	}

	// Bootstrap if this is a new cluster
	if err := s.maybeBootstrap(); err != nil {
		s.cleanupRaft()
		s.cleanupStorage()
		return fmt.Errorf("bootstrap: %w", err)
	}

	s.started = true
	s.log.Info("shard RAFT store started")
	return nil
}

// initStorage initializes the log store and snapshot store.
func (s *Store) initStorage() error {
	// Create the data directory
	if err := os.MkdirAll(s.dataPath, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", s.dataPath, err)
	}

	// Initialize BoltDB log store
	var err error
	s.logStore, err = raftbolt.NewBoltStore(filepath.Join(s.dataPath, raftDBName))
	if err != nil {
		return fmt.Errorf("bolt db: %w", err)
	}

	// Initialize log cache
	s.logCache, err = raft.NewLogCache(logCacheCapacity, s.logStore)
	if err != nil {
		s.logStore.Close()
		return fmt.Errorf("log cache: %w", err)
	}

	// Initialize snapshot store
	s.snapshotStore, err = raft.NewFileSnapshotStore(s.dataPath, nRetainedSnapshots, s.config.Logger.Writer())
	if err != nil {
		s.logStore.Close()
		return fmt.Errorf("snapshot store: %w", err)
	}

	return nil
}

// initRaft creates the RAFT instance.
func (s *Store) initRaft() error {
	cfg := s.raftConfig()

	var err error
	s.raft, err = raft.NewRaft(cfg, s.fsm, s.logCache, s.logStore, s.snapshotStore, s.transport)
	if err != nil {
		return fmt.Errorf("new raft: %w", err)
	}

	return nil
}

// raftConfig creates the RAFT configuration.
func (s *Store) raftConfig() *raft.Config {
	cfg := raft.DefaultConfig()

	if s.config.HeartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = s.config.HeartbeatTimeout
	}
	if s.config.ElectionTimeout > 0 {
		cfg.ElectionTimeout = s.config.ElectionTimeout
	}
	if s.config.LeaderLeaseTimeout > 0 {
		cfg.LeaderLeaseTimeout = s.config.LeaderLeaseTimeout
	}
	if s.config.SnapshotInterval > 0 {
		cfg.SnapshotInterval = s.config.SnapshotInterval
	}
	if s.config.SnapshotThreshold > 0 {
		cfg.SnapshotThreshold = s.config.SnapshotThreshold
	}
	if s.config.TrailingLogs > 0 {
		cfg.TrailingLogs = s.config.TrailingLogs
	} else {
		// Shard-level default: zero trailing logs. Out-of-band state transfer
		// handles followers that fall behind, so no trailing logs are needed
		// for catch-up via log replay.
		cfg.TrailingLogs = 0
	}

	cfg.LocalID = raft.ServerID(s.config.NodeID)
	cfg.LogLevel = s.config.Logger.GetLevel().String()
	cfg.NoLegacyTelemetry = true
	cfg.Logger = log.NewHCLogrusLogger("shard-raft", s.config.Logger)

	return cfg
}

// maybeBootstrap bootstraps the RAFT cluster if this is a new cluster.
func (s *Store) maybeBootstrap() error {
	// Check if the cluster has already been bootstrapped
	hasState, err := raft.HasExistingState(s.logCache, s.logStore, s.snapshotStore)
	if err != nil {
		return fmt.Errorf("check existing state: %w", err)
	}

	if hasState {
		s.log.Info("RAFT store already bootstrapped, skipping bootstrap")
		return nil
	}

	// Build the server configuration from members
	var servers []raft.Server
	for _, member := range s.config.Members {
		servers = append(servers, raft.Server{
			ID:       raft.ServerID(member),
			Address:  raft.ServerAddress(member), // Will be resolved by the transport
			Suffrage: raft.Voter,
		})
	}

	configuration := raft.Configuration{Servers: servers}

	s.log.WithField("servers", len(servers)).Info("bootstrapping RAFT store")
	fut := s.raft.BootstrapCluster(configuration)
	if err := fut.Error(); err != nil {
		// Ignore "already bootstrapped" error
		if !errors.Is(err, raft.ErrCantBootstrap) {
			return fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return nil
}

// Stop gracefully stops the RAFT cluster.
func (s *Store) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	if !s.started {
		s.closed = true
		return nil
	}

	s.log.Info("stopping shard RAFT store")

	s.cleanupRaft()
	s.cleanupStorage()

	s.started = false
	s.closed = true
	s.log.Info("shard RAFT store stopped")
	return nil
}

func (s *Store) cleanupRaft() {
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil {
			s.log.WithError(err).Error("error shutting down raft")
		}
		s.raft = nil
	}
}

func (s *Store) cleanupStorage() {
	if s.logStore != nil {
		if err := s.logStore.Close(); err != nil {
			s.log.WithError(err).Error("error closing log store")
		}
		s.logStore = nil
	}
	s.logCache = nil
	s.snapshotStore = nil
}

// Apply applies a command to the RAFT cluster. It blocks until the command is committed and applied on all replicas.
func (s *Store) Apply(ctx context.Context, req *shardproto.ApplyRequest) (uint64, error) {
	s.mu.RLock()
	if !s.started {
		s.mu.RUnlock()
		return 0, ErrNotStarted
	}
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrAlreadyClosed
	}
	r := s.raft
	s.mu.RUnlock()

	// Serialize the command
	cmdBytes, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal command: %w", err)
	}

	// Apply to RAFT
	fut := r.Apply(cmdBytes, s.config.ApplyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, ErrNotLeader
		}
		return 0, fmt.Errorf("raft apply: %w", err)
	}

	// Always wait for the response
	futureResponse := fut.Response()
	resp, ok := futureResponse.(Response)
	if !ok {
		// This should not happen, but it's better to log an error *if* it happens than panic and crash.
		return 0, fmt.Errorf("response returned from raft apply is not of type Response instead got: %T, this should not happen", futureResponse)
	}
	return resp.Version, resp.Error
}

// IsLeader returns true if this node is the leader of the shard's RAFT cluster.
func (s *Store) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.closed || s.raft == nil {
		return false
	}
	return s.raft.State() == raft.Leader
}

// Leader returns the current leader's address, or empty string if unknown.
func (s *Store) Leader() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.closed || s.raft == nil {
		return ""
	}
	addr, _ := s.raft.LeaderWithID()
	return string(addr)
}

// LeaderID returns the current leader's node ID, or empty string if unknown.
func (s *Store) LeaderID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.closed || s.raft == nil {
		return ""
	}
	_, id := s.raft.LeaderWithID()
	return string(id)
}

// VerifyLeader ensures this node is still the leader. Used for linearizable reads.
func (s *Store) VerifyLeader() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.closed || s.raft == nil {
		return ErrNotStarted
	}
	return s.raft.VerifyLeader().Error()
}

// State returns the current RAFT state of this node.
func (s *Store) State() raft.RaftState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.started || s.closed || s.raft == nil {
		return raft.Shutdown
	}
	return s.raft.State()
}

// LastAppliedIndex returns the last applied RAFT log index.
func (s *Store) LastAppliedIndex() uint64 {
	return s.fsm.LastAppliedIndex()
}

// WaitForAppliedIndex blocks until the local FSM has applied at least
// targetIndex, or the context is cancelled. Used by followers to ensure
// their local state has caught up before performing a local read.
func (s *Store) WaitForAppliedIndex(ctx context.Context, targetIndex uint64) error {
	return s.fsm.WaitForIndex(ctx, targetIndex)
}

// WaitForLeader blocks until this node observes a leader for the shard's RAFT
// cluster (either local or remote), or the context is cancelled.
func (s *Store) WaitForLeader(ctx context.Context) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrAlreadyClosed
	}
	if !s.started {
		s.mu.RUnlock()
		return ErrNotStarted
	}
	r := s.raft
	s.mu.RUnlock()

	if hasLeader(r) {
		return nil
	}

	leaderCh := r.LeaderCh()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ErrLeaderElectionTimeout
			}
			return ctx.Err()
		case <-leaderCh:
			if hasLeader(r) {
				return nil
			}
		case <-ticker.C:
			if hasLeader(r) {
				return nil
			}
		}
	}
}

func hasLeader(r *raft.Raft) bool {
	if r == nil {
		return false
	}
	addr, id := r.LeaderWithID()
	return addr != "" && id != ""
}

type Response struct {
	Error   error
	Version uint64
}

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
