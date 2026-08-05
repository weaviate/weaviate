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
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime/debug"
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
	// reqIDSaltMask / reqIDCounterMask split an Apply reqID into its per-Store
	// random salt (high 32 bits) and counter (low 32); see Store.pending.
	reqIDSaltMask    = uint64(0xffffffff) << 32
	reqIDCounterMask = ^reqIDSaltMask

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

	// defaultMaxUncommittedEntriesSize bounds the leader-side uncommitted raft
	// log. Beyond it Propose returns raft.ErrProposalDropped, surfaced to
	// Apply as the retryable ErrProposalBackpressure. Without a bound, an
	// import against a slow quorum grows the in-memory uncommitted tail
	// without limit.
	defaultMaxUncommittedEntriesSize = 32 * 1024 * 1024

	// defaultMaxCommittedSizePerReady: with AsyncStorageWrites this quota
	// spans ALL outstanding unacknowledged MsgStorageApply messages (etcd
	// pauses committed-entry emission beyond it), so it — not the applyCh
	// depth — is what bounds the committed-but-unapplied pipeline: at most
	// 8MB of entries per group are in flight to the apply worker, matching
	// the previous applyQueueDepth(4) × MaxSizePerMsg(2MB) budget.
	defaultMaxCommittedSizePerReady = 8 * 1024 * 1024

	// defaultMaxInflightBytes bounds the append bytes a leader keeps in
	// flight to one follower. Unset, the bound is MaxInflightMsgs ×
	// MaxSizePerMsg = 256 × 2MB = 512MB per follower per group. Because a
	// follower's MsgAppResp is delivered only after its fsync, in-flight
	// bytes are exactly the un-fsynced backlog a slow follower disk can be
	// handed — cap it at 32MB per group.
	defaultMaxInflightBytes = 32 * 1024 * 1024

	// maxRaftCommandBytes caps one marshaled command (= one raft entry's
	// payload) at the write boundary; Store.Apply rejects anything larger with
	// the non-retryable ErrCommandTooLarge. It is half the smallest
	// wedge-relevant 32MB quota family (the per-stripe send-lane byte cap,
	// MaxInflightBytes, MaxUncommittedEntriesSize): a frame larger than the
	// send-lane cap can NEVER enqueue — the leader re-sends it forever and the
	// group wedges permanently, which is what this guard exists to prevent.
	// (MaxCommittedSizePerReady is smaller but has etcd's first-entry
	// exception and only batches apply — it cannot wedge, so it does not set
	// the bound.) The factor of 2 absorbs frame/entry overhead, lets an
	// oversized frame enqueue against a part-full stripe instead of only an
	// empty one, keeps admission under the uncommitted quota possible with a
	// ≤16MB backlog, and sits 4x under the receiver's 64MB corrupt-stream
	// bound. Measured post-compression (the true entry bytes), so large but
	// compressible objects are unaffected. Commands beyond this need the
	// sideloading design — see plans/oversized-objects.md.
	maxRaftCommandBytes = 16 * 1024 * 1024

	// applyQueueDepth sizes the Ready-loop→apply-worker channel. Pacing of
	// the committed-entry pipeline is enforced by etcd/raft's
	// MaxCommittedSizePerReady quota (see defaultMaxCommittedSizePerReady);
	// the channel plus the loop-side staging queue only smooth the handoff —
	// the loop never blocks on it (see flushStaged). Sized so the apply
	// worker's backlog drain can pick up the whole staged pipeline in one
	// round (the coalescing win, see applyItems) instead of four items at a
	// time; memory stays bounded by the raft quota regardless.
	applyQueueDepth = 64

	// appendQueueDepth sizes the Ready-loop→append-worker channel. Same
	// non-blocking staging applies; the memory behind queued appends is
	// raft's unstable log (the messages hold slices into it), bounded by
	// MaxUncommittedEntriesSize on a leader and MaxInflightBytes on a
	// follower.
	appendQueueDepth = 64

	// localMsgChanSize buffers storage-protocol responses (MsgAppResp to
	// self, MsgStorageAppendResp, MsgStorageApplyResp) travelling from the
	// append/apply workers back to the Ready loop. Sends are blocking (these
	// are never droppable); the buffer only decouples bursts.
	localMsgChanSize = 64

	// proposeChanSize / incomingMsgChanSize buffer the Ready loop's inbound
	// channels. raft tolerates message loss, so overflow simply drops.
	proposeChanSize     = 64
	incomingMsgChanSize = 256
	// readIndexChanSize buffers ReadIndex requests queued from VerifyLeader.
	readIndexChanSize = 64

	// defaultVerifyLeaderTimeout caps a VerifyLeader wait when ElectionTimeout
	// is unset.
	defaultVerifyLeaderTimeout = 2 * time.Second

	// defaultMaxCommitApplyLagEntries bounds how far quorum commit may run
	// ahead of local FSM apply before new proposals are rejected with
	// ErrProposalBackpressure. Apply acks at commit, so the client ack no
	// longer throttles proposals to apply throughput — without this bound a
	// sustained import grows the committed-but-unapplied backlog (and with it
	// linearizable-read waits, snapshot lag, and restart replay) without
	// limit. In-memory cost is unaffected (MaxCommittedSizePerReady caps the
	// staged pipeline); this bounds the on-disk backlog and the applied-wait.
	defaultMaxCommitApplyLagEntries = 1024
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

	// ErrProposalBackpressure is returned to Apply when raft drops a proposal
	// on a node that IS the leader (uncommitted log over
	// MaxUncommittedEntriesSize, or a leadership transfer in progress).
	// Retryable at this same node — unlike ErrNotLeader it must NOT reroute
	// the caller to another node.
	ErrProposalBackpressure = errors.New("shard raft: proposal dropped due to backpressure")

	// ErrCommandTooLarge is returned by Apply — before proposing — when one
	// marshaled command exceeds maxRaftCommandBytes. Deliberately
	// NON-retryable (and mapped to codes.InvalidArgument across the RPC
	// boundary): the command can never commit, so retrying only burns the
	// caller's retry budget. Covers every write path, including the unchunked
	// batch ones (DeleteObjects, AddReferences, MergeObject).
	ErrCommandTooLarge = errors.New("shard raft: command exceeds max raft entry size")

	// ErrGroupFailed is the terminal error pending operations receive when a
	// panic on one of the store's core goroutines (Ready loop, append worker,
	// apply worker) fails the group (see failGroup). Mapped to
	// codes.Unavailable across the RPC boundary: the group lives on on its
	// other voters, so a forwarded client re-resolves and retries there.
	ErrGroupFailed = errors.New("shard raft group failed: panic in store goroutine")

	// ErrGroupPoisoned is returned by Start when WAL boot validation
	// quarantined this group's persisted state (see sharedlog validateGroups):
	// serving it would either panic etcd or strand followers unhealably.
	// Surfaces through OnShardCreated as a per-shard init failure — the other
	// groups on the node are unaffected. Dropping the shard clears the
	// quarantine along with the damaged state.
	ErrGroupPoisoned = errors.New("shard raft group state failed WAL boot validation (poisoned)")

	// ErrClassDropped is returned by Apply — before proposing — when the
	// shard's class is no longer in the local schema: the write raced a class
	// drop and admitting it would only ack an entry the apply-side schema
	// fence will abandon. NON-retryable (codes.FailedPrecondition across the
	// RPC boundary): the drop is deliberate, retrying cannot help.
	ErrClassDropped = errors.New("shard raft: class dropped from schema")
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
	// SnapshotBytesThreshold is the committed-entry byte volume accumulated
	// since the last snapshot that triggers a snapshot — whichever of the two
	// thresholds fires first wins. Bulk imports pack many objects per entry,
	// so an entry-count threshold alone can leave hundreds of MB live in the
	// shared raft log (the flush-latency aging curve); the byte threshold
	// bounds the retained log regardless of entry packing. Zero means
	// defaultSnapshotBytesThreshold.
	SnapshotBytesThreshold uint64
	// SnapshotMinInterval is the age floor for small groups: a group with
	// retained entries older than this snapshots even when the entry/byte
	// thresholds never fire (a small tenant shard's handful of entries never
	// reaches them), bounding restart replay in age instead of leaving it
	// proportional to the group's full history. The effective per-group
	// deadline is jittered ±20% by group ID (jitterMinInterval) so thousands
	// of small groups spread their snapshots apart. Zero disables the age
	// trigger (the env layer supplies the production default).
	SnapshotMinInterval time.Duration
	// MaxCommitApplyLagEntries bounds the committed-but-unapplied entry
	// backlog before proposals surface ErrProposalBackpressure (see
	// defaultMaxCommitApplyLagEntries). Zero means the default.
	MaxCommitApplyLagEntries uint64

	// PreferredLeader is the birth designation: the member every voter's
	// placement function (PreferredBirthLeader) selected to campaign first
	// when this group is newly bootstrapped. Only consulted on the birth path
	// (never on restart of an existing group) and only compared against
	// NodeID — the node that matches campaigns as soon as the bootstrap conf
	// changes are applied; everyone else behaves exactly as before. Empty
	// disables birth campaigning for this store.
	PreferredLeader string
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
// proposedAt is stamped by Apply before the proposal is queued and read by the
// Ready loop at commit staging, where the ack is delivered (the proposeCh
// channel edge orders the two — no atomics needed). Nothing else crosses
// goroutines here: the commit stamp the apply worker needs for the
// commit→apply histogram travels inside the applyItem, because this struct is
// deleted the moment Apply returns — at commit, before local apply.
type pendingApply struct {
	done       chan applyResult
	proposedAt time.Time
}

// readResult resolves one VerifyLeader: index is the commit index as of the
// ReadIndex round (the watermark the FSM must reach before the read is
// linearizable), err the error that resolved the round instead.
type readResult struct {
	index uint64
	err   error
}

// pendingRead correlates one in-flight VerifyLeader with its ReadState.
type pendingRead struct {
	done chan readResult
}

// proposal is one command queued from Apply onto the Ready loop.
type proposal struct {
	reqID uint64
	data  []byte
}

// applyItem is one unit of ordered work handed to the apply worker: either a
// committed-entry batch (entries, from a MsgStorageApply staged by the Ready
// loop) or a snapshot to install (snap, handed off by the append worker after
// persisting it). resps carries the storage message's response set, delivered
// only after the batch is applied / the restore has succeeded. For snapshot
// installs, restored is closed once restore, bookkeeping, and response
// delivery are all complete — the append worker holds its FIFO on it.
// commitStamps parallels entries: a non-zero stamp at position i is the
// commit-staging (ack) time of a locally-proposed entry, carried here for the
// commit→apply histogram because the pendingApply is deleted when Apply
// returns at commit; nil when the batch has no locally-proposed entries.
type applyItem struct {
	entries      []raftpb.Entry
	snap         *raftpb.Snapshot
	resps        []raftpb.Message
	restored     chan struct{}
	commitStamps []time.Time
}

// appendItem is one unit of ordered work handed from the Ready loop to the
// append worker: either a MsgStorageAppend to persist (msg) or a locally
// created snapshot to persist and compact behind (snap, from
// onSnapshotResult). Exactly one field is set.
type appendItem struct {
	msg  *raftpb.Message
	snap *raftpb.Snapshot
}

// workerReq is a worker→loop round-trip for state only the Ready loop may
// touch (the RawNode, and the confState / lastSnapshotIndex / compaction
// bookkeeping). Exactly one of cc / snap is set. The loop closes done once
// the request has been served.
type workerReq struct {
	cc   raftpb.ConfChangeI
	snap *raftpb.Snapshot
	done chan struct{}
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

	// localID is this node's etcd/raft uint64 ID, set during Start. The
	// append/apply workers use it to split a storage message's responses
	// into self-directed (stepped via localMsgCh) and peer-directed
	// (transport) messages.
	localID uint64

	// rawNode and the Ready-loop channels are owned by the run() goroutine
	// after Start; only run() touches rawNode (it is not thread-safe).
	rawNode       *raft.RawNode
	proposeCh     chan proposal
	incomingMsgCh chan raftpb.Message
	snapResultCh  chan SnapshotResult
	loopCtx       context.Context
	loopCancel    context.CancelFunc
	loopDone      chan struct{}

	// applyCh hands committed work to the apply worker in strict log order;
	// workerReqCh carries worker→loop round-trips (ApplyConfChange, snapshot
	// bookkeeping); workerDone closes when the worker has exited. See
	// applyWorker.
	applyCh     chan applyItem
	workerReqCh chan workerReq
	workerDone  chan struct{}

	// appendCh hands MsgStorageAppend work (and local snapshot persists) to
	// the append worker in Ready order; appendDone closes when it has
	// exited. See appendWorker.
	appendCh   chan appendItem
	appendDone chan struct{}

	// localMsgCh carries storage-protocol responses from the append/apply
	// workers back to the Ready loop, which Steps them into the RawNode.
	// Reliable and ordered per sender — these messages are never re-sent, so
	// unlike incomingMsgCh they must not be dropped.
	localMsgCh chan raftpb.Message

	// snapPersistedCh reports a locally created snapshot's index back to the
	// loop once the append worker has persisted and compacted it; the loop
	// then updates lastSnapshotIndex and clears snapshotPending. Capacity 1:
	// snapshotPending admits at most one outstanding snapshot.
	snapPersistedCh chan uint64

	// pendingAppendQ / pendingApplyQ are Ready-loop-local staging FIFOs in
	// front of appendCh / applyCh. The loop NEVER blocks handing work to the
	// workers — a blocked handoff would park tick/step/transmit exactly like
	// the synchronous storage write this design removes — it stages instead
	// and drains opportunistically (flushStaged) on every loop iteration.
	// Growth is bounded by raft itself: appends by the unstable log
	// (MaxUncommittedEntriesSize / MaxInflightBytes), applies by
	// MaxCommittedSizePerReady.
	pendingAppendQ []appendItem
	pendingApplyQ  []applyItem

	// ticker / lastTick drive wall-clock tick replay and are Ready-loop-local;
	// electionTicks (the replay cap) is set once by raftConfig during Start.
	ticker        *time.Ticker
	lastTick      time.Time
	electionTicks int

	// CheckQuorum crossing gate (Ready-loop-local). While leader, raft
	// evaluates quorum activity at every electionTicks-th fed tick and CLEARS
	// the RecentActive evidence flags at each evaluation — its design assumes
	// a full election timeout of response-gathering wall time between
	// evaluations. Replay bursts break that assumption via elapsed CARRY: a
	// crossing resets raft's electionElapsed mid-burst and the burst tail
	// re-accrues, so the next crossing needs only (electionTicks − carry)
	// ticks, which wall-accurate replay can feed a few hundred ms later —
	// evaluating freshly-cleared flags before slow-but-alive followers can
	// respond again (observed live: step-downs with both followers' responses
	// stepped 150-306ms earlier, µs-identical ages). The gate therefore never
	// ticks PAST a crossing: the crossing ends the burst and the leftover
	// backlog is dropped, so every inter-crossing tick is backed by new wall
	// time and evaluations stay ≥ one election timeout of wall apart — with
	// heartbeats flowing normally in between (no tick holds).
	//
	// gateTicksToCrossing mirrors the fed-tick distance to the next crossing
	// (exact while continuously leader: raft resets elapsed only at
	// becomeLeader and at crossings; TransferLeadership — the one other reset
	// site — is unused in this codebase); gateLeader tracks mirror validity
	// across leadership changes.
	gateTicksToCrossing int
	gateLeader          bool

	// Replication-wedge watchdog + step-down forensics. All Ready-loop-local:
	// lastRespAt records when a response-class message from each peer was last
	// stepped; wedgeTrack remembers each voter's Match and how long it has
	// been stuck there; lastWatchdog paces the check. slowLog / wedgeLog
	// rate-limit the respective WARN lines. wedgeAfter is how long a voter's
	// Match may sit behind an advancing log before it is declared wedged.
	lastRespAt   map[uint64]time.Time
	wedgeTrack   map[uint64]wedgeTrack
	lastWatchdog time.Time
	wedgeAfter   time.Duration
	slowLog      *logLimiter
	wedgeLog     *logLimiter

	// confState / lastSnapshotIndex / snapshotPending are Ready-loop-local.
	confState         raftpb.ConfState
	lastSnapshotIndex uint64
	snapshotPending   bool

	// snapMinInterval is the jittered age floor for the third snapshot
	// trigger (StoreConfig.SnapshotMinInterval, 0 = disabled).
	// lastSnapshotProgressAt is when snapshot progress was last recorded —
	// Start, local snapshot completion, or received-snapshot install.
	// Ready-loop-local (stamped in Start before the loop launches).
	snapMinInterval        time.Duration
	lastSnapshotProgressAt time.Time

	// Birth-campaign state (Ready-loop-local after Start): armed on a newly
	// bootstrapped group when this node is the birth designation
	// (config.PreferredLeader). The campaign cannot fire inside Start:
	// Bootstrap applies the initial conf changes to the raft config in-place
	// but leaves the log-applied cursor at 0, and etcd's hup() scan-gate
	// refuses MsgHup while ConfChange entries sit in (applied, committed] —
	// so the fire is deferred to the Ready loop, on the storage-apply
	// response path, once Applied covers the birthCampaignEntries bootstrap
	// entries (see maybeBirthCampaign). One-shot: disarmed on fire, on any
	// observed leader, or once the node is no longer a follower; after that
	// the fields are dead weight and the group pays nothing.
	birthCampaignArmed   bool
	birthCampaignEntries uint64

	// Snapshot cadence state (see snapshot_cadence.go). snapMarks and
	// pendingSnapMeta are Ready-loop-local; the resolved thresholds are
	// immutable after NewStore.
	snapMarks          commitMarks
	pendingSnapMeta    pendingSnapMeta
	snapEntryThreshold uint64
	snapBytesThreshold uint64

	// leadership snapshots, written by the Ready loop, read by accessors.
	state    atomic.Uint32 // ShardRaftState
	leaderID atomic.Uint64
	leaderCh chan struct{}

	// committedStaged is the highest committed entry index staged for the
	// apply worker — the ack site, so it covers every acknowledged write by
	// construction. Written by the Ready loop; read by CommittedIndex (the
	// read-your-writes watermark GetLastAppliedIndex reports) and by
	// handlePropose (commit→apply lag backpressure). maxCommitApplyLag is the
	// backlog bound, resolved once in NewStore.
	committedStaged   atomic.Uint64
	maxCommitApplyLag uint64

	// pending correlates Apply reqIDs with their committed entries.
	//
	// reqIDs are salted: the high 32 bits are a random per-Store-instance salt,
	// the low 32 a counter. Entries carry their proposer's reqID, and every
	// node runs wakePending against every applied entry — with bare counters, a
	// pending registered on a node that is not (or no longer) the leader could
	// collide with another node's (or a previous boot's replayed) entry and be
	// woken with a false success and a wrong index. The salt makes foreign and
	// cross-boot reqIDs unresolvable here.
	pending   sync.Map // map[uint64]*pendingApply
	reqIDSalt uint64
	nextReqID atomic.Uint64

	// readIndexCh queues ReadIndex rctx tokens from VerifyLeader to the Ready
	// loop; pendingReads correlates each rctx with its waiting VerifyLeader.
	readIndexCh  chan []byte
	pendingReads sync.Map // map[string]*pendingRead, keyed by string(rctx)
	nextReadID   atomic.Uint64

	mu      sync.RWMutex
	started bool
	closed  bool

	// failOnce makes failGroup idempotent: whichever guarded goroutine
	// panics first fails the group; later panics (e.g. cascades during the
	// teardown) only log.
	failOnce sync.Once
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

	var salt [4]byte
	if _, err := cryptorand.Read(salt[:]); err != nil {
		return nil, fmt.Errorf("shard store: generate request-ID salt: %w", err)
	}

	maxLag := config.MaxCommitApplyLagEntries
	if maxLag == 0 {
		maxLag = defaultMaxCommitApplyLagEntries
	}

	entryThreshold := config.SnapshotThreshold
	if entryThreshold == 0 {
		entryThreshold = defaultSnapshotThreshold
	}
	bytesThreshold := config.SnapshotBytesThreshold
	if bytesThreshold == 0 {
		bytesThreshold = defaultSnapshotBytesThreshold
	}

	groupID := hashGroupID(config.ClassName, config.ShardName)

	log := config.Logger.WithFields(logrus.Fields{
		"component": "shard_raft_store",
		"class":     config.ClassName,
		"shard":     config.ShardName,
		"group":     groupID,
	})

	return &Store{
		config:             config,
		log:                log,
		groupID:            groupID,
		fsm:                NewFSM(config.ClassName, config.ShardName, config.NodeID, config.Logger),
		transport:          config.Transport,
		sharedLog:          config.SharedLog,
		snapshotter:        config.Snapshotter,
		nodeIDs:            nodeIDs,
		resolver:           config.Resolver,
		raftStorage:        config.SharedLog.Storage(groupID),
		tickInterval:       config.TickInterval,
		leaderCh:           make(chan struct{}, 1),
		pending:            sync.Map{},
		reqIDSalt:          uint64(binary.BigEndian.Uint32(salt[:])) << 32,
		pendingReads:       sync.Map{},
		maxCommitApplyLag:  maxLag,
		snapEntryThreshold: entryThreshold,
		snapBytesThreshold: bytesThreshold,
		snapMinInterval:    jitterMinInterval(config.SnapshotMinInterval, groupID),
		snapMarks:          commitMarks{granularity: markGranularity(bytesThreshold)},
	}, nil
}

// GroupID returns this shard's etcd/raft group identifier.
func (s *Store) GroupID() uint64 { return s.groupID }

// PreferredLeader returns the birth designation — the node the placement
// function selected to campaign first when this group was born ("" when
// none was configured). Observability plus the single-tenant reconciliation
// anchor: for single-tenant shards it equals the schema-persisted first
// replica and stays recomputable; for multi-tenant shards it is birth-time
// snapshot only (the tenant-count phase drifts with churn) and must not be
// read as a current placement target. It says nothing about who leads now —
// compare with LeaderID for that.
func (s *Store) PreferredLeader() string { return s.config.PreferredLeader }

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

	// Boot-validation gate: a group whose persisted WAL state is poisoned
	// must not serve — pre-validation, this state panicked etcd's snapshot
	// send on the first lagging-follower catch-up (minor-issues.md #9).
	// Failing Start keeps the blast radius per-shard: it surfaces through
	// OnShardCreated as this one shard's init failure.
	if reason, poisoned := s.sharedLog.PoisonedReason(s.groupID); poisoned {
		return fmt.Errorf("%w: group %d (%s/%s): %s",
			ErrGroupPoisoned, s.groupID, s.config.ClassName, s.config.ShardName, reason)
	}

	// Baseline for the age trigger: snapshot progress is measured from
	// process start, not from the persisted snapshot's wall time — the floor
	// bounds how long NEW retention may age, it does not race to catch up on
	// restart.
	s.lastSnapshotProgressAt = time.Now()

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
		// Arm the one-shot birth campaign on the designated node. Birth only:
		// restarts take the hasGroup branch, so failover and recovery
		// elections never see this. The fire is deferred to the Ready loop —
		// see the birthCampaignArmed field for why Campaign here would be
		// refused by etcd's hup() scan-gate.
		if s.config.PreferredLeader != "" && s.config.PreferredLeader == s.config.NodeID {
			s.birthCampaignArmed = true
			s.birthCampaignEntries = uint64(len(peers))
		}
	} else if snap, err := s.raftStorage.Snapshot(); err == nil && !raft.IsEmptySnap(snap) {
		// A group without a persisted snapshot answers with
		// raft.ErrSnapshotTemporarilyUnavailable (never empty+nil — etcd's
		// contract, see groupStorage.Snapshot), which correctly skips this
		// restore branch; an unreadable snapshot is caught by WAL boot
		// validation (poisoning) before Start gets here.
		// Restart from a local snapshot: re-seed the FSM's applied index so
		// WaitForAppliedIndex stays correct across restarts. A failed restore
		// fails Start: Config.Applied is seeded from this snapshot, so
		// continuing would resume applies above an FSM the shard never
		// actually restored — a silent data hole.
		if len(snap.Data) > 0 {
			var meta shardSnapshotData
			if err := json.Unmarshal(snap.Data, &meta); err != nil {
				return fmt.Errorf("decode local snapshot data: %w", err)
			}
			if err := s.fsm.RestoreFromSnapshot(meta); err != nil {
				return fmt.Errorf("restore FSM from local snapshot: %w", err)
			}
		}
		s.confState = snap.Metadata.ConfState
		s.lastSnapshotIndex = snap.Metadata.Index
	}

	s.localID = localID
	s.rawNode = rn
	s.proposeCh = make(chan proposal, proposeChanSize)
	s.incomingMsgCh = make(chan raftpb.Message, incomingMsgChanSize)
	s.readIndexCh = make(chan []byte, readIndexChanSize)
	s.snapResultCh = make(chan SnapshotResult, 1)
	s.applyCh = make(chan applyItem, applyQueueDepth)
	s.workerReqCh = make(chan workerReq)
	s.workerDone = make(chan struct{})
	s.appendCh = make(chan appendItem, appendQueueDepth)
	s.appendDone = make(chan struct{})
	s.localMsgCh = make(chan raftpb.Message, localMsgChanSize)
	s.snapPersistedCh = make(chan uint64, 1)
	s.pendingAppendQ = nil
	s.pendingApplyQ = nil
	s.lastRespAt = make(map[uint64]time.Time)
	s.wedgeTrack = make(map[uint64]wedgeTrack)
	s.wedgeAfter = wedgeAfterDuration(s.config.ElectionTimeout)
	s.slowLog = newLogLimiter(time.Second)
	s.wedgeLog = newLogLimiter(30 * time.Second)
	s.loopCtx, s.loopCancel = context.WithCancel(context.Background())
	s.loopDone = make(chan struct{})

	s.goGuarded("ready_loop", s.run)
	s.goGuarded("apply_worker", s.applyWorker)
	s.goGuarded("append_worker", s.appendWorker)

	s.started = true
	s.log.Info("shard RAFT store started")
	return nil
}

// goGuarded launches one of the store's core goroutines with a group-failure
// recovery layered INSIDE the GoWrapper: a panic anywhere on the Ready loop
// or the storage workers must fail the group loudly and tear it down —
// GoWrapper alone would log the recovery and let the goroutine die silently,
// leaving the group a zombie on this node (no Ready processing, inbound
// dropped, waiters hung: the exact outage of minor-issues.md #9). This
// includes the append worker's deliberate durability panics: their fail-stop
// intent is preserved as a loud, visible group failure rather than a silent
// swallow. Costs one deferred recover on goroutine exit — nothing on any hot
// path.
func (s *Store) goGuarded(name string, f func()) {
	enterrors.GoWrapper(func() {
		defer func() {
			if r := recover(); r != nil {
				s.failGroup(name, r)
			}
		}()
		f()
	}, s.log)
}

// failGroup handles a panic on a core goroutine: count it, log it with the
// stack, fail every pending operation with a terminal error, and stop the
// store cleanly. Idempotent. Deliberately NO in-place restart: the panic
// means an invariant was violated, and rerunning over the same state risks a
// tight panic loop — the group's other voters elect around this node and
// clients re-resolve (ErrGroupFailed maps to codes.Unavailable), while the
// operator sees the metric and the Errorf.
//
// Stop runs on a fresh goroutine: the panicking goroutine's own defers
// (loopDone/appendDone/workerDone) run as its panic unwinds, so Stop's joins
// cannot deadlock on the goroutine that died — but they must not run ON it.
func (s *Store) failGroup(goroutine string, cause any) {
	s.failOnce.Do(func() {
		shardRaftStorePanics.WithLabelValues(s.config.ClassName, s.config.ShardName, goroutine).Inc()
		s.log.Errorf("shard raft %s panicked, failing the group on this node: %v\n%s",
			goroutine, cause, debug.Stack())
		s.drainPending(ErrGroupFailed)
		s.drainPendingReads(ErrGroupFailed)
		enterrors.GoWrapper(func() {
			if err := s.Stop(); err != nil {
				s.log.Errorf("stopping failed shard raft group: %v", err)
			}
		}, s.log)
	})
}

// raftConfig builds the etcd/raft configuration for this group. As a side
// effect it records electionTicks, the cap replayTicks uses so a long loop
// stall replays at most one election timeout's worth of ticks.
func (s *Store) raftConfig(localID uint64) *raft.Config {
	hb := ticksFromDuration(s.config.HeartbeatTimeout, s.tickInterval, defaultHeartbeatTicks)
	el := ticksFromDuration(s.config.ElectionTimeout, s.tickInterval, defaultElectionTicks)
	if el <= hb {
		el = hb + 1
	}
	s.electionTicks = el

	var applied uint64
	// No persisted snapshot surfaces as ErrSnapshotTemporarilyUnavailable
	// (etcd's contract, see groupStorage.Snapshot) and leaves Applied at 0.
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
		// AsyncStorageWrites takes the storage-durability wait off the Ready
		// loop: appends and FSM application arrive as MsgStorageAppend /
		// MsgStorageApply messages consumed by the append and apply workers,
		// while everything not durability-gated (heartbeats, MsgApp, vote
		// REQUESTS) is handed to the transport's sender lanes immediately —
		// a multi-second sharedlog fsync can no longer park raft timing (the
		// mid-import step-down convoy).
		// Durability is unchanged: acks that require persistence (MsgAppResp,
		// MsgVoteResp, MsgPreVoteResp — raft.send routes exactly these
		// through msgsAfterAppend) ride MsgStorageAppend.Responses and are
		// delivered by the append worker only after the covering fsync.
		AsyncStorageWrites: true,
		// Bounds the leader's in-memory uncommitted tail; overflow surfaces
		// as ErrProposalBackpressure from Apply.
		MaxUncommittedEntriesSize: defaultMaxUncommittedEntriesSize,
		// In async mode this quota spans all outstanding unacked
		// MsgStorageApply messages — it bounds the committed-but-unapplied
		// pipeline the way the blocking applyCh handoff used to.
		MaxCommittedSizePerReady: defaultMaxCommittedSizePerReady,
		// Caps a follower's un-fsynced append backlog (acks are post-fsync).
		MaxInflightBytes: defaultMaxInflightBytes,
		CheckQuorum:      true,
		PreVote:          true,
		// ReadOnlySafe (etcd's zero-value default) confirms each ReadIndex via
		// a quorum heartbeat round — what VerifyLeader relies on for active
		// linearizable-read leadership confirmation.
		ReadOnlyOption: raft.ReadOnlySafe,
		Logger:         raftLogger{s.log},
	}
}

// verifyLeaderTimeout caps a VerifyLeader wait. CheckQuorum forces a leader
// that has lost quorum to step down within ~1 election timeout (which drains
// any pending read), so 2x that is a safe upper bound; falls back to a fixed
// default when ElectionTimeout is unset.
func verifyLeaderTimeout(electionTimeout time.Duration) time.Duration {
	if electionTimeout <= 0 {
		return defaultVerifyLeaderTimeout
	}
	return 2 * electionTimeout
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

// run is the Ready loop: the single goroutine that owns the RawNode. It
// ticks, steps inbound messages and storage-protocol responses, drains
// Ready(), transmits network messages, and stages storage work for the
// append/apply workers. With AsyncStorageWrites nothing on this loop ever
// waits for a disk write or an FSM apply — heartbeats, elections, and
// CheckQuorum stay wall-clock accurate under both apply load and fsync
// convoys (Advance does not exist in this mode; worker responses replace it).
func (s *Store) run() {
	defer close(s.loopDone)

	s.ticker = time.NewTicker(s.tickInterval)
	defer s.ticker.Stop()
	s.lastTick = time.Now()

	for {
		select {
		case <-s.loopCtx.Done():
			return
		case <-s.ticker.C:
			t0 := time.Now()
			// Step everything already queued BEFORE replaying missed ticks:
			// after a loop stall, the ticker and the stall window's inbound
			// messages become ready together, and an election-timeout replay
			// burst would otherwise evaluate CheckQuorum against pre-stall
			// RecentActive flags while the followers' fresh responses sit
			// unread in the queue — a spurious step-down against healthy
			// followers.
			s.drainInbound()
			s.replayTicks()
			s.sampleOccupancy()
			s.maybeRunWatchdog(time.Now())
			shardRaftLoopPhase.WithLabelValues("tick_replay").Observe(time.Since(t0).Seconds())
		case m := <-s.incomingMsgCh:
			t0 := time.Now()
			s.stepPeerMsg(m)
			shardRaftLoopPhase.WithLabelValues("step").Observe(time.Since(t0).Seconds())
		case m := <-s.localMsgCh:
			t0 := time.Now()
			s.stepLocalMsg(m)
			shardRaftLoopPhase.WithLabelValues("step").Observe(time.Since(t0).Seconds())
		case p := <-s.proposeCh:
			s.handlePropose(p)
		case rctx := <-s.readIndexCh:
			s.handleReadIndex(rctx)
		case r := <-s.snapResultCh:
			s.onSnapshotResult(r)
		case req := <-s.workerReqCh:
			s.serveWorkerReq(req)
		case idx := <-s.snapPersistedCh:
			s.completeLocalSnapshot(idx)
		}

		drainStart := time.Now()
		for s.rawNode.HasReady() {
			s.processReady()
		}
		drain := time.Since(drainStart)
		shardRaftLoopPhase.WithLabelValues("ready_drain").Observe(drain.Seconds())
		if drain > slowLoopThreshold && s.slowLog.Allow("drain") {
			s.log.WithFields(logrus.Fields{
				"duration":         drain.String(),
				"staged_appends":   len(s.pendingAppendQ),
				"staged_applies":   len(s.pendingApplyQ),
				"incoming_pending": len(s.incomingMsgCh),
				"local_pending":    len(s.localMsgCh),
			}).Warn("shard raft ready loop iteration exceeded stall threshold")
		}
		// Drain staged storage work opportunistically. The ticker guarantees
		// another drain within one tick interval even when the loop is idle.
		s.flushStaged()
	}
}

// slowLoopThreshold is the Ready-loop stall budget: anything on the loop
// slower than this is attributed and WARN-logged, because loop latency
// directly dilates raft timing for the group.
const slowLoopThreshold = 100 * time.Millisecond

// stepPeerMsg steps one inbound peer message, recording response-class
// arrivals for CheckQuorum forensics.
func (s *Store) stepPeerMsg(m raftpb.Message) {
	if isResponseClass(m.Type) {
		s.lastRespAt[m.From] = time.Now()
	}
	if err := s.rawNode.Step(m); err != nil {
		s.log.Debugf("raft step: %v", err)
	}
}

// stepLocalMsg steps one storage-protocol response from the append/apply
// workers.
func (s *Store) stepLocalMsg(m raftpb.Message) {
	if err := s.rawNode.Step(m); err != nil {
		s.log.Debugf("raft step storage response: %v", err)
	}
	// Storage-apply responses are the only messages that advance the
	// log-applied cursor, so this is the exact edge where a newly
	// bootstrapped group becomes campaignable (hup()'s scan-gate passes once
	// Applied covers the bootstrap conf entries). Armed only during birth on
	// the designated node; nothing here runs for idle or established groups.
	if s.birthCampaignArmed && m.Type == raftpb.MsgStorageApplyResp {
		s.maybeBirthCampaign()
	}
}

// maybeBirthCampaign fires the one-shot designated birth campaign once the
// bootstrap conf-change entries are applied. Runs on the Ready loop (rawNode
// owner). The designation is a hint: any condition that makes the immediate
// election pointless (a leader already exists, this node already left
// follower state via its own randomized timeout) disarms without firing, and
// the group falls back to the normal election race. A fired campaign is
// PreVote-first, so even a mistaken fire (e.g. a fresh replica bootstrapping
// into an established group) can never disturb a live leader — the peers'
// CheckQuorum lease rejects it.
func (s *Store) maybeBirthCampaign() {
	st := s.rawNode.BasicStatus()
	if st.Lead != raft.None || st.RaftState != raft.StateFollower {
		s.birthCampaignArmed = false
		return
	}
	if st.Applied < s.birthCampaignEntries {
		return // bootstrap conf entries not fully applied yet; keep armed
	}
	s.birthCampaignArmed = false
	if err := s.rawNode.Campaign(); err != nil {
		s.log.Warnf("birth campaign failed to start, falling back to election race: %v", err)
		return
	}
	shardRaftBirthCampaigns.WithLabelValues(s.config.ClassName, s.config.ShardName).Inc()
	s.log.Info("designated birth campaigner starting immediate election")
}

// drainInbound steps every already-queued inbound message — peer traffic and
// storage-protocol responses — without blocking. It runs before every tick
// replay so that raft's timer-driven decisions (CheckQuorum above all) are
// evaluated against current knowledge: evidence that arrived during a stall
// must be stepped before the burst that crosses the decision threshold. A
// follower that really was silent through a stall leaves nothing to drain, so
// honest step-downs still fire at the correct wall-clock time.
//
// No accrual conservatism beyond this is needed: the replay cap
// (electionTicks) already bounds a burst to one election timeout, so at most
// one CheckQuorum evaluation can occur per burst, and with the drain it sees
// everything received up to the wake. Evidence arriving DURING the
// microsecond-scale burst is indistinguishable from evidence arriving just
// after a normal-cadence check — the same benign race exists in an unstalled
// loop. Diluting accrual instead (fewer ticks per wake) would reintroduce
// exactly the timer dilation tick replay exists to remove.
func (s *Store) drainInbound() {
	for {
		select {
		case m := <-s.incomingMsgCh:
			s.stepPeerMsg(m)
		case m := <-s.localMsgCh:
			s.stepLocalMsg(m)
		default:
			return
		}
	}
}

// isResponseClass reports whether a network message counts as peer response
// traffic for CheckQuorum forensics (the messages whose absence makes a leader
// consider a follower inactive).
func isResponseClass(t raftpb.MessageType) bool {
	switch t {
	case raftpb.MsgAppResp, raftpb.MsgHeartbeatResp, raftpb.MsgVoteResp, raftpb.MsgPreVoteResp:
		return true
	default:
		return false
	}
}

// sampleOccupancy records the loop channels' fill ratios once per tick fire.
func (s *Store) sampleOccupancy() {
	shardRaftChanOccupancy.WithLabelValues("incoming").Observe(chanRatio(len(s.incomingMsgCh), cap(s.incomingMsgCh)))
	shardRaftChanOccupancy.WithLabelValues("local").Observe(chanRatio(len(s.localMsgCh), cap(s.localMsgCh)))
	shardRaftChanOccupancy.WithLabelValues("propose").Observe(chanRatio(len(s.proposeCh), cap(s.proposeCh)))
	shardRaftChanOccupancy.WithLabelValues("append").Observe(chanRatio(len(s.appendCh), cap(s.appendCh)))
	shardRaftChanOccupancy.WithLabelValues("apply").Observe(chanRatio(len(s.applyCh), cap(s.applyCh)))
}

func chanRatio(length, capacity int) float64 {
	if capacity == 0 {
		return 0
	}
	return float64(length) / float64(capacity)
}

// missedTicks converts the wall time elapsed since last into whole tick
// intervals, capping the backlog at maxTicks so a very long stall replays at
// most one election timeout's worth of ticks — enough to trigger the correct
// timer transitions without a step-down storm. It returns the tick count, the
// new last-tick watermark, and whether the backlog was clamped; when clamped,
// the watermark jumps to now, deliberately dropping the excess backlog.
func missedTicks(last, now time.Time, interval time.Duration, maxTicks int) (int, time.Time, bool) {
	if interval <= 0 {
		return 0, now, false
	}
	n := int(now.Sub(last) / interval)
	if n <= 0 {
		return 0, last, false
	}
	if n > maxTicks {
		return maxTicks, now, true
	}
	return n, last.Add(time.Duration(n) * interval), false
}

// gatePlan decides how many of n requested ticks may feed the RawNode, given
// the CheckQuorum crossing gate. Pure, for table tests.
//
// ticksToCrossing is the fed-tick distance to the next quorum evaluation
// (1 = the very next tick evaluates). It returns the allowed tick count, the
// mirror distance after feeding it, and whether the allowed range ends at the
// crossing.
//
// A burst never ticks PAST a crossing: when the crossing falls inside n, the
// burst is truncated at it and the caller drops the leftover backlog. Every
// tick between two crossings is then backed by newly-elapsed wall time, so
// consecutive quorum evaluations are always at least one election timeout of
// WALL time apart — restoring stock raft's evidence-accumulation window —
// while heartbeat generation continues at wall pace in between (the gate
// never withholds pre-crossing ticks).
func gatePlan(n, ticksToCrossing, electionTicks int) (allowed, newTicksToCrossing int, crossed bool) {
	if n < ticksToCrossing {
		return n, ticksToCrossing - n, false
	}
	return ticksToCrossing, electionTicks, true
}

// replayTicks advances the raft logical clock by every tick interval that has
// elapsed on the wall clock. The ticker channel has capacity 1, so a slow
// loop iteration (log fsync, TCP sends, a full apply queue) silently drops
// tick events; without replay, heartbeat generation and the CheckQuorum /
// election timers dilate with load — the ~10x collapse seen in production.
// Replaying against the wall clock means an overloaded leader that has truly
// lost quorum steps down at the CORRECT time, and a stalled follower
// campaigns on time, instead of both acting late.
//
// While leader, tick feeding passes through the crossing gate (gatePlan) so
// quorum evaluations stay at least one election timeout of WALL time apart —
// see the gate fields on Store for the failure this prevents.
func (s *Store) replayTicks() {
	now := time.Now()
	n, last, _ := missedTicks(s.lastTick, now, s.tickInterval, s.electionTicks)

	// BasicStatus is loop-owned and reflects any leadership change the
	// preceding drain just caused (unlike the s.state atomic, which lags
	// until the next Ready is processed). On (re)gaining leadership raft has
	// reset electionElapsed, so the mirror re-inits to a full period — and
	// the pre-leadership tick backlog is dropped: those intervals belong to
	// the candidate phase, and feeding them now would let the first quorum
	// evaluation fire instantly against a fresh leader's virgin (all-false)
	// RecentActive flags.
	if st := s.rawNode.BasicStatus(); st.RaftState == raft.StateLeader {
		if !s.gateLeader {
			s.gateLeader = true
			s.gateTicksToCrossing = s.electionTicks
			n, last = 0, now
		}
		if n > 0 {
			allowed, newTTC, crossed := gatePlan(n, s.gateTicksToCrossing, s.electionTicks)
			if crossed && allowed < n {
				last = now // drop the backlog behind the crossing
			}
			n = allowed
			s.gateTicksToCrossing = newTTC
		}
	} else {
		s.gateLeader = false
	}

	s.lastTick = last
	for i := 0; i < n; i++ {
		s.rawNode.Tick()
	}
}

// wedgeTrack remembers one voter's replication Match and since when it has
// been stuck there, across watchdog rounds.
type wedgeTrack struct {
	match uint64
	since time.Time
}

// replicaProgress is the watchdog's view of one voter, extracted from
// raft.Status on the Ready loop.
type replicaProgress struct {
	id              uint64
	match           uint64
	state           string
	recentActive    bool
	pendingSnapshot uint64
}

// wedgeAfterDuration returns how long a voter's Match may stall behind an
// advancing log before the watchdog declares it wedged: four election
// timeouts, floored at 5s so aggressive test timings do not flap.
func wedgeAfterDuration(electionTimeout time.Duration) time.Duration {
	d := 4 * electionTimeout
	if d < 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// evaluateWedges returns the voters whose replication is wedged — Match behind
// leaderMatch and unchanged for at least wedgeAfter — plus the refreshed
// tracking state for the next round. The predicate is deliberately
// Progress-state-agnostic: a paused StateProbe, a stale StateSnapshot, and any
// future silent pause all look identical in Match terms, and Match is the one
// signal that cannot lie about replication progress.
func evaluateWedges(
	peers []replicaProgress,
	leaderMatch uint64,
	prev map[uint64]wedgeTrack,
	now time.Time,
	wedgeAfter time.Duration,
) (wedged []replicaProgress, next map[uint64]wedgeTrack) {
	next = make(map[uint64]wedgeTrack, len(peers))
	for _, p := range peers {
		if p.match >= leaderMatch {
			next[p.id] = wedgeTrack{match: p.match, since: now}
			continue
		}
		tr, ok := prev[p.id]
		if !ok || tr.match != p.match {
			// First sighting at this Match: start (or restart) the clock.
			next[p.id] = wedgeTrack{match: p.match, since: now}
			continue
		}
		next[p.id] = tr
		if now.Sub(tr.since) >= wedgeAfter {
			wedged = append(wedged, p)
		}
	}
	return wedged, next
}

// maybeRunWatchdog runs the replication-wedge check on the Ready loop, at most
// once per watchdog interval, while this node leads the group. On a wedge it
// WARNs (rate-limited per peer) with full per-voter forensics and raises the
// wedged-replicas gauge — the alarm whose absence let a replica silently
// diverge for 17 hours.
func (s *Store) maybeRunWatchdog(now time.Time) {
	interval := 2 * s.config.ElectionTimeout
	if interval < time.Second {
		interval = time.Second
	}
	if now.Sub(s.lastWatchdog) < interval {
		return
	}
	s.lastWatchdog = now

	gauge := shardRaftWedgedReplicas.WithLabelValues(s.config.ClassName, s.config.ShardName)
	if ShardRaftState(s.state.Load()) != ShardStateLeader {
		if len(s.wedgeTrack) > 0 {
			s.wedgeTrack = make(map[uint64]wedgeTrack)
		}
		gauge.Set(0)
		return
	}

	st := s.rawNode.Status()
	peers := make([]replicaProgress, 0, len(st.Progress))
	for id, pr := range st.Progress {
		if id == s.localID {
			continue
		}
		peers = append(peers, replicaProgress{
			id:              id,
			match:           pr.Match,
			state:           pr.State.String(),
			recentActive:    pr.RecentActive,
			pendingSnapshot: pr.PendingSnapshot,
		})
	}
	leaderMatch := st.Progress[s.localID].Match

	wedged, next := evaluateWedges(peers, leaderMatch, s.wedgeTrack, now, s.wedgeAfter)
	s.wedgeTrack = next
	gauge.Set(float64(len(wedged)))

	for _, p := range wedged {
		if !s.wedgeLog.Allow(groupLabel(p.id)) {
			continue
		}
		peerID, _ := s.nodeIDs.stringID(p.id)
		s.log.WithFields(logrus.Fields{
			"peer":             peerID,
			"peer_raft_id":     p.id,
			"peer_match":       p.match,
			"leader_match":     leaderMatch,
			"progress_state":   p.state,
			"recent_active":    p.recentActive,
			"pending_snapshot": p.pendingSnapshot,
			"last_response":    s.respAge(p.id, now),
		}).Warn("replication to voter is wedged: Match is not advancing while the log grows")
	}
}

// respAge renders how long ago a response-class message from peer was stepped,
// or "never" if none has been seen since Start.
func (s *Store) respAge(peer uint64, now time.Time) string {
	t, ok := s.lastRespAt[peer]
	if !ok {
		return "never"
	}
	return now.Sub(t).String()
}

// processReady drains one Ready in AsyncStorageWrites mode: transmit network
// messages immediately, stage MsgStorageAppend / MsgStorageApply for the
// append and apply workers, resolve read barriers, track leadership, and
// maybe trigger a new snapshot. Advance is never called — the workers'
// response messages (stepped back via localMsgCh) take its place.
//
// The Entries / HardState / Snapshot / CommittedEntries fields of the Ready
// are deliberately ignored: in async mode they are mirrored into the storage
// messages in rd.Messages, which are the single source acted on.
//
// Durability ordering holds by construction: everything appearing directly in
// rd.Messages (MsgApp, heartbeats, vote REQUESTS, MsgSnap) is safe to send
// before local persistence, while every ack that requires persistence
// (MsgAppResp / MsgVoteResp / MsgPreVoteResp and the self-directed storage
// responses) is attached to a MsgStorageAppend's Responses and delivered by
// the append worker only after the covering fsync.
func (s *Store) processReady() {
	readyStart := time.Now()
	rd := s.rawNode.Ready()
	shardRaftLoopPhase.WithLabelValues("ready_get").Observe(time.Since(readyStart).Seconds())

	var netMsgs []raftpb.Message
	var snapSentTo []uint64
	for i := range rd.Messages {
		m := rd.Messages[i]
		switch m.To {
		case raft.LocalAppendThread:
			msg := m
			s.pendingAppendQ = append(s.pendingAppendQ, appendItem{msg: &msg})
		case raft.LocalApplyThread:
			// Committed entries to apply. Snapshots never arrive here — they
			// ride MsgStorageAppend; the append worker sequences the FSM
			// restore into applyCh itself. Staging is the client ack site:
			// entries here are quorum-committed and locally durable (async
			// mode emits committed entries only once locally stable), so
			// ackCommitted wakes the waiting Applies without waiting for FSM
			// materialization.
			stamps := s.ackCommitted(m.Entries)
			s.pendingApplyQ = append(s.pendingApplyQ, applyItem{entries: m.Entries, resps: m.Responses, commitStamps: stamps})
		default:
			if m.Type == raftpb.MsgSnap {
				snapSentTo = append(snapSentTo, m.To)
			}
			netMsgs = append(netMsgs, m)
		}
	}
	if len(netMsgs) > 0 {
		// Send only encodes and enqueues onto per-peer sender lanes — the
		// wire writes happen on the transport's writer goroutines — so the
		// transmit phase can never stall the loop on a slow peer.
		sendStart := time.Now()
		s.transport.Send(s.groupID, netMsgs)
		shardRaftLoopPhase.WithLabelValues("transmit").Observe(time.Since(sendStart).Seconds())
	}
	// Report each transmitted snapshot as finished immediately after handoff
	// (enqueued onto the peer's sender lane). The transport is
	// fire-and-forget, so handoff is the last observable event; reporting
	// success moves the peer from StateSnapshot back to probing at the
	// snapshot index. If the snapshot is dropped downstream (lane overflow,
	// write failure), lost in flight, or its ack arrives after this leader
	// has compacted past it, the probe discovers the truth and either resends
	// entries or a fresh snapshot — without this report a stale snapshot ack
	// leaves the peer in StateSnapshot forever
	// (see TestStore_SnapshotInstall_StaleAckAfterCompaction_LeaderResumes).
	for _, to := range snapSentTo {
		s.rawNode.ReportSnapshot(to, raft.SnapshotFinish)
		s.log.WithField("to", to).Info("snapshot handed to transport; resuming probe-based replication")
	}

	// Resolve linearizable-read barriers confirmed by quorum this round.
	// ReadStates are volatile (etcd clears them on Ready accept) — consume
	// now. rs.Index is the commit index as of the ReadIndex round, so it
	// covers every write acked (committed) before the read began;
	// VerifyLeader completes only once the FSM has applied to at least
	// rs.Index. The applied watermark alone would NOT cover acked writes —
	// Apply acks at commit, ahead of local apply.
	for _, rs := range rd.ReadStates {
		s.wakePendingRead(rs.RequestCtx, readResult{index: rs.Index})
	}

	if rd.SoftState != nil {
		s.handleSoftState(rd.SoftState)
	}

	s.flushStaged()
	s.maybeSnapshot()
}

// flushStaged drains the staging FIFOs into the worker channels without ever
// blocking the loop. Leftovers stay staged and are retried on every loop
// iteration (the ticker bounds the retry latency at one tick interval);
// per-target FIFO order is preserved. See pendingAppendQ for the growth
// bounds.
func (s *Store) flushStaged() {
	s.pendingAppendQ = drainStaged(s.pendingAppendQ, s.appendCh)
	s.pendingApplyQ = drainStaged(s.pendingApplyQ, s.applyCh)
}

// drainStaged moves queue heads into ch until ch is full or q is empty. It
// zeroes handed-off slots (releasing entry references) and returns nil on a
// full drain so the backing array can be collected.
func drainStaged[T any](q []T, ch chan<- T) []T {
	for len(q) > 0 {
		select {
		case ch <- q[0]:
			var zero T
			q[0] = zero
			q = q[1:]
		default:
			return q
		}
	}
	return nil
}

// serveWorkerReq performs, on the Ready loop, the slice of an apply-worker
// item that must touch loop-owned state: the RawNode for conf changes, and
// confState / lastSnapshotIndex for snapshot installs. Sequencing these
// through the worker keeps every mutation in log order, so outbound snapshot
// metadata can never pair a newer ConfState with an older applied index.
// Log compaction after an install runs on the worker (a shared-log fsync
// must never run on the loop).
func (s *Store) serveWorkerReq(req workerReq) {
	switch {
	case req.cc != nil:
		// With AsyncStorageWrites, raft's internal applied index advances on
		// MsgStorageApplyResp — delivered after the worker applied the batch
		// containing this entry — so the library's "one unapplied conf change
		// at a time" proposal gate observes truthful applied progress.
		if cs := s.rawNode.ApplyConfChange(req.cc); cs != nil {
			s.confState = *cs
		}
	case req.snap != nil:
		s.confState = req.snap.Metadata.ConfState
		s.lastSnapshotIndex = req.snap.Metadata.Index
		// A received snapshot supersedes the local log prefix: rebase the
		// cadence byte accounting so the tail counts only bytes above it,
		// and count it as snapshot progress for the age floor.
		s.lastSnapshotProgressAt = time.Now()
		s.snapMarks.pruneTo(req.snap.Metadata.Index)
	}
	close(req.done)
}

// applyWorker is the per-Store goroutine that applies committed work: the
// LocalApplyThread of the AsyncStorageWrites topology. It preserves the FSM's
// single-threaded, log-order dispatch contract: items arrive in Ready order
// (entry batches staged by the loop; snapshot installs sequenced in by the
// append worker, which raft's snapshot pause keeps interleave-free). On each
// wake it DRAINS the queued entry batches — the committed-but-unapplied
// backlog, bounded by MaxCommittedSizePerReady — and materializes them
// together (applyItems), coalescing consecutive put-batch entries into fewer,
// larger LSM rounds; each item's MsgStorageApplyResp is delivered as soon as
// the unit covering its last entry has materialized, advancing raft's applied
// index progressively. Snapshot installs are barriers: the drain stops at
// one, the accumulated batches materialize first, then the install runs
// alone. RawNode state is never touched here — conf changes and snapshot
// bookkeeping round-trip to the loop via workerReqCh.
//
// On Stop the worker abandons whatever is still queued or drained-but-
// unmaterialized — committed-but-unapplied entries are re-delivered from the
// last persisted snapshot on restart and re-applied idempotently (the LSM
// write path is WAL-backed and last-write-wins) — and finishes only the
// materialization unit already in flight, keeping Stop bounded by one merged
// round.
func (s *Store) applyWorker() {
	defer close(s.workerDone)
	for {
		// Deterministic abandon: once shutdown begins, exit before taking
		// another item even if the queue is non-empty.
		if s.loopCtx.Err() != nil {
			return
		}
		select {
		case <-s.loopCtx.Done():
			return
		case item, ok := <-s.applyCh:
			if !ok {
				return
			}
			if item.snap != nil {
				if !s.installSnapshot(item) {
					return
				}
				continue
			}
			items := []applyItem{item}
			var snapItem *applyItem
		drain:
			for {
				select {
				case next, ok := <-s.applyCh:
					if !ok {
						break drain
					}
					if next.snap != nil {
						sn := next
						snapItem = &sn
						break drain
					}
					items = append(items, next)
				default:
					break drain
				}
			}
			// Deterministic abandon of the drained backlog on shutdown —
			// mirrors the pre-drain check above.
			if s.loopCtx.Err() != nil {
				return
			}
			if !s.applyItemsParking(items, snapItem) {
				return
			}
		}
	}
}

// Park-and-retry cadence for a committed entry whose materialization keeps
// failing environmentally. Parking is indefinite by design (Decision A in the
// storage-error taxonomy): writes are never discarded, so the entry retries
// with capped exponential backoff until it lands, a snapshot install
// supersedes it, or the group is torn down. parkWarnAfter is the purely
// observational long-park threshold — it changes log wording, never behavior.
const (
	parkInitialBackoff = 500 * time.Millisecond
	parkMaxBackoff     = 30 * time.Second
	parkWarnAfter      = 5 * time.Minute
)

// parkLog rate-limits the per-retry park error lines per group.
var parkLog = newLogLimiter(10 * time.Second)

// applyItemsParking materializes a drained run, owning the park-and-retry
// loop of the write-durability contract: when applyItems parks at a failing
// entry, the un-materialized remainder is retried with capped backoff —
// indefinitely — while the apply channel keeps draining, so later committed
// batches queue behind the parked entry and a queued snapshot install at or
// above it supersedes it (the follower-catch-up cure). pendingSnap is a
// snapshot install the worker's drain stopped at (a barrier in Ready order):
// it runs after the drained batches materialize or, if they park, supersedes
// them immediately — it must never wait behind an indefinite park, because
// the append worker holds its FIFO on the install and the install IS the
// cure.
//
// Composition: while parked, the FSM's applied index is frozen at the last
// complete entry, so the durable floor and the snapshot cadence can never
// cover the parked entry; on a leader the commit-apply lag cap turns the
// frozen watermark into ErrProposalBackpressure for new writes; a follower
// falls behind on disk and recovers by replay or state transfer. Returns
// false when the store shuts down.
func (s *Store) applyItemsParking(items []applyItem, pendingSnap *applyItem) bool {
	gauge := shardRaftApplyParkedAge.WithLabelValues(s.config.ClassName, s.config.ShardName)
	var parkedSince time.Time
	defer func() {
		// Clears on every exit: resume, shutdown, and group teardown alike.
		if !parkedSince.IsZero() {
			gauge.Set(0)
		}
	}()
	backoff := parkInitialBackoff
	for {
		parked, ok := s.applyItems(items)
		if !ok {
			return false
		}
		if parked == nil {
			if pendingSnap != nil {
				if !s.installSnapshot(*pendingSnap) {
					return false
				}
			}
			if !parkedSince.IsZero() {
				gauge.Set(0)
				s.log.WithField("parked_for", time.Since(parkedSince).Round(time.Millisecond).String()).
					Info("apply resumed: parked entry materialized")
				parkedSince = time.Time{}
			}
			return true
		}

		now := time.Now()
		if parkedSince.IsZero() {
			parkedSince = now
			backoff = parkInitialBackoff
		}
		age := now.Sub(parkedSince)
		gauge.Set(age.Seconds())
		shardRaftApplyParkRetries.WithLabelValues(s.config.ClassName, s.config.ShardName).Inc()
		if parkLog.Allow(groupLabel(s.groupID)) {
			entry := s.log.WithFields(logrus.Fields{
				"index": parked.index,
				"age":   age.Round(time.Second).String(),
			})
			if age >= parkWarnAfter {
				entry.Errorf("apply LONG-PARKED (over %s) at committed entry %d — this node's shard data is frozen at the parked entry while peers carry the shard; if this persists, repair is operator-driven (drop/replace the node): %v",
					parkWarnAfter, parked.index, parked.err)
			} else {
				entry.Errorf("apply parked at committed entry %d, retrying with backoff (writes are never discarded): %v",
					parked.index, parked.err)
			}
		}

		items = parked.remaining
		if pendingSnap != nil {
			// The barrier install supersedes the parked backlog immediately.
			if !s.installSnapshotSuperseding(*pendingSnap, &items) {
				return false
			}
			pendingSnap = nil
			continue
		}
		if !s.parkWait(backoff, &items) {
			return false
		}
		if backoff *= 2; backoff > parkMaxBackoff {
			backoff = parkMaxBackoff
		}
	}
}

// parkWait blocks for backoff while keeping the apply channel drained: new
// committed batches append to the pending backlog (they must wait behind the
// parked entry anyway), and a queued snapshot install runs immediately —
// superseding whatever it covers — after which the caller retries at once.
// Returns false on shutdown.
func (s *Store) parkWait(backoff time.Duration, items *[]applyItem) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	for {
		select {
		case <-s.loopCtx.Done():
			return false
		case <-timer.C:
			return true
		case next, ok := <-s.applyCh:
			if !ok {
				return false
			}
			if next.snap == nil {
				*items = append(*items, next)
				continue
			}
			return s.installSnapshotSuperseding(next, items)
		}
	}
}

// installSnapshotSuperseding installs a received snapshot while a parked
// backlog is pending. Entries at or below the snapshot index are superseded —
// the restored state already covers them — and are dropped from the backlog;
// entries above it stay queued and materialize after. The held
// MsgStorageApplyResp of fully-superseded items deliver AFTER the restore
// succeeds, in item order up to the first item that retains entries.
// Delivering them is mandatory, not optional: those acks release raft's
// MaxCommittedSizePerReady quota (appliedSnap reduces no apply-quota bytes),
// and stale acks are safe — raft floors the applied index at its current
// value and defends the quota subtraction against underflow. Returns false
// when the store shuts down.
func (s *Store) installSnapshotSuperseding(item applyItem, items *[]applyItem) bool {
	if !s.installSnapshot(item) {
		return false
	}
	snapIdx := item.snap.Metadata.Index
	rest := *items
	i := 0
	for ; i < len(rest); i++ {
		it := &rest[i]
		cut := 0
		for cut < len(it.entries) && it.entries[cut].Index <= snapIdx {
			cut++
		}
		if cut > 0 {
			it.entries = it.entries[cut:]
			if it.commitStamps != nil {
				it.commitStamps = it.commitStamps[cut:]
			}
		}
		if len(it.entries) > 0 {
			break
		}
		if !s.deliverResponses(it.resps) {
			return false
		}
	}
	*items = rest[i:]
	return true
}

// roundTrip sends one request to the Ready loop and waits for it to be
// served. It returns false when the store is shutting down (the loop may
// already be gone); the caller abandons the current item — it is re-delivered
// on restart.
func (s *Store) roundTrip(req workerReq) bool {
	select {
	case s.workerReqCh <- req:
	case <-s.loopCtx.Done():
		return false
	}
	select {
	case <-req.done:
		return true
	case <-s.loopCtx.Done():
		return false
	}
}

// installSnapshot installs a snapshot received from the leader, on the apply
// worker: restore the FSM from its metadata — which may trigger a slow
// out-of-band state transfer, safe here where it cannot stall the Ready loop
// — then round-trip to the loop to record ConfState / lastSnapshotIndex,
// discard the now-stale log prefix, and only THEN deliver the snapshot's
// held responses (see appendWorker: the self-directed MsgStorageAppendResp
// acks the snapshot as applied and the MsgAppResp reports this follower
// caught up, so neither may leave before the restore has truly succeeded).
//
// A failing restore is retried with capped backoff until it succeeds or the
// store stops: while it fails, no success is recorded anywhere — bookkeeping,
// compaction, and every response stay held, pausing this group's apply
// pipeline loudly instead of recording a snapshot the shard does not actually
// have. The leader meanwhile keeps the follower in StateSnapshot; send-side
// failure reporting (raft.ReportSnapshot) is deliberately not wired here —
// it belongs to the transfer-observability work.
//
// Returns false when the store shuts down mid-install.
func (s *Store) installSnapshot(item applyItem) bool {
	snap := item.snap
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := s.restoreFSMFromSnapshot(*snap)
		if err == nil {
			break
		}
		s.log.WithField("index", snap.Metadata.Index).Errorf(
			"restore FSM from received snapshot failed (attempt %d), group apply paused until it succeeds: %v",
			attempt, err,
		)
		select {
		case <-time.After(backoff):
		case <-s.loopCtx.Done():
			return false
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	if !s.roundTrip(workerReq{snap: snap, done: make(chan struct{})}) {
		return false
	}
	if err := s.sharedLog.Compact(s.groupID, snap.Metadata.Index+1); err != nil {
		s.log.Warnf("compact log after snapshot install: %v", err)
	}
	if !s.deliverResponses(item.resps) {
		return false
	}
	if item.restored != nil {
		close(item.restored)
	}
	return true
}

// restoreFSMFromSnapshot decodes a snapshot's payload and restores the FSM
// from it. A snapshot without payload carries no FSM state (nothing to do).
func (s *Store) restoreFSMFromSnapshot(snap raftpb.Snapshot) error {
	if len(snap.Data) == 0 {
		return nil
	}
	var meta shardSnapshotData
	if err := json.Unmarshal(snap.Data, &meta); err != nil {
		return fmt.Errorf("decode snapshot data: %w", err)
	}
	return s.fsm.RestoreFromSnapshot(meta)
}

// appendTicket is one MsgStorageAppend whose covering sharedlog flush is
// still outstanding. done resolves with the flush result; a nil done marks a
// payload-free message (nothing to persist), whose responses are predicated
// only on PRIOR appends and therefore deliver when the ticket reaches the
// FIFO head.
type appendTicket struct {
	msg  *raftpb.Message
	done <-chan error
}

// appendWorker is the per-Store storage-append thread of the
// AsyncStorageWrites topology: it consumes MsgStorageAppend work in Ready
// order and persists it through the node-wide sharedlog. Writes are
// PIPELINED: each message is submitted to the sharedlog batcher without
// waiting for the previous message's fsync (AsyncStorageWrites explicitly
// permits multiple outstanding appends), so every append staged while a flush
// is in flight rides the next flush together — one fsync covers them all,
// instead of one fsync per message. The durability gate is unchanged: a
// ticket's responses (MsgAppResp / MsgVoteResp / MsgPreVoteResp and the
// self-directed MsgStorageAppendResp) are delivered only after its covering
// fsync resolves, in strict FIFO order — etcd's same-target in-order
// processing requirement.
//
// Snapshot work is a pipeline barrier: locally created snapshot persists and
// received-snapshot installs drain every outstanding ticket first, then run
// fully synchronously (persist, compact, and — for received snapshots — the
// FSM restore sequenced through the apply worker), exactly as before
// pipelining.
//
// The ticket FIFO needs no cap of its own: tickets reference raft's unstable
// log (no copies), which MaxUncommittedEntriesSize bounds on a leader and
// MaxInflightBytes on a follower.
//
// On Stop the worker abandons queued items and outstanding tickets alike: an
// append whose responses were never delivered was never acknowledged
// anywhere, so dropping it is crash-equivalent — the leader re-probes and
// re-sends after restart. (The sharedlog still completes submitted writes;
// its inflight accounting is batcher-side, so abandoned tickets cannot wedge
// sharedlog.Close.)
func (s *Store) appendWorker() {
	defer close(s.appendDone)
	var inflight []appendTicket
	for {
		// Deterministic abandon, mirroring applyWorker.
		if s.loopCtx.Err() != nil {
			return
		}
		// Resolve payload-free heads immediately: everything before them has
		// completed (FIFO), which is all their responses are predicated on.
		if len(inflight) > 0 && inflight[0].done == nil {
			if !s.completeAppendHead(&inflight, nil) {
				return
			}
			continue
		}
		if len(inflight) == 0 {
			select {
			case <-s.loopCtx.Done():
				return
			case item, ok := <-s.appendCh:
				if !ok {
					return
				}
				if !s.admitAppend(item, &inflight) {
					return
				}
			}
			continue
		}
		select {
		case <-s.loopCtx.Done():
			return
		case item, ok := <-s.appendCh:
			if !ok {
				return
			}
			if !s.admitAppend(item, &inflight) {
				return
			}
		case err := <-inflight[0].done:
			if !s.completeAppendHead(&inflight, err) {
				return
			}
		}
	}
}

// admitAppend routes one appendItem into the pipeline: snapshot work drains
// the pipeline and runs synchronously (see appendWorker); everything else is
// submitted to the sharedlog batcher and queued as a ticket. Returns false
// when the store shuts down.
func (s *Store) admitAppend(item appendItem, inflight *[]appendTicket) bool {
	if item.snap != nil {
		if !s.drainAppendInflight(inflight) {
			return false
		}
		return s.persistLocalSnapshot(*item.snap)
	}
	m := item.msg
	if m.Snapshot != nil {
		if !s.drainAppendInflight(inflight) {
			return false
		}
		return s.handleSnapshotAppend(*m)
	}
	gw := sharedlog.GroupWrite{GroupID: s.groupID, Entries: m.Entries}
	hs := raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
	if !raft.IsEmptyHardState(hs) {
		gw.HardState = &hs
	}
	if len(gw.Entries) == 0 && gw.HardState == nil {
		// Payload-free: nothing to persist. Deliver now if the pipeline is
		// empty, else queue behind the outstanding fsyncs (FIFO).
		if len(*inflight) == 0 {
			return s.deliverResponses(m.Responses)
		}
		*inflight = append(*inflight, appendTicket{msg: m})
		return true
	}
	done, err := s.sharedLog.AppendAsync(s.loopCtx, gw)
	if err != nil {
		// Store closed or shutdown race: nothing was persisted and no
		// response will be delivered — crash-equivalent, the leader re-sends.
		return false
	}
	*inflight = append(*inflight, appendTicket{msg: m, done: done})
	return true
}

// completeAppendHead finishes the pipeline's head ticket after its covering
// flush resolved (err nil for payload-free tickets): a failed flush upholds
// the durability panic unless the store is closing; a successful one delivers
// the head's responses. Returns false when the store shuts down.
func (s *Store) completeAppendHead(inflight *[]appendTicket, err error) bool {
	if err != nil {
		if errors.Is(err, sharedlog.ErrStoreClosed) {
			return false
		}
		panic(fmt.Sprintf("shard raft %s/%s: durability invariant violated persisting raft state: %v",
			s.config.ClassName, s.config.ShardName, err))
	}
	head := (*inflight)[0]
	(*inflight)[0] = appendTicket{}
	*inflight = (*inflight)[1:]
	return s.deliverResponses(head.msg.Responses)
}

// drainAppendInflight completes every outstanding ticket in FIFO order — the
// pipeline barrier in front of snapshot work. Returns false when the store
// shuts down.
func (s *Store) drainAppendInflight(inflight *[]appendTicket) bool {
	for len(*inflight) > 0 {
		if (*inflight)[0].done == nil {
			if !s.completeAppendHead(inflight, nil) {
				return false
			}
			continue
		}
		select {
		case err := <-(*inflight)[0].done:
			if !s.completeAppendHead(inflight, err) {
				return false
			}
		case <-s.loopCtx.Done():
			return false
		}
	}
	return true
}

// handleSnapshotAppend persists a snapshot-carrying MsgStorageAppend durably
// (snapshot plus any entries/HardState riding the same message), then
// sequences the FSM restore into the apply worker and holds this thread —
// and therefore all later appends' responses (FIFO) — until the restore
// succeeds. Runs only with the pipeline drained. Returns false when the store
// shuts down.
func (s *Store) handleSnapshotAppend(m raftpb.Message) bool {
	gw := sharedlog.GroupWrite{GroupID: s.groupID, Entries: m.Entries, Snapshot: m.Snapshot}
	hs := raftpb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit}
	if !raft.IsEmptyHardState(hs) {
		gw.HardState = &hs
	}
	if err := s.sharedLog.Append(context.Background(), gw); err != nil {
		if errors.Is(err, sharedlog.ErrStoreClosed) {
			// Shutdown race: nothing was persisted and no response will
			// be delivered — crash-equivalent, the leader re-sends.
			return false
		}
		panic(fmt.Sprintf("shard raft %s/%s: durability invariant violated persisting raft state: %v",
			s.config.ClassName, s.config.ShardName, err))
	}
	return s.sequenceSnapshotInstall(m)
}

// sequenceSnapshotInstall hands a durably persisted received snapshot to the
// apply worker for FSM restore and blocks until the install completes.
// Blocking here is what keeps response order intact: raft pauses
// committed-entry emission while a snapshot is in flight, earlier
// MsgStorageApply batches are already queued ahead of the install item, and
// later MsgStorageAppend responses queue behind this wait. Returns false when
// the store shuts down.
func (s *Store) sequenceSnapshotInstall(m raftpb.Message) bool {
	restored := make(chan struct{})
	select {
	case s.applyCh <- applyItem{snap: m.Snapshot, resps: m.Responses, restored: restored}:
	case <-s.loopCtx.Done():
		return false
	}
	select {
	case <-restored:
		return true
	case <-s.loopCtx.Done():
		return false
	}
}

// persistLocalSnapshot persists a locally created snapshot (from a completed
// Snapshotter job), compacts the log behind it, and reports the new snapshot
// index back to the loop. Runs on the append worker so no shared-log fsync
// ever blocks the Ready loop. Returns false when the store shuts down.
func (s *Store) persistLocalSnapshot(snap raftpb.Snapshot) bool {
	if err := s.sharedLog.Append(context.Background(), sharedlog.GroupWrite{
		GroupID:  s.groupID,
		Snapshot: &snap,
	}); err != nil {
		if errors.Is(err, sharedlog.ErrStoreClosed) {
			return false
		}
		panic(fmt.Sprintf("shard raft %s/%s: durability invariant violated persisting snapshot: %v",
			s.config.ClassName, s.config.ShardName, err))
	}
	if err := s.sharedLog.Compact(s.groupID, snap.Metadata.Index+1); err != nil {
		s.log.Warnf("compact log after snapshot: %v", err)
	}
	select {
	case s.snapPersistedCh <- snap.Metadata.Index:
		return true
	case <-s.loopCtx.Done():
		return false
	}
}

// deliverResponses forwards a storage message's response set after its write
// or apply completed: peer-targeted messages (MsgAppResp, MsgVoteResp,
// MsgPreVoteResp) go over the transport; self-targeted ones (MsgAppResp to
// self, MsgStorageAppendResp, MsgStorageApplyResp) are stepped back into the
// Ready loop via localMsgCh — reliably and in slice order, preserving the
// library's requirement that the self MsgAppResp is handled before its
// MsgStorageAppendResp. Returns false when the store shuts down; undelivered
// responses are crash-equivalent and re-derived on restart.
func (s *Store) deliverResponses(resps []raftpb.Message) bool {
	var peer []raftpb.Message
	for i := range resps {
		if resps[i].To != s.localID {
			peer = append(peer, resps[i])
			continue
		}
		select {
		case s.localMsgCh <- resps[i]:
		case <-s.loopCtx.Done():
			return false
		}
	}
	if len(peer) > 0 {
		s.transport.Send(s.groupID, peer)
	}
	return true
}

// applyEntries materializes one staged batch — the single-item form of
// applyItems, kept for sites (and tests) that operate on one applyItem.
// Returns true only when the batch fully materialized (no park, no abort).
func (s *Store) applyEntries(item applyItem) bool {
	parked, ok := s.applyItems([]applyItem{item})
	return ok && parked == nil
}

// applyParked reports a materialization run stopping at one committed entry
// for an environmental reason: everything before the entry landed (applied
// advanced exactly that far, covered responses delivered), the entry and
// everything after wait in remaining — a trimmed backlog the caller retries.
type applyParked struct {
	index     uint64      // the parked entry's raft log index
	err       error       // the environmental error that parked it
	remaining []applyItem // the parked entry and everything after, responses preserved
}

// trimItemsAt returns the sub-run starting at flat entry position p: items
// fully materialized before p are dropped (their responses were already
// delivered), the item containing p keeps its entry suffix (commit stamps
// trimmed alike) and its responses — an item's MsgStorageApplyResp delivers
// only once its LAST entry materializes, so a partially-consumed item's
// responses travel with the remainder.
func trimItemsAt(items []applyItem, p int) []applyItem {
	start := 0
	for i := range items {
		end := start + len(items[i].entries)
		if end <= p {
			start = end
			continue
		}
		out := make([]applyItem, 0, len(items)-i)
		it := items[i]
		if off := p - start; off > 0 {
			it.entries = it.entries[off:]
			if it.commitStamps != nil {
				it.commitStamps = it.commitStamps[off:]
			}
		}
		out = append(out, it)
		return append(out, items[i+1:]...)
	}
	return nil
}

// applyItems materializes a drained run of staged committed-entry batches on
// the apply worker. The client ack already happened at commit staging
// (ackCommitted), so per-entry outcomes here are never client-visible: a
// deterministic failure is skipped identically on every replica (counted at
// the FSM), and an environmental failure PARKS the run — applyItems returns
// the park point with the un-materialized remainder; the caller
// (applyItemsParking) owns the retry. Writes are never discarded.
//
// The run is flattened into one log-order entry sequence. Command entries are
// handed to the FSM in segments (DispatchBatch merges consecutive put-batch
// commands into coalesced LSM rounds); conf-change and other non-command
// entries split segments — the pending segment materializes first, then the
// entry round-trips to the Ready loop (which owns the RawNode), keeping every
// mutation in log order. The applied watermark only ever advances behind
// materialization (per completed unit, exactly to the last complete entry on
// a park; conf changes as today).
//
// Each original item's responses (its MsgStorageApplyResp) are delivered as
// soon as the unit covering the item's last entry completes, in item order —
// never before every entry the item covers has materialized. Entries stamped
// at commit observe the commit→apply histogram at their covering unit's
// completion. Returns ok=false when the store shuts down mid-run.
func (s *Store) applyItems(items []applyItem) (parked *applyParked, ok bool) {
	var ents []raftpb.Entry
	var stamps []time.Time
	type itemMark struct {
		flatEnd int // flat index of the item's last entry; -1 for empty items
		resps   []raftpb.Message
	}
	marks := make([]itemMark, 0, len(items))
	for i := range items {
		for j := range items[i].entries {
			var st time.Time
			if items[i].commitStamps != nil {
				st = items[i].commitStamps[j]
			}
			ents = append(ents, items[i].entries[j])
			stamps = append(stamps, st)
		}
		marks = append(marks, itemMark{flatEnd: len(ents) - 1, resps: items[i].resps})
	}

	nextMark := 0
	deliverThrough := func(flat int) bool {
		for nextMark < len(marks) && marks[nextMark].flatEnd <= flat {
			if !s.deliverResponses(marks[nextMark].resps) {
				return false
			}
			nextMark++
		}
		return true
	}
	// Items with no entries ahead of any command deliver immediately.
	if !deliverThrough(-1) {
		return nil, false
	}

	var cmds []fsmCmd
	var cmdFlat []int // flat entry index per cmd
	flushCmds := func() (*applyParked, bool) {
		if len(cmds) == 0 {
			return nil, true
		}
		park, dispatchOK := s.fsm.DispatchBatch(cmds, func(from, to int, resps []Response) bool {
			for c := from; c <= to; c++ {
				if resps[c-from].Error != nil {
					shardRaftApplyDispatchFailures.WithLabelValues(s.config.ClassName, s.config.ShardName).Inc()
					s.log.WithField("index", cmds[c].index).Errorf(
						"post-commit dispatch failed — node-local invariant violation, not client-visible: %v", resps[c-from].Error)
				}
				if st := stamps[cmdFlat[c]]; !st.IsZero() {
					shardRaftCommitApply.WithLabelValues(s.config.ClassName, s.config.ShardName).
						Observe(time.Since(st).Seconds())
				}
			}
			return deliverThrough(cmdFlat[to])
		})
		if !dispatchOK {
			return nil, false
		}
		var fp *applyParked
		if park != nil {
			fp = &applyParked{
				index:     park.index,
				err:       park.err,
				remaining: trimItemsAt(items, cmdFlat[park.cmd]),
			}
		}
		cmds, cmdFlat = cmds[:0], cmdFlat[:0]
		return fp, true
	}

	for i := range ents {
		ent := ents[i]
		switch ent.Type {
		case raftpb.EntryNormal:
			// Empty and malformed entries become nil-payload no-op commands:
			// their applied-index bookkeeping rides the segment, so it can
			// never run ahead of an unmaterialized earlier entry.
			var payload []byte
			if len(ent.Data) > 0 {
				if _, p, ok := decodeCmd(ent.Data); ok {
					payload = p
				} else {
					s.log.WithField("index", ent.Index).Error("malformed command entry, skipping")
				}
			}
			cmds = append(cmds, fsmCmd{payload: payload, index: ent.Index})
			cmdFlat = append(cmdFlat, i)
		case raftpb.EntryConfChange:
			if park, flushOK := flushCmds(); !flushOK {
				return nil, false
			} else if park != nil {
				return park, true
			}
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ent.Data); err != nil {
				s.log.Errorf("unmarshal conf change: %v", err)
			} else if !s.roundTrip(workerReq{cc: cc, done: make(chan struct{})}) {
				return nil, false
			}
			s.fsm.setApplied(ent.Index)
			if !deliverThrough(i) {
				return nil, false
			}
		case raftpb.EntryConfChangeV2:
			if park, flushOK := flushCmds(); !flushOK {
				return nil, false
			} else if park != nil {
				return park, true
			}
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(ent.Data); err != nil {
				s.log.Errorf("unmarshal conf change v2: %v", err)
			} else if !s.roundTrip(workerReq{cc: cc, done: make(chan struct{})}) {
				return nil, false
			}
			s.fsm.setApplied(ent.Index)
			if !deliverThrough(i) {
				return nil, false
			}
		default:
			if park, flushOK := flushCmds(); !flushOK {
				return nil, false
			} else if park != nil {
				return park, true
			}
			s.fsm.setApplied(ent.Index)
			if !deliverThrough(i) {
				return nil, false
			}
		}
	}
	return flushCmds()
}

// handleSoftState records a leadership change and wakes waiters; on losing
// leadership it fails all pending Applies and ReadIndex requests.
func (s *Store) handleSoftState(ss *raft.SoftState) {
	prev := ShardRaftState(s.state.Load())
	next := mapRaftState(ss.RaftState)
	s.state.Store(uint32(next))

	// A leader emerged before the birth campaign fired (a peer won the birth
	// race, or this group joined an established configuration): the birth
	// designation's window is over.
	if s.birthCampaignArmed && ss.Lead != raft.None {
		s.birthCampaignArmed = false
	}

	prevLeader := s.leaderID.Load()
	s.leaderID.Store(ss.Lead)
	if ss.Lead != prevLeader {
		select {
		case s.leaderCh <- struct{}{}:
		default:
		}
	}

	if prev == ShardStateLeader && next != ShardStateLeader {
		s.logStepDownForensics()
		s.drainPending(ErrLeadershipLost)
		// etcd silently drops pending ReadIndex requests on step-down (no
		// ReadState is ever produced), so we must drain them here.
		s.drainPendingReads(ErrLeadershipLost)
	}
}

// logStepDownForensics records, at the moment this node loses leadership, when
// each peer last responded and where the watchdog last saw its Match — the
// evidence needed to attribute a CheckQuorum step-down (slow follower disk vs
// dead transport vs wedged replication) after the fact. Runs on the Ready
// loop; raft's own Progress is already reset at this point, so the loop-local
// tracking state is the only surviving record.
func (s *Store) logStepDownForensics() {
	now := time.Now()
	fields := logrus.Fields{}
	for _, m := range s.config.Members {
		id := hashNodeID(m)
		if id == s.localID {
			continue
		}
		fields["last_response_"+m] = s.respAge(id, now)
		if tr, ok := s.wedgeTrack[id]; ok {
			fields["match_"+m] = tr.match
		}
	}
	s.log.WithFields(fields).Warn("lost shard raft leadership; per-follower forensics attached")
}

// handlePropose proposes a queued command, or fails it fast if not leader or
// if the commit→apply lag bound is exceeded.
func (s *Store) handlePropose(p proposal) {
	if ShardRaftState(s.state.Load()) != ShardStateLeader {
		s.wakePending(p.reqID, applyResult{err: ErrNotLeader})
		return
	}
	// Ack-at-commit removes the client ack as an implicit apply throttle, so
	// the apply pipeline carries its own bound: past it, proposals surface
	// the same retryable same-node backpressure as a full uncommitted log —
	// write pressure becomes throttling, and the applied-waits behind
	// linearizable reads stay bounded by the cap. The applied watermark
	// advances in materialization-unit jumps (up to one coalesced put window
	// at a time — see applyItems), so the effective tolerance is up to one
	// window's entry count narrower than the configured cap.
	if applied := s.fsm.LastAppliedIndex(); s.committedStaged.Load() > applied+s.maxCommitApplyLag {
		s.wakePending(p.reqID, applyResult{err: ErrProposalBackpressure})
		return
	}
	if err := s.rawNode.Propose(p.data); err != nil {
		// On a node that IS the leader, a dropped proposal is backpressure
		// (uncommitted log over MaxUncommittedEntriesSize, or a leadership
		// transfer in progress) — retryable here, not a cue to reroute the
		// caller to another node.
		if errors.Is(err, raft.ErrProposalDropped) {
			s.wakePending(p.reqID, applyResult{err: ErrProposalBackpressure})
			return
		}
		s.wakePending(p.reqID, applyResult{err: ErrNotLeader})
	}
}

// handleReadIndex issues a ReadIndex round for a queued VerifyLeader, or fails
// it fast if this node is not the leader. Runs on the Ready loop.
func (s *Store) handleReadIndex(rctx []byte) {
	if ShardRaftState(s.state.Load()) != ShardStateLeader {
		s.wakePendingRead(rctx, readResult{err: ErrNotLeader})
		return
	}
	s.rawNode.ReadIndex(rctx)
}

// maybeSnapshot dispatches a snapshot job once the applied-entry delta OR the
// retained committed-byte volume has grown past its threshold (see
// snapshot_cadence.go for the policy, the Match floor, and the escape hatch).
// Snapshots are advisory: if the pool is busy the attempt simply retries on a
// later round.
//
// Runs at the end of every processReady, so trigger evaluation is per-Ready —
// two integer compares per round; the Progress copy behind voterMatchFloor
// happens only once a trigger has fired. Overshoot past the byte threshold is
// therefore bounded by the bytes committed during one snapshot round-trip
// (submit → flush → persist, sub-second) while snapshotPending gates
// re-triggering — a few MB at measured import rates, small against the 32MiB
// default threshold.
func (s *Store) maybeSnapshot() {
	if s.snapshotPending {
		return
	}
	applied := s.fsm.LastAppliedIndex()
	var entriesDelta uint64
	if applied > s.lastSnapshotIndex {
		entriesDelta = applied - s.lastSnapshotIndex
	}
	tail := s.snapMarks.tail()
	fire, trigger, escape := snapshotDue(entriesDelta, tail, s.snapEntryThreshold, s.snapBytesThreshold)
	if !fire {
		// Age floor: a small group's retained tail never reaches either
		// threshold, so bound its age instead — otherwise restart replay for
		// such a group grows with its full history. entriesDelta==0 is the
		// idle short-circuit: no retained entries, no clock read, no work.
		// The age trigger never escapes the Match floor (nothing urgent
		// about a small tail) and, like every trigger, its index is capped
		// by the durable flush watermark below.
		if entriesDelta == 0 || s.snapMinInterval <= 0 ||
			time.Since(s.lastSnapshotProgressAt) < s.snapMinInterval {
			return
		}
		trigger = snapshotTriggerAge
	}
	sh := s.fsm.getShard()
	if sh == nil {
		return
	}
	floor, haveFloor := s.voterMatchFloor()
	// The durable floor is read AFTER applied (its concurrency contract): a
	// bucket holding writes of entries <= applied is guaranteed visible to
	// the read, so the cap can never overshoot what is actually flushed.
	target, floorCapped, wmCapped := snapshotIndex(applied, floor, haveFloor, escape, sh.DurableRaftFloor())
	if target <= s.lastSnapshotIndex {
		// The Match floor or the flush watermark pins the snapshot at (or
		// below) the one we already have: retain the log and re-evaluate next
		// round. For the Match floor the escape hatch bounds the wait; for
		// the flush watermark the background flush cadence advances it.
		return
	}
	err := s.snapshotter.Submit(SnapshotRequest{
		GroupID:      s.groupID,
		ClassName:    s.config.ClassName,
		ShardName:    s.config.ShardName,
		NodeID:       s.config.NodeID,
		AppliedIndex: target,
		Flusher:      sh,
		Result:       s.snapResultCh,
	})
	if err != nil {
		return // busy/closed — retry next round
	}
	s.snapshotPending = true
	s.pendingSnapMeta = pendingSnapMeta{
		trigger:     trigger,
		tailBytes:   tail,
		floorCapped: floorCapped,
		wmCapped:    wmCapped,
		escape:      escape,
	}
}

// voterMatchFloor returns the minimum replication Match among peer voters —
// the compaction floor while this node leads. The second result is false on
// non-leaders (followers track no Progress and serve no one — they compact on
// their own applied progress, today's semantics) and on single-voter groups.
// Runs on the Ready loop; called only after a trigger fired, because Status()
// copies the Progress map.
func (s *Store) voterMatchFloor() (uint64, bool) {
	if s.rawNode.BasicStatus().RaftState != raft.StateLeader {
		return 0, false
	}
	st := s.rawNode.Status()
	var floor uint64
	found := false
	for id, pr := range st.Progress {
		if id == s.localID || pr.IsLearner {
			continue
		}
		if !found || pr.Match < floor {
			floor, found = pr.Match, true
		}
	}
	return floor, found
}

// completeLocalSnapshot records a persisted-and-compacted local snapshot on
// the Ready loop: advance the snapshot bookkeeping, rebase the cadence byte
// accounting, and emit the per-snapshot observability (log line + trigger
// counter). idx is always above the previous snapshot index (maybeSnapshot
// skips non-advancing targets).
func (s *Store) completeLocalSnapshot(idx uint64) {
	prev := s.lastSnapshotIndex
	s.lastSnapshotIndex = idx
	s.snapshotPending = false
	s.lastSnapshotProgressAt = time.Now()
	s.snapMarks.pruneTo(idx)
	meta := s.pendingSnapMeta
	shardRaftSnapshots.WithLabelValues(s.config.ClassName, s.config.ShardName, meta.trigger).Inc()
	s.log.WithFields(logrus.Fields{
		"trigger":           meta.trigger,
		"tail_bytes":        meta.tailBytes,
		"entries_covered":   idx - prev,
		"snapshot_index":    idx,
		"compacted_through": idx + 1,
		"floor_capped":      meta.floorCapped,
		"wm_capped":         meta.wmCapped,
		"floor_escape":      meta.escape,
	}).Info("shard raft snapshot persisted and log compacted")
}

// onSnapshotResult stages a completed snapshot for persistence by the append
// worker (no shared-log fsync may run on the Ready loop). snapshotPending stays
// true until the worker reports back over snapPersistedCh, which then updates
// lastSnapshotIndex — so at most one snapshot is ever in flight. Runs
// single-threaded on the Ready loop.
func (s *Store) onSnapshotResult(r SnapshotResult) {
	if r.Err != nil {
		s.snapshotPending = false
		s.log.WithField("index", r.Index).Warnf("snapshot job failed: %v", r.Err)
		return
	}
	term, err := s.raftStorage.Term(r.Index)
	if err != nil {
		s.snapshotPending = false
		s.log.WithField("index", r.Index).Warnf("snapshot index already compacted, skipping: %v", err)
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
	s.pendingAppendQ = append(s.pendingAppendQ, appendItem{snap: &snap})
}

// loadFromPending resolves a reqID to its in-flight Apply. reqIDs whose salt
// is not this Store instance's are rejected before the lookup: applied entries
// from other nodes' proposals (and replays from a previous boot of this node)
// carry foreign salts, and must never resolve — a colliding counter would wake
// an unrelated waiter with a false success (see the pending field docs).
func (s *Store) loadFromPending(reqID uint64) (*pendingApply, bool) {
	if reqID&reqIDSaltMask != s.reqIDSalt {
		return nil, false
	}
	pAny, ok := s.pending.Load(reqID)
	if !ok {
		return nil, false
	}
	p, ok := pAny.(*pendingApply)
	if !ok {
		return nil, false
	}
	return p, true
}

// ackCommitted acknowledges, on the Ready loop, every locally-proposed entry
// in a committed batch being staged for the apply worker — the ack site: the
// entry is quorum-committed and locally durable, its outcome is decided (raft
// applies committed entries on every replica), so the waiting Apply is woken
// with the entry index without waiting for local materialization. The salt
// guard in loadFromPending is what keeps foreign and cross-boot entries from
// falsely acking a local pending; for those the per-entry cost stays an
// 8-byte prefix read plus the guard.
//
// This is also the commit-side observation point of the histogram pair:
// propose→commit is observed here, and the batch's commit stamp is returned
// as a slice parallel to entries (nil when none is local) so the apply worker
// can observe commit→apply at dispatch completion — the pendingApply is
// deleted when Apply returns, so the stamp must travel with the applyItem.
// The committedStaged watermark advances for every staged batch, local
// proposer or not.
func (s *Store) ackCommitted(entries []raftpb.Entry) []time.Time {
	if n := len(entries); n > 0 {
		s.committedStaged.Store(entries[n-1].Index)
	}
	var stamps []time.Time
	var now time.Time
	for i := range entries {
		ent := &entries[i]
		// Byte accounting for the snapshot cadence: every committed-staged
		// entry (commands and conf changes alike) is retained log volume.
		s.snapMarks.observe(ent.Index, uint64(ent.Size()))
		if ent.Type != raftpb.EntryNormal || len(ent.Data) < 8 {
			continue
		}
		reqID := binary.BigEndian.Uint64(ent.Data[:8])
		p, ok := s.loadFromPending(reqID)
		if !ok {
			continue
		}
		if now.IsZero() {
			now = time.Now()
		}
		if stamps == nil {
			stamps = make([]time.Time, len(entries))
		}
		stamps[i] = now
		shardRaftProposeCommit.WithLabelValues(s.config.ClassName, s.config.ShardName).
			Observe(now.Sub(p.proposedAt).Seconds())
		s.wakePending(reqID, applyResult{idx: ent.Index})
	}
	return stamps
}

// wakePending delivers a result to a waiting Apply, if one is registered.
// The send is non-blocking: a result may already have been delivered (e.g. a
// leadership-loss drain racing a commit), in which case this is a no-op.
func (s *Store) wakePending(reqID uint64, res applyResult) {
	p, ok := s.loadFromPending(reqID)
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
	s.pending.Range(func(key, value any) bool {
		p, ok := value.(*pendingApply)
		if !ok {
			return true
		}
		select {
		case p.done <- applyResult{err: err}:
		default:
		}
		return true
	})
}

// wakePendingRead delivers a VerifyLeader result to its waiter, if registered.
// The send is non-blocking: a result may already have been delivered (e.g. a
// leadership-loss drain racing a ReadState), in which case this is a no-op.
func (s *Store) wakePendingRead(rctx []byte, res readResult) {
	pAny, ok := s.pendingReads.Load(string(rctx))
	if !ok {
		return
	}
	p, ok := pAny.(*pendingRead)
	if !ok {
		return
	}
	select {
	case p.done <- res:
	default:
	}
}

// drainPendingReads fails every in-flight VerifyLeader with err.
func (s *Store) drainPendingReads(err error) {
	s.pendingReads.Range(func(key, value any) bool {
		p, ok := value.(*pendingRead)
		if !ok {
			return true
		}
		select {
		case p.done <- readResult{err: err}:
		default:
		}
		return true
	})
}

// step hands an inbound raft message to the Ready loop. Called by the
// Registry's MessageRouter. Non-blocking: a message that arrives before Start
// or after Stop, or when the queue is full, is dropped — raft re-sends.
//
// Local-only messages are rejected at this trust boundary: storage-protocol
// types (MsgStorageAppend/Apply and their responses) and messages claiming a
// local-thread From/To must never arrive from the network — a spoofed
// MsgStorageAppendResp would corrupt the unstable-log bookkeeping.
func (s *Store) step(msg raftpb.Message) {
	if raft.IsLocalMsg(msg.Type) || raft.IsLocalMsgTarget(msg.From) || raft.IsLocalMsgTarget(msg.To) {
		shardRaftDropped.WithLabelValues(dropSiteStepLocalSpoof).Inc()
		s.log.WithField("type", msg.Type.String()).Warn("dropping inbound raft message with local-only type or target")
		return
	}
	s.mu.RLock()
	ch := s.incomingMsgCh
	live := s.started && !s.closed
	s.mu.RUnlock()
	if !live || ch == nil {
		shardRaftDropped.WithLabelValues(dropSiteStepNotLive).Inc()
		if stepDropLog.Allow(groupLabel(s.groupID)) {
			s.log.WithField("type", msg.Type.String()).Warn("dropping inbound raft message: store not started or already stopped")
		}
		return
	}
	select {
	case ch <- msg:
	default:
		shardRaftDropped.WithLabelValues(dropSiteStepQueueFull).Inc()
		s.log.WithField("type", msg.Type).Warn("dropping inbound raft message: queue full")
	}
}

// stepDropLog rate-limits the not-live drop WARN per group: during a normal
// shard init every pre-registration message would otherwise log a line.
var stepDropLog = newLogLimiter(time.Second)

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
	applyCh := s.applyCh
	workerDone := s.workerDone
	appendCh := s.appendCh
	appendDone := s.appendDone
	s.mu.Unlock()

	s.log.Info("stopping shard RAFT store")

	loopCancel()
	<-loopDone

	// The loop has exited (it is the sole sender on both worker queues), so
	// closing them is safe. The append worker exits first: it abandons
	// queued appends (never acknowledged — the leader re-sends after
	// restart) and finishes only an in-flight sharedlog batch; it must be
	// gone before applyCh closes because a snapshot install makes it an
	// applyCh sender. The apply worker then abandons anything still queued —
	// committed entries re-deliver on restart — finishes only its in-flight
	// item, and exits. Waiting for both before returning upholds the
	// teardown contract: after Stop, nothing touches the shard or the
	// shared log.
	close(appendCh)
	<-appendDone
	close(applyCh)
	<-workerDone

	// Fail any Apply / VerifyLeader still waiting on a result the (now
	// stopped) loop and worker will never deliver.
	s.drainPending(ErrAlreadyClosed)
	s.drainPendingReads(ErrAlreadyClosed)

	s.mu.Lock()
	s.started = false
	s.mu.Unlock()

	s.log.Info("shard RAFT store stopped")
	return nil
}

// Apply applies a command to the RAFT cluster. It blocks until the command is
// quorum-committed and locally durable — NOT until the local FSM has applied
// it — or the context is cancelled. The returned index is the committed
// entry's log index; local materialization completes asynchronously on the
// apply worker, bounded by the commit→apply lag cap (past it, new proposals
// surface ErrProposalBackpressure).
//
// Read-your-writes is owned by the read protocol, not by this ack:
// VerifyLeader completes only once the FSM has applied to at least its
// ReadState's commit index (which covers every previously acked write), and
// GetLastAppliedIndex reports the committed-staged watermark — see
// processReady's ReadStates handling and CommittedIndex.
//
// A post-commit FSM dispatch failure cannot reach this ack: committed entries
// must apply deterministically on every replica, so such a failure is a
// node-local invariant violation, logged and counted at applyEntries. The one
// realistic lifecycle error — Apply before SetShard — fails fast here, before
// proposing.
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
	sh := s.fsm.getShard()
	if sh == nil {
		return 0, fmt.Errorf("shard not set for %s/%s", s.config.ClassName, s.config.ShardName)
	}
	// Reject-fast at admission, before marshal/propose. A read-only (e.g.
	// resource-pressured) shard refuses writes for minutes: proposing would
	// quorum-ack a write whose materialization the whole fleet currently
	// refuses, and driver-side retry against the same leader buys nothing —
	// the full reason crosses to the client with a non-retryable code (2PC
	// parity with the prepare gate). Both checks are best-effort races by
	// nature; the apply-side park/fence machinery carries the durability
	// guarantee regardless of what slips past admission.
	if err := sh.ReadOnlyErr(); err != nil {
		return 0, err
	}
	if !sh.ClassPresent() {
		return 0, fmt.Errorf("%w: class %q (shard %s) rejects new writes during its drop window",
			ErrClassDropped, s.config.ClassName, s.config.ShardName)
	}

	body, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal command: %w", err)
	}
	// Reject oversized commands before proposing: past this point every
	// quota admits an oversized first item (etcd's first-entry exception),
	// and the first hard fence — the per-stripe send-lane byte cap — drops
	// the frame on every re-send, wedging the group permanently. The +8 is
	// encodeCmd's reqID prefix, i.e. the entry's actual payload size.
	if len(body)+8 > maxRaftCommandBytes {
		return 0, fmt.Errorf("%w: command for %s/%s is %d bytes, limit is %d — oversized objects require the sideloading design (plans/oversized-objects.md)",
			ErrCommandTooLarge, s.config.ClassName, s.config.ShardName, len(body)+8, maxRaftCommandBytes)
	}

	reqID := s.reqIDSalt | (s.nextReqID.Add(1) & reqIDCounterMask)
	p := &pendingApply{done: make(chan applyResult, 1), proposedAt: time.Now()}
	s.pending.Store(reqID, p)
	defer s.pending.Delete(reqID)

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

// VerifyLeader confirms this node is still the leader by driving an active
// etcd/raft ReadIndex round, then blocks until the local FSM has applied to at
// least the round's ReadState index — the full linearizable-read barrier:
// quorum-confirmed leadership plus read-your-writes. Apply acks at quorum
// commit, so every acked write's index is <= the commit index the ReadState
// carries; the applied-wait is what makes a local read after VerifyLeader
// observe those writes. Used for linearizable (STRONG/DIRECT) reads.
//
// The ReadState phase is bounded by an internal cap of 2x ElectionTimeout
// composed with the caller's context — a leader that has lost quorum is forced
// to step down by CheckQuorum within ~1 election timeout, which drains the
// pending read. The applied-wait that follows is bounded by the caller's
// context alone: it makes guaranteed progress (the apply worker drains a
// backlog bounded by the commit→apply lag cap), so an election-derived cap
// would only turn a loaded-but-healthy shard into spurious read errors.
func (s *Store) VerifyLeader(ctx context.Context) error {
	s.mu.RLock()
	started, closed, loopDone := s.started, s.closed, s.loopDone
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

	readCtx, cancel := context.WithTimeout(ctx, verifyLeaderTimeout(s.config.ElectionTimeout))
	defer cancel()

	id := s.nextReadID.Add(1)
	rctx := make([]byte, 8)
	binary.BigEndian.PutUint64(rctx, id)
	key := string(rctx)

	p := &pendingRead{done: make(chan readResult, 1)}
	s.pendingReads.Store(key, p)
	defer s.pendingReads.Delete(key)

	select {
	case s.readIndexCh <- rctx:
	case <-readCtx.Done():
		return readCtx.Err()
	case <-loopDone:
		return ErrAlreadyClosed
	}

	var res readResult
	select {
	case res = <-p.done:
	case <-readCtx.Done():
		return readCtx.Err()
	case <-loopDone:
		return ErrAlreadyClosed
	}
	if res.err != nil {
		return res.err
	}
	return s.fsm.WaitForIndex(ctx, res.index)
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

// CommittedIndex returns the highest committed entry index this store has
// staged for local apply — the watermark that covers every acknowledged write
// (Apply acks exactly at staging). Read-your-writes helpers wait for applied
// >= this value; it can run ahead of LastAppliedIndex by up to the
// commit→apply lag cap.
func (s *Store) CommittedIndex() uint64 {
	return s.committedStaged.Load()
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
				t.log.WithField("group", in.groupID).Warnf("route inbound message: %v", err)
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
