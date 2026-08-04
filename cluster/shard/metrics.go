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
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.etcd.io/raft/v3/raftpb"
)

// Message-loss forensics: every raft message is counted at the two ends of the
// node-local pipeline — sender-enqueued (MuxTransport.Send) and
// receiver-routed (Registry.RouteMessage) — per group and per type-class.
// Diffing the two ends across nodes localizes a loss point in one query;
// the 17-hour silent replica-divergence incident was undiagnosable precisely
// because no such ledger existed.
//
// Label cardinality: "group" is the decimal group ID — bounded by the number
// of shard-raft groups a node hosts. Revisit before very-high-shard-count
// deployments (flagged in the migration plan).
var (
	shardRaftMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_messages_total",
		Help: "Raft messages by pipeline point (send=sender-enqueued, route=receiver-routed), type-class, and group.",
	}, []string{"point", "class", "group"})

	// shardRaftDropped counts every message discarded at a site that used to
	// be silent or debug-only. A non-zero rate here during an incident is the
	// first grep.
	shardRaftDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_dropped_total",
		Help: "Raft messages dropped, by drop site.",
	}, []string{"site"})

	// shardRaftStorePanics counts panics recovered on a store's core
	// goroutines (Ready loop, append/apply workers). Each one fails the
	// group on this node (see Store.failGroup) — any increase is an
	// incident-grade signal, not noise.
	shardRaftStorePanics = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_store_panics_total",
		Help: "Panics on shard raft store goroutines; each fails the group on this node.",
	}, []string{"class", "shard", "goroutine"})

	// shardRaftWedgedReplicas is the Match-progress watchdog's gauge: the
	// number of voters this leader considers replication-wedged (Match behind
	// and not advancing for wedgeAfter). Non-zero for more than a scrape
	// interval means a replica is silently diverging RIGHT NOW.
	shardRaftWedgedReplicas = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "weaviate_shard_raft_wedged_replicas",
		Help: "Voters whose replication progress is wedged, per group (leader-side view).",
	}, []string{"class", "shard"})

	// shardRaftLoopPhase measures where Ready-loop wall time goes. The loop
	// must never stall: >100ms in any phase is slow-logged with attribution.
	// The transmit phase covers only frame encode + sender-lane enqueue; the
	// wire writes happen on per-peer sender goroutines (shardRaftSendPeer).
	shardRaftLoopPhase = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_loop_phase_seconds",
		Help:    "Ready-loop time per phase (step, tick_replay, ready_get, transmit, ready_drain).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 4, 10), // 0.5ms .. ~131s
	}, []string{"phase"})

	// shardRaftSendPeer splits a frame's transport journey into queue wait vs
	// stream write, per destination node — discriminates sender-lane backlog
	// (slow peer, frames queueing) from network/window stalls (the write
	// itself crawling). Both phases are observed on the per-peer sender
	// goroutines; nothing here runs on a Ready loop.
	shardRaftSendPeer = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_send_peer_seconds",
		Help:    "Per-peer transport send durations, split into queue_wait (enqueue to sender-lane pickup) and write (stream write).",
		Buckets: prometheus.ExponentialBuckets(0.0005, 4, 10),
	}, []string{"peer", "phase"})

	// shardRaftChanOccupancy samples loop-channel fill ratios once per tick.
	// Sustained occupancy near 1.0 on a channel identifies the backpressure
	// edge (inbound steps vs storage responses vs worker handoff).
	shardRaftChanOccupancy = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_chan_occupancy_ratio",
		Help:    "Ready-loop channel occupancy ratio sampled each tick.",
		Buckets: []float64{0, 0.25, 0.5, 0.75, 0.9, 1},
	}, []string{"channel"})

	// shardRaftProposeCommit / shardRaftCommitApply decompose the leader-side
	// write path into its two halves: consensus (proposal handed to raft →
	// entry quorum-committed, dominated by log fsync and replication —
	// observed at the commit ack site on the Ready loop, so it IS the
	// client-visible Apply latency) and materialization (committed → FSM
	// apply finished, including apply-queue wait — observed by the apply
	// worker via the commit stamps carried on each applyItem; no longer
	// client-visible, but it bounds linearizable-read applied-waits and
	// feeds the commit→apply lag backpressure).
	shardRaftProposeCommit = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_propose_commit_seconds",
		Help:    "Leader-side time from client proposal to quorum commit, per shard.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms .. ~16s
	}, []string{"class", "shard"})

	shardRaftCommitApply = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_commit_apply_seconds",
		Help:    "Time from quorum commit to FSM apply completion (queue wait plus materialization), per shard.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms .. ~16s
	}, []string{"class", "shard"})

	// shardRaftApplyWindowEntries is the apply lane's coalescing signal: how
	// many committed entries one FSM materialization unit covered — a merged
	// put window or a single command. Sustained samples near 1 under import
	// load mean the backlog drain is not merging (one LSM round per entry,
	// the pre-coalescing bottleneck).
	shardRaftApplyWindowEntries = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "weaviate_shard_raft_apply_window_entries",
		Help:    "Committed entries covered by one FSM materialization round (merged put window or single command), per shard.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 11), // 1 .. 1024
	}, []string{"class", "shard"})

	// shardRaftSnapshots counts completed snapshot+compaction rounds per
	// group, split by which cadence threshold fired (bytes|entries) — the
	// operator-side signal that log compaction is keeping up with import
	// volume (zero during a bulk import means the shared raft log is
	// accumulating and flush latency will age).
	shardRaftSnapshots = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_snapshots_total",
		Help: "Completed shard-raft snapshot+compaction rounds, by trigger (bytes|entries).",
	}, []string{"class", "shard", "trigger"})

	// shardRaftBirthCampaigns counts designated-campaigner elections fired at
	// group birth (see Store.maybeBirthCampaign). Compared against group
	// creations it shows how often birth placement actually took effect vs
	// fell back to the randomized election race.
	shardRaftBirthCampaigns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_birth_campaigns_total",
		Help: "Designated-campaigner immediate elections fired at group birth, per shard.",
	}, []string{"class", "shard"})

	// shardRaftApplyDispatchFailures counts post-commit FSM dispatch
	// failures. Apply acks at quorum commit, so these no longer reach
	// clients: a committed entry that fails dispatch is a node-local
	// invariant violation (committed entries must apply deterministically on
	// every replica) — logged at the apply worker and counted here as the
	// operator-visible signal.
	shardRaftApplyDispatchFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_apply_dispatch_failures_total",
		Help: "Post-commit FSM dispatch failures (node-local invariant violations, not client-visible).",
	}, []string{"class", "shard"})

	// shardRaftApplyParkedAge is the apply worker's park gauge: seconds since
	// materialization parked at a committed entry that keeps failing
	// environmentally (read-only flip, ENOSPC, damaged local state), 0 when
	// not parked. Parking is indefinite by design — the entry retries with
	// capped backoff until it lands, is superseded by a snapshot install, or
	// the group is torn down — so a growing age IS the operator signal: the
	// shard's data on this node is frozen at the parked entry while its peers
	// carry the shard.
	shardRaftApplyParkedAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "weaviate_shard_raft_apply_parked_age_seconds",
		Help: "Seconds the apply worker has been parked at a failing committed entry (0 = not parked).",
	}, []string{"class", "shard"})

	// shardRaftApplyParkRetries counts park retry attempts — the rate says
	// how actively a parked group is re-probing its failing entry.
	shardRaftApplyParkRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_apply_park_retries_total",
		Help: "Retry attempts of a parked committed entry on the apply worker.",
	}, []string{"class", "shard"})

	// shardRaftApplySkipped counts materialization units skipped
	// deterministically: items whose error is explicitly marked deterministic
	// at the shard boundary (validation, marshalling — identical on every
	// replica), undecodable commands, and whole entries abandoned by the
	// schema fence after their class was dropped post-admission. Skips never
	// lose acknowledged-and-valid writes; the reason label separates the
	// taxonomy bins.
	shardRaftApplySkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_shard_raft_apply_skipped_total",
		Help: "Apply items/entries skipped deterministically, by reason (deterministic|class_dropped).",
	}, []string{"class", "shard", "reason"})
)

// Reason labels for shardRaftApplySkipped.
const (
	skipReasonDeterministic = "deterministic"
	skipReasonClassDropped  = "class_dropped"
)

// Drop sites for shardRaftDropped. Every discarded frame is counted exactly
// once, at the most specific site that killed it.
const (
	dropSiteRouteUnknownGroup = "route_unknown_group"
	dropSiteStepNotLive       = "step_not_live"
	dropSiteStepQueueFull     = "step_queue_full"
	dropSiteStepLocalSpoof    = "step_local_spoof"
	dropSiteSendWriteError    = "send_write_error"
	// Sender-lane overflow: the destination peer is not draining its queue
	// fast enough and the bound was hit — raft's probe/retry machinery
	// re-sends after the loss.
	dropSiteSendBulkQueueFull = "send_bulk_queue_full"
	dropSiteSendPrioQueueFull = "send_prio_queue_full"
	// Frames still queued (or being handed off) when the transport shut down.
	dropSiteSendShutdown = "send_shutdown"
	// Frames still queued on a group's bulk stripes when the group was
	// removed from this node (shard drop/unload) — expected discards, never
	// message loss: the group is gone.
	dropSiteSendGroupRemoved = "send_group_removed"
	dropSitePeerResolve      = "peer_resolve"
	dropSitePeerDial         = "peer_dial"
	dropSitePeerOpenStream   = "peer_open_stream"
	dropSiteHeartbeatEncode  = "heartbeat_encode"
	dropSiteEncodeFrame      = "encode_frame"
)

// msgClass buckets raft message types by the pipeline they ride, which is the
// diagnostic dimension: "heartbeat" is the coalesced path, "append"/"vote"/
// "snap" the direct path, "response" the durability-gated acks delivered only
// after an fsync, and "storage_local" the local storage protocol that must
// never appear on the wire.
func msgClass(t raftpb.MessageType) string {
	switch t {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		return "heartbeat"
	case raftpb.MsgApp:
		return "append"
	case raftpb.MsgAppResp, raftpb.MsgVoteResp, raftpb.MsgPreVoteResp:
		return "response"
	case raftpb.MsgVote, raftpb.MsgPreVote:
		return "vote"
	case raftpb.MsgSnap, raftpb.MsgSnapStatus:
		return "snap"
	case raftpb.MsgStorageAppend, raftpb.MsgStorageAppendResp,
		raftpb.MsgStorageApply, raftpb.MsgStorageApplyResp:
		return "storage_local"
	default:
		return "other"
	}
}

// groupLabelCache memoizes the decimal rendering of group IDs so the
// per-message counter path does not allocate in steady state.
var groupLabelCache sync.Map // uint64 -> string

func groupLabel(groupID uint64) string {
	if v, ok := groupLabelCache.Load(groupID); ok {
		return v.(string)
	}
	s := strconv.FormatUint(groupID, 10)
	groupLabelCache.Store(groupID, s)
	return s
}

// countMessages records one counter increment per message at a pipeline point
// ("send" or "route").
func countMessages(point string, groupID uint64, msgs []raftpb.Message) {
	g := groupLabel(groupID)
	for i := range msgs {
		shardRaftMessages.WithLabelValues(point, msgClass(msgs[i].Type), g).Inc()
	}
}

// logLimiter rate-limits WARN logging per key so drop storms cannot flood the
// log while still guaranteeing at least one line per interval — the exact
// inverse of the silent drops that made past incidents undiagnosable.
type logLimiter struct {
	mu       sync.Mutex
	last     map[string]time.Time
	interval time.Duration
}

func newLogLimiter(interval time.Duration) *logLimiter {
	return &logLimiter{last: make(map[string]time.Time), interval: interval}
}

// Allow reports whether a log line for key is due, and records the emission
// time when it is.
func (l *logLimiter) Allow(key string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[key]; ok && now.Sub(t) < l.interval {
		return false
	}
	l.last[key] = now
	return true
}
