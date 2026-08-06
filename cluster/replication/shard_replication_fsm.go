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

package replication

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type ShardReplicationOp struct {
	ID   uint64
	UUID strfmt.UUID

	// Targeting information of the replication operation
	SourceShard shardFQDN
	TargetShard shardFQDN

	TransferType    api.ShardReplicationTransferType
	StartTimeUnixMs int64 // Unix timestamp when the operation started
}

func (s ShardReplicationOp) MarshalText() (text []byte, err error) {
	// We have to implement MarshalText to be able to use this struct as a key for a map
	// We have to trick go to avoid an infinite recursion here as we still want to use the default json marshal/unmarshal
	// code
	type shardReplicationOpCopy ShardReplicationOp
	return json.Marshal(shardReplicationOpCopy(s))
}

func (s *ShardReplicationOp) UnmarshalText(text []byte) error {
	type shardReplicationOpCopy ShardReplicationOp
	return json.Unmarshal(text, (*shardReplicationOpCopy)(s))
}

func NewShardReplicationOp(id uint64, sourceNode, targetNode, collectionId, shardId string, transferType api.ShardReplicationTransferType) ShardReplicationOp {
	return ShardReplicationOp{
		ID:           id,
		SourceShard:  newShardFQDN(sourceNode, collectionId, shardId),
		TargetShard:  newShardFQDN(targetNode, collectionId, shardId),
		TransferType: transferType,
	}
}

type ShardReplicationFSM struct {
	opsLock sync.RWMutex

	// idsByUuiid stores user-facing UUID -> repo-facing raft log index
	idsByUuid map[strfmt.UUID]uint64
	// opsByTarget stores the array of ShardReplicationOp for each "target" node
	opsByTarget map[string][]ShardReplicationOp
	// opsBySource stores the array of ShardReplicationOp for each "source" node
	opsBySource map[string][]ShardReplicationOp
	// opsByCollection stores the array of ShardReplicationOp for each collection
	opsByCollection map[string][]ShardReplicationOp
	// opsByCollectionAndShard stores the array of ShardReplicationOp for each collection and shard
	opsByCollectionAndShard map[string]map[string][]ShardReplicationOp
	// opsByTargetFQDN stores the registered ShardReplicationOps for each destination replica.
	opsByTargetFQDN map[shardFQDN][]ShardReplicationOp
	// opsBySourceFQDN stores the registered ShardReplicationOps for each source replica
	opsBySourceFQDN map[shardFQDN][]ShardReplicationOp
	// opsById stores opId -> replicationOp
	opsById map[uint64]ShardReplicationOp
	// opsStatus stores op -> opStatus
	statusById map[uint64]ShardReplicationOpStatus

	opsByStateGauge *prometheus.GaugeVec
}

func NewShardReplicationFSM(reg prometheus.Registerer) *ShardReplicationFSM {
	fsm := &ShardReplicationFSM{
		idsByUuid:               make(map[strfmt.UUID]uint64),
		opsByTarget:             make(map[string][]ShardReplicationOp),
		opsBySource:             make(map[string][]ShardReplicationOp),
		opsByCollection:         make(map[string][]ShardReplicationOp),
		opsByCollectionAndShard: make(map[string]map[string][]ShardReplicationOp),
		opsByTargetFQDN:         make(map[shardFQDN][]ShardReplicationOp),
		opsBySourceFQDN:         make(map[shardFQDN][]ShardReplicationOp),
		opsById:                 make(map[uint64]ShardReplicationOp),
		statusById:              make(map[uint64]ShardReplicationOpStatus),
	}

	fsm.opsByStateGauge = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_operation_fsm_ops_by_state",
		Help:      "Current number of replication operations in each state of the FSM lifecycle",
	}, []string{"state"})

	return fsm
}

type snapshot struct {
	Ops map[ShardReplicationOp]ShardReplicationOpStatus
}

func (s *ShardReplicationFSM) Snapshot() ([]byte, error) {
	s.opsLock.RLock()
	ops := make(map[ShardReplicationOp]ShardReplicationOpStatus, len(s.statusById))
	for id, status := range s.statusById {
		op, ok := s.opsById[id]
		if !ok {
			s.opsLock.RUnlock()
			return nil, fmt.Errorf("op %d not found in opsById", op.ID)
		}
		ops[op] = status
	}
	s.opsLock.RUnlock()

	return json.Marshal(&snapshot{Ops: ops})
}

func (s *ShardReplicationFSM) Restore(bytes []byte) error {
	var snap snapshot
	if err := json.Unmarshal(bytes, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	s.opsLock.Lock()
	defer s.opsLock.Unlock()

	s.resetState()

	for op, status := range snap.Ops {
		s.insertOpIntoFSM(op, status)
	}

	return nil
}

// resetState reset the state of the FSM to empty. This is used when restoring a snapshot to ensure we restore a snapshot
// into a clean FSM
// The lock onto the underlying data is *not acquired* by this function the callee must ensure the lock is held
func (s *ShardReplicationFSM) resetState() {
	// Reset data
	clear(s.idsByUuid)
	clear(s.opsByTarget)
	clear(s.opsBySource)
	clear(s.opsByCollection)
	clear(s.opsByCollectionAndShard)
	clear(s.opsByTargetFQDN)
	clear(s.opsBySourceFQDN)
	clear(s.opsById)
	clear(s.statusById)

	s.opsByStateGauge.Reset()
}

func (s *ShardReplicationFSM) GetOpByUuid(uuid strfmt.UUID) (ShardReplicationOpAndStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	id, ok := s.idsByUuid[uuid]
	if !ok {
		return ShardReplicationOpAndStatus{}, false
	}
	op, ok := s.opsById[id]
	if !ok {
		return ShardReplicationOpAndStatus{}, false
	}
	status, ok := s.statusById[id]
	if !ok {
		return ShardReplicationOpAndStatus{}, false
	}
	return NewShardReplicationOpAndStatus(op, status), true
}

func (s *ShardReplicationFSM) GetOpById(id uint64) (ShardReplicationOpAndStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	op, ok := s.opsById[id]
	if !ok {
		return ShardReplicationOpAndStatus{}, false
	}
	status, ok := s.statusById[id]
	if !ok {
		return ShardReplicationOpAndStatus{}, false
	}
	return NewShardReplicationOpAndStatus(op, status), true
}

// GetOpsForTarget returns a copy. Callers iterate the result after the lock is
// dropped, while removeReplicationOps compacts the bucket's backing array in
// place, so returning the bucket itself would be an unsynchronized read.
func (s *ShardReplicationFSM) GetOpsForTarget(node string) []ShardReplicationOp {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	return slices.Clone(s.opsByTarget[node])
}

func (s *ShardReplicationFSM) GetOpsForCollection(collection string) ([]ShardReplicationOpAndStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	ops, ok := s.opsByCollection[collection]
	if !ok {
		return nil, false
	}
	return s.getOpsWithStatus(ops), true
}

func (s *ShardReplicationFSM) GetOpsForCollectionAndShard(collection string, shard string) ([]ShardReplicationOpAndStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	shardOps, ok := s.opsByCollectionAndShard[collection]
	if !ok {
		return nil, false
	}
	ops, ok := shardOps[shard]
	if !ok {
		return nil, false
	}
	return s.getOpsWithStatus(ops), true
}

// HasActiveReplicationForShard reports whether a non-terminal replication op exists for
// collection/shard. The result is independent of which node hosts the source or target
// replica — both share the collection/shard key — and reads only RAFT-replicated state, so
// every node in the cluster returns the same answer.
func (s *ShardReplicationFSM) HasActiveReplicationForShard(collection, shard string) bool {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	for _, op := range s.opsByCollectionAndShard[collection][shard] {
		if status, ok := s.statusById[op.ID]; ok && status.ShouldConsumeOps() {
			return true
		}
	}
	return false
}

// HasActiveReplicationForCollection is HasActiveReplicationForShard across every shard of the
// collection, for gating class-wide schema mutations.
func (s *ShardReplicationFSM) HasActiveReplicationForCollection(collection string) bool {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	for _, op := range s.opsByCollection[collection] {
		if status, ok := s.statusById[op.ID]; ok && status.ShouldConsumeOps() {
			return true
		}
	}
	return false
}

// HasActiveTargetReplicationForShard is an eventually-consistent hint read from this
// node's local FSM (bounded by RAFT apply lag), not a synchronization barrier. It is
// polled on every hashbeat cycle of every shard and must stay allocation-free.
func (s *ShardReplicationFSM) HasActiveTargetReplicationForShard(collection, shard, targetNode string) bool {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	// The source-keyed bucket holds exactly the ops targeting (collection, shard):
	// both op constructors build TargetShard from the source collection/shard.
	for _, op := range s.opsByCollectionAndShard[collection][shard] {
		if op.TargetShard.NodeId != targetNode {
			continue
		}
		status, ok := s.statusById[op.ID]
		if !ok {
			continue
		}
		switch status.GetCurrentState() {
		case api.READY, api.CANCELLED:
			// terminal — does not block async-repl gating
		default:
			return true
		}
	}
	return false
}

func (s *ShardReplicationFSM) getOpsWithStatus(ops []ShardReplicationOp) []ShardReplicationOpAndStatus {
	opsWithStatus := make([]ShardReplicationOpAndStatus, 0, len(ops))
	for _, op := range ops {
		status, ok := s.statusById[op.ID]
		if !ok {
			continue
		}
		opsWithStatus = append(opsWithStatus, NewShardReplicationOpAndStatus(op, status))
	}
	return opsWithStatus
}

func (s *ShardReplicationFSM) GetOpsForTargetNode(node string) ([]ShardReplicationOpAndStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	ops, ok := s.opsByTarget[node]
	return s.getOpsWithStatus(ops), ok
}

// StaleOp is one sweep candidate. The state travels with the id so the cleanup
// metric's per-state label stays exact when only some batches of a tick apply.
type StaleOp struct {
	ID    uint64
	State api.ShardReplicationState
}

// SelectStaleOps returns at most limit eligible ops, ascending by id, plus the
// number of ops that matched state and age but carried a flag. That count is the
// whole such population, not just the part within limit: it is the diagnostic
// for a READY gauge that plateaus above zero.
//
// An op is eligible when all of:
//
//  1. its current state is READY, or CANCELLED when includeCancelled is true;
//  2. it carries neither ShouldCancel nor ShouldDelete. Those ops are owned by an
//     in-flight deletion and are the only terminal ops whose removal moves a
//     gate predicate, because ShouldConsumeOps() is true only for them;
//  3. its current-state start time is before cutoffUnixMs, or is zero or
//     negative. Ops predating the field carry no timestamp and are infinitely old.
//
// Candidates are collected in full, sorted, and only then truncated: truncating
// a randomly-ordered map iteration would starve an ancient op behind a churning
// backlog. The sort is by id because ids are RAFT log indices, identical on every
// node, whereas StartTimeUnixMs is stamped locally at apply time and would
// reshuffle the priority order on every leadership change.
func (s *ShardReplicationFSM) SelectStaleOps(cutoffUnixMs int64, includeCancelled bool, limit int) (ops []StaleOp, flaggedSkipped int) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	candidates := make([]uint64, 0, len(s.statusById))
	for id, status := range s.statusById {
		switch status.GetCurrentState() {
		case api.READY:
		case api.CANCELLED:
			if !includeCancelled {
				continue
			}
		default:
			continue
		}

		st := status.Current.StartTimeUnixMs
		oldEnough := st <= 0 || st < cutoffUnixMs
		if !oldEnough {
			continue
		}

		if status.ShouldCancel || status.ShouldDelete {
			flaggedSkipped++
			continue
		}
		candidates = append(candidates, id)
	}

	slices.Sort(candidates)
	if limit >= 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	ops = make([]StaleOp, 0, len(candidates))
	for _, id := range candidates {
		status := s.statusById[id]
		ops = append(ops, StaleOp{ID: id, State: status.GetCurrentState()})
	}
	return ops, flaggedSkipped
}

func (s *ShardReplicationFSM) GetStatusByOps() map[ShardReplicationOp]ShardReplicationOpStatus {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	opsStatus := make(map[ShardReplicationOp]ShardReplicationOpStatus, len(s.statusById))
	for id, status := range s.statusById {
		op, ok := s.opsById[id]
		if !ok {
			continue
		}
		opsStatus[op] = status
	}
	return opsStatus
}

// ShouldConsumeOps returns true if the operation should be consumed by the consumer
//
// It checks the following two conditions:
//
// 1. The operation is neither cancelled nor ready, meaning that it is still in progress performing some long-running op like hydrating/finalizing
//
// 2. The operation is cancelled or ready and should be deleted, meaning that the operation is finished and should be removed from the FSM
func (s ShardReplicationOpStatus) ShouldConsumeOps() bool {
	state := s.GetCurrentState()
	return (
	// Check if op is not in cancelled or ready state -> we schedule it
	(state != api.CANCELLED && state != api.READY) ||
		// If op is in cancelled or ready state, only schedule it if it should be deleted
		(state == api.CANCELLED || state == api.READY) && s.ShouldDelete)
}

func (s *ShardReplicationFSM) GetOpState(op ShardReplicationOp) (ShardReplicationOpStatus, bool) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	v, ok := s.statusById[op.ID]
	return v, ok
}

func (s *ShardReplicationFSM) FilterOneShardReplicasRead(collection string, shard string, shardReplicasLocation []string) []string {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	// Check if the specified shard is current undergoing replication at all.
	// If not we can return early as all replicas can be used for reads
	byCollection, ok := s.opsByCollectionAndShard[collection]
	if !ok {
		return shardReplicasLocation
	}
	_, ok = byCollection[shard]
	if !ok {
		return shardReplicasLocation
	}
	readReplicas, _ := s.readWriteReplicas(collection, shard, shardReplicasLocation)
	return readReplicas
}

func (s *ShardReplicationFSM) FilterOneShardReplicasWrite(collection string, shard string, shardReplicasLocation []string) []string {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	// Check if the specified shard is current undergoing replication at all.
	// If not we can return early as all replicas can be used for writes
	byCollection, ok := s.opsByCollectionAndShard[collection]
	if !ok {
		return shardReplicasLocation
	}
	if _, ok := byCollection[shard]; !ok {
		return shardReplicasLocation
	}

	_, writeReplicas := s.readWriteReplicas(collection, shard, shardReplicasLocation)
	return writeReplicas
}

func (s *ShardReplicationFSM) readWriteReplicas(collection, shard string, shardReplicasLocation []string) ([]string, []string) {
	readReplicas := make([]string, 0, len(shardReplicasLocation))
	writeReplicas := make([]string, 0, len(shardReplicasLocation))
	for _, shardReplicaLocation := range shardReplicasLocation {
		readOk, writeOk := s.filterOneReplicaReadWrite(shardReplicaLocation, collection, shard)
		if readOk {
			readReplicas = append(readReplicas, shardReplicaLocation)
		}
		if writeOk {
			writeReplicas = append(writeReplicas, shardReplicaLocation)
		}
	}
	return readReplicas, writeReplicas
}

// filterOneReplicaReadWrite returns whether the replica node for collection and
// shard is usable for read and write, as a (readOk, writeOk) tuple.
func (s *ShardReplicationFSM) filterOneReplicaReadWrite(node string, collection string, shard string) (bool, bool) {
	replicaFQDN := newShardFQDN(node, collection, shard)
	ops, ok := s.opsByTargetFQDN[replicaFQDN]
	// No target replication ops for that replica, ensure we check if it's a source
	if !ok {
		return s.filterOneReplicaAsSourceReadWrite(node, collection, shard)
	}

	targetOk, sawLive := false, false
	for _, op := range ops {
		opState, ok := s.statusById[op.ID]
		if !ok {
			// A missing status should never happen (every indexed op has one).
			// Bail conservatively as read+write allowed.
			return true, true
		}
		switch opState.GetCurrentState() {
		case api.READY, api.DEHYDRATING, api.INTEGRATING:
			// Target is a counted r/w replica while the CCL is still draining.
			targetOk = true
		case api.CANCELLED:
			// Terminal and inert: admission skips cancelled ops
			// (checkNoConflictingOp) and routing must too, or a lingering
			// cancelled record de-routes a healthy replica once the sweep
			// deletes its READY sibling.
		default:
			sawLive = true
		}
	}
	if !targetOk && !sawLive {
		// Only cancelled target ops: same as no target entry at all.
		return s.filterOneReplicaAsSourceReadWrite(node, collection, shard)
	}
	if !targetOk {
		return false, false
	}
	// A routable target record must not mask the node's own source state: if a
	// later MOVE off this node is DEHYDRATING, a consistency=ONE write routed
	// here is dropped with the shard. AND the source side in, which also makes
	// deleting a READY op routing-neutral.
	return s.filterOneReplicaAsSourceReadWrite(node, collection, shard)
}

// filterOneReplicaAsSourceReadWrite returns whether the replica node is usable
// for read and write given its source-side ops, as a (readOk, writeOk) tuple.
func (s *ShardReplicationFSM) filterOneReplicaAsSourceReadWrite(node string, collection string, shard string) (bool, bool) {
	replicaFQDN := newShardFQDN(node, collection, shard)
	ops, ok := s.opsBySourceFQDN[replicaFQDN]
	// No source replication ops for that replica it can be used for both read and writes
	if !ok {
		return true, true
	}

	readOk := true
	writeOk := true
	for _, op := range ops {
		opState, ok := s.statusById[op.ID]
		if !ok {
			// This should never happen
			continue
		}
		switch opState.GetCurrentState() {
		case api.DEHYDRATING:
			readOk = false
			writeOk = false
		default:
		}
	}
	return readOk, writeOk
}

// AllPeersAtLeast reports whether every peer has PerNodeState[peer] >= target.
// Missing peers count as not satisfied.
func (s *ShardReplicationFSM) AllPeersAtLeast(opID uint64, target api.ShardReplicationState, peers []string) bool {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	st, ok := s.statusById[opID]
	if !ok {
		return false
	}
	floor := api.StateRank(target)
	for _, peer := range peers {
		state, ok := st.PerNodeState[peer]
		if !ok {
			return false
		}
		if api.StateRank(state) < floor {
			return false
		}
	}
	return true
}

// NonTerminalOpStates returns the current state of every op that has not reached
// a terminal state (READY/CANCELLED), keyed by op id. It is used after a
// snapshot restore to re-announce this node's reached state for in-progress ops;
// see Manager.Restore.
func (s *ShardReplicationFSM) NonTerminalOpStates() map[uint64]api.ShardReplicationState {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	out := make(map[uint64]api.ShardReplicationState, len(s.statusById))
	for id, status := range s.statusById {
		switch status.GetCurrentState() {
		case api.READY, api.CANCELLED:
			continue
		default:
			out[id] = status.GetCurrentState()
		}
	}
	return out
}
