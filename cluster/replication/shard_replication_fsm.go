//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/maps"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type ShardReplicationOp struct {
	ID   uint64
	UUID strfmt.UUID

	// Targeting information of the replication operation
	SourceShard shardFQDN
	TargetShard shardFQDN

	TransferType api.ShardReplicationTransferType
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
	// opsByTargetFQDN stores the registered ShardReplicationOp (if any) for each destination replica
	opsByTargetFQDN map[shardFQDN]ShardReplicationOp
	// opsBySourceFQDN stores the registered ShardReplicationOp (if any) for each source replica
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
		opsByTargetFQDN:         make(map[shardFQDN]ShardReplicationOp),
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
		if err := s.writeOpIntoFSM(op, status); err != nil {
			return err
		}
	}

	return nil
}

// resetState reset the state of the FSM to empty. This is used when restoring a snapshot to ensure we restore a snapshot
// into a clean FSM
// The lock onto the underlying data is *not acquired* by this function the callee must ensure the lock is held
func (s *ShardReplicationFSM) resetState() {
	// Reset data
	maps.Clear(s.idsByUuid)
	maps.Clear(s.opsByTarget)
	maps.Clear(s.opsBySource)
	maps.Clear(s.opsByCollection)
	maps.Clear(s.opsByCollectionAndShard)
	maps.Clear(s.opsByTargetFQDN)
	maps.Clear(s.opsBySourceFQDN)
	maps.Clear(s.opsById)
	maps.Clear(s.statusById)

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

func (s *ShardReplicationFSM) GetOpsForTarget(node string) []ShardReplicationOp {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	return s.opsByTarget[node]
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

func (s *ShardReplicationFSM) FilterOneShardReplicasWrite(collection string, shard string, shardReplicasLocation []string) ([]string, []string) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	// Check if the specified shard is current undergoing replication at all.
	// If not we can return early as all replicas can be used for writes
	byCollection, ok := s.opsByCollectionAndShard[collection]
	if !ok {
		return shardReplicasLocation, []string{}
	}
	ops, ok := byCollection[shard]
	if !ok {
		return shardReplicasLocation, []string{}
	}

	_, writeReplicas := s.readWriteReplicas(collection, shard, shardReplicasLocation)

	additionalWriteReplicas := []string{}
	for _, op := range ops {
		opState, ok := s.statusById[op.ID]
		if !ok {
			continue
		}
		if opState.GetCurrentState() == api.FINALIZING {
			additionalWriteReplicas = append(additionalWriteReplicas, op.TargetShard.NodeId)
		}
	}
	return writeReplicas, additionalWriteReplicas
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

// filterOneReplicaAsTargetReadWrite returns whether the replica node for collection and shard is usable for read and write
// It returns a tuple of boolean (readOk, writeOk)
func (s *ShardReplicationFSM) filterOneReplicaReadWrite(node string, collection string, shard string) (bool, bool) {
	replicaFQDN := newShardFQDN(node, collection, shard)
	op, ok := s.opsByTargetFQDN[replicaFQDN]
	// No target replication ops for that replica, ensure we check if it's a source
	if !ok {
		return s.filterOneReplicaAsSourceReadWrite(node, collection, shard)
	}

	opState, ok := s.statusById[op.ID]
	if !ok {
		// TODO: This should never happens
		return true, true
	}

	// Filter read/write based on the state of the replica op
	readOk := false
	writeOk := false
	switch opState.GetCurrentState() {
	case api.READY:
		readOk = true
		writeOk = true
	case api.DEHYDRATING:
		readOk = true
		writeOk = true
	default:
	}
	return readOk, writeOk
}

// filterOneReplicaAsSourceReadWrite returns a tuple of boolean (found, readOk, writeOk)
// if found is true it means there's a source replication op for that replica and readOk and writeOk should be considered
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
