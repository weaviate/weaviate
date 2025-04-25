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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/maps"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type shardReplicationOpStatus struct {
	// state is the current state of the shard replication operation
	state api.ShardReplicationState
}

type ShardReplicationOp struct {
	ID uint64

	// Targeting information of the replication operation
	SourceShard shardFQDN
	TargetShard shardFQDN
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

func NewShardReplicationOp(id uint64, sourceNode, targetNode, collectionId, shardId string) ShardReplicationOp {
	return ShardReplicationOp{
		ID:          id,
		SourceShard: newShardFQDN(sourceNode, collectionId, shardId),
		TargetShard: newShardFQDN(targetNode, collectionId, shardId),
	}
}

type ShardReplicationFSM struct {
	opsLock sync.RWMutex

	// opsByNode stores the array of ShardReplicationOp for each "target" node
	opsByNode map[string][]ShardReplicationOp
	// opsByCollection stores the array of ShardReplicationOp for each collection
	opsByCollection map[string][]ShardReplicationOp
	// opsByShard stores the array of ShardReplicationOp for each shard
	opsByShard map[string][]ShardReplicationOp
	// opsByTargetFQDN stores the registered ShardReplicationOp (if any) for each destination replica
	opsByTargetFQDN map[shardFQDN]ShardReplicationOp
	// opsByShard stores opId -> replicationOp
	opsById map[uint64]ShardReplicationOp
	// opsStatus stores op -> opStatus
	opsStatus map[ShardReplicationOp]shardReplicationOpStatus

	opsByStateGauge *prometheus.GaugeVec
}

func newShardReplicationFSM(reg prometheus.Registerer) *ShardReplicationFSM {
	fsm := &ShardReplicationFSM{
		opsByNode:       make(map[string][]ShardReplicationOp),
		opsByCollection: make(map[string][]ShardReplicationOp),
		opsByShard:      make(map[string][]ShardReplicationOp),
		opsByTargetFQDN: make(map[shardFQDN]ShardReplicationOp),
		opsById:         make(map[uint64]ShardReplicationOp),
		opsStatus:       make(map[ShardReplicationOp]shardReplicationOpStatus),
	}

	fsm.opsByStateGauge = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      "replication_operation_fsm_ops_by_state",
		Help:      "Current number of replication operations in each state of the FSM lifecycle",
	}, []string{"state"})

	return fsm
}

type snapshot struct {
	Ops map[ShardReplicationOp]shardReplicationOpStatus
}

func (s *ShardReplicationFSM) Snapshot() ([]byte, error) {
	s.opsLock.RLock()
	ops := maps.Clone(s.opsStatus)
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
	maps.Clear(s.opsByNode)
	maps.Clear(s.opsByCollection)
	maps.Clear(s.opsByShard)
	maps.Clear(s.opsByTargetFQDN)
	maps.Clear(s.opsById)
	maps.Clear(s.opsStatus)

	s.opsByStateGauge.Reset()
}

func (s *ShardReplicationFSM) GetOpsForNode(node string) []ShardReplicationOp {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	return s.opsByNode[node]
}

func (s shardReplicationOpStatus) ShouldRestartOp() bool {
	return s.state == api.REGISTERED || s.state == api.HYDRATING
}

func (s *ShardReplicationFSM) GetOpState(op ShardReplicationOp) shardReplicationOpStatus {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	return s.opsStatus[op]
}

func (s *ShardReplicationFSM) FilterOneShardReplicasReadWrite(collection string, shard string, shardReplicasLocation []string) ([]string, []string) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	_, ok := s.opsByShard[shard]
	// Check if the specified shard is current undergoing replication at all.
	// If not we can return early as all replicas can be used for read/writes
	if !ok {
		return shardReplicasLocation, shardReplicasLocation
	}

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

func (s *ShardReplicationFSM) filterOneReplicaReadWrite(node string, collection string, shard string) (bool, bool) {
	targetFQDN := newShardFQDN(node, collection, shard)
	op, ok := s.opsByTargetFQDN[targetFQDN]
	// There's no replication ops for that replicas, it can be used for both read and writes
	if !ok {
		return true, true
	}

	opState, ok := s.opsStatus[op]
	if !ok {
		// TODO: This should never happens
		return true, true
	}

	// Filter read/write based on the state of the replica
	readOk := false
	writeOk := false
	switch opState.state {
	case api.FINALIZING:
		writeOk = true
	case api.READY:
		readOk = true
		writeOk = true
	default:
	}
	return readOk, writeOk
}

// IsOpCompletedOrInProgress returns true if the given replication operation has started or completed execution.
func (s *ShardReplicationFSM) IsOpCompletedOrInProgress(op ShardReplicationOp) bool {
	return api.REGISTERED != s.GetOpState(op).state
}
