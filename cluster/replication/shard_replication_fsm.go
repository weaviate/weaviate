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
	"sync"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type shardReplicationOpStatus struct {
	// state is the current state of the shard replication operation
	state api.ShardReplicationState
}

type shardReplicationOp struct {
	id uint64

	// Targeting information of the replication operation
	sourceShard shardFQDN
	targetShard shardFQDN
}

type ShardReplicationFSM struct {
	opsLock sync.RWMutex

	// opsByNode stores the array of shardReplicationOp for each "target" node
	opsByNode map[string][]shardReplicationOp
	// opsByCollection stores the array of shardReplicationOp for each collection
	opsByCollection map[string][]shardReplicationOp
	// opsByShard stores the array of shardReplicationOp for each shard
	opsByShard map[string][]shardReplicationOp
	// opsByTargetFQDN stores the registered shardReplicationOp (if any) for each destination replica
	opsByTargetFQDN map[shardFQDN]shardReplicationOp
	// opsByShard stores opId -> replicationOp
	opsById map[uint64]shardReplicationOp
	// opsStatus stores op -> opStatus
	opsStatus map[shardReplicationOp]shardReplicationOpStatus
}

func newShardReplicationFSM() *ShardReplicationFSM {
	return &ShardReplicationFSM{
		opsByNode:       make(map[string][]shardReplicationOp),
		opsByCollection: make(map[string][]shardReplicationOp),
		opsByShard:      make(map[string][]shardReplicationOp),
		opsByTargetFQDN: make(map[shardFQDN]shardReplicationOp),
		opsById:         make(map[uint64]shardReplicationOp),
		opsStatus:       make(map[shardReplicationOp]shardReplicationOpStatus),
	}
}

func (s *ShardReplicationFSM) GetOpsForNode(node string) []shardReplicationOp {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()
	return s.opsByNode[node]
}

func (s *ShardReplicationFSM) GetOpState(op shardReplicationOp) shardReplicationOpStatus {
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
