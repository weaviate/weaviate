package replication

import (
	"fmt"
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
	opsLock *sync.RWMutex

	// opsByNode stores the array of shardReplicationOp for each "source" node
	opsByNode map[string][]shardReplicationOp
	// opsByCollection stores the array of shardReplicationOp for each collection
	opsByCollection map[string][]shardReplicationOp
	// opsByShard stores the array of shardReplicationOp for each shard
	opsByShard map[string][]shardReplicationOp
	// opsByShard stores the registered shardReplicationOp (if any) for each destination replica
	opsByTargetFQDN map[shardFQDN]shardReplicationOp
	// opsByShard stores opId -> replicationOp
	opsById map[uint64]shardReplicationOp
	// opsStatus stores op -> opStatus
	opsStatus map[shardReplicationOp]shardReplicationOpStatus
}

func newShardReplicationFSM() *ShardReplicationFSM {
	return &ShardReplicationFSM{
		opsStatus:       make(map[shardReplicationOp]shardReplicationOpStatus),
		opsByNode:       make(map[string][]shardReplicationOp),
		opsByCollection: make(map[string][]shardReplicationOp),
		opsByShard:      make(map[string][]shardReplicationOp),
		opsByTargetFQDN: make(map[shardFQDN]shardReplicationOp),
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

// TODO: see if necessary to keep this function, or we always lookup replicas for a single shard
func (s *ShardReplicationFSM) FilterAllShardReplicasReadWrite(collection string, shards []string, shardsReplicasLocation map[string][]string) (map[string][]string, map[string][]string, error) {
	s.opsLock.RLock()
	defer s.opsLock.RUnlock()

	readShardsReplicas := make(map[string][]string, len(shards))
	writeShardsReplicas := make(map[string][]string, len(shards))
	for _, shard := range shards {
		shardReplicasLocation, ok := shardsReplicasLocation[shard]
		if !ok {
			return readShardsReplicas, writeShardsReplicas, fmt.Errorf("could not find replicas for shard %s", shard)
		}
		readReplicas, writeReplicas := s.FilterOneShardReplicasReadWrite(collection, shard, shardReplicasLocation)
		readShardsReplicas[shard] = readReplicas
		writeShardsReplicas[shard] = writeReplicas
	}
	return readShardsReplicas, writeShardsReplicas, nil
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
	// We want to filter out which replicas can't be used for read and writes.
	// A replica has to be filtered out for read if
	// 1. It is in any other state than LIVE
	// A replica has to be filtered out for write if
	// 1. It is in REGISTERED state (it might not be ready to receive a write)
	// 2. It is in DEHYDRATING state (we are decomissioning that shard and it will be deleted)
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
	case api.HYDRATING:
		writeOk = true
	case api.READY:
		writeOk = true
	case api.LIVE:
		readOk = true
		writeOk = true
	default:
	}
	return readOk, writeOk
}
