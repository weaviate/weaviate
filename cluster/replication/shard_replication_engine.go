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
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/replica/copier"
)

const (
	replicationEngineLogAction                = "replication_engine"
	replicationEngineMaxConcurrentReplication = 5
)

type shardReplicationEngine struct {
	node   string
	logger *logrus.Entry

	replicationFSM         *ShardReplicationFSM
	ongoingReplications    atomic.Int64
	ongoingReplicationLock sync.RWMutex
	ongoingReplicationOps  map[shardReplicationOp]struct{}
	opChan                 chan shardReplicationOp
	stopChan               chan bool
}

func newShardReplicationEngine(logger *logrus.Logger, replicationFSM *ShardReplicationFSM) *shardReplicationEngine {
	return &shardReplicationEngine{
		logger:                logger.WithFields(logrus.Fields{"action": replicationEngineLogAction}),
		ongoingReplicationOps: make(map[shardReplicationOp]struct{}),
		opChan:                make(chan shardReplicationOp, 100),
		stopChan:              make(chan bool),
	}
}

func (s *shardReplicationEngine) Start() {
	go s.replicationFSMMonitor()
	s.logger.Info("replication engine started")
}

func (s *shardReplicationEngine) Stop() {
	s.stopChan <- true
}

func (s *shardReplicationEngine) replicationFSMMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	for {
	LOOP_RESET:
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			ongoingShardReplicationOps, newShardReplicationOps := s.getShardReplicationOps()

			// First handle ongoing shard replication ops and check if need to recover from failure any of them
			// The reason we do these first is that we want to prioritise unfinished shard replication operation vs new shard replication
			// operation
			for _, op := range ongoingShardReplicationOps {
				// If we reach the maximum, break and backoff. We want to avoid overloading the engine with too many concurrent operation
				if s.ongoingReplications.Load() > replicationEngineMaxConcurrentReplication {
					goto LOOP_RESET
				}

				if _, ok := s.ongoingReplicationOps[op]; !ok {
					s.recoverShardReplication(op)
				}
			}

			for _, op := range newShardReplicationOps {
				// If we reach the maximum, break and backoff. We want to avoid overloading the engine with too many concurrent operation
				if s.ongoingReplications.Load() > replicationEngineMaxConcurrentReplication {
					goto LOOP_RESET
				}

				s.startShardReplication(op)
			}
		}
	}
}

func (s *shardReplicationEngine) registerStartShardReplication(op shardReplicationOp) {
	s.ongoingReplicationLock.Lock()
	defer s.ongoingReplicationLock.Unlock()

	s.ongoingReplicationOps[op] = struct{}{}
}

func (s *shardReplicationEngine) startShardReplication(op shardReplicationOp) {
	s.ongoingReplications.Add(1)
	s.registerStartShardReplication(op)

	go func() {
		defer s.ongoingReplications.Add(-1)
		// TODO defer deleting the op from ongoing ops map and fsm maps as well? but only if it doesn't work? this is the "in memory" tracking of the replica movement?
		if s.node == op.targetShard.nodeId {
			// TODO pass in RemoteIndex to copier here
			copyController := copier.New()
			copyController.Run(op.sourceShard.nodeId, op.targetShard.nodeId, op.sourceShard.collectionId, op.targetShard.shardId)
		}
	}()
}

func (s *shardReplicationEngine) recoverShardReplication(op shardReplicationOp) {
	s.ongoingReplications.Add(1)
	s.registerStartShardReplication(op)

	go func() {
		defer s.ongoingReplications.Add(-1)

	}()
}

func (s *shardReplicationEngine) getShardReplicationOps() ([]shardReplicationOp, []shardReplicationOp) {
	var newShardReplicationOp []shardReplicationOp
	var ongoingShardReplicationOp []shardReplicationOp
	for _, op := range s.replicationFSM.GetOpsForNode(s.node) {
		switch s.replicationFSM.GetOpState(op).state {
		case api.REGISTERED:
			newShardReplicationOp = append(newShardReplicationOp, op)
		case api.HYDRATING:
			s.ongoingReplicationLock.RLock()
			_, ok := s.ongoingReplicationOps[op]
			s.ongoingReplicationLock.RUnlock()
			if !ok {
				ongoingShardReplicationOp = append(ongoingShardReplicationOp, op)
			}
		default:
			continue
		}
	}
	return ongoingShardReplicationOp, newShardReplicationOp
}
