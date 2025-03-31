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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	replicationEngineLogAction                = "replication_engine"
	replicationEngineMaxConcurrentReplication = 5
)

type ShardReplicationEngine struct {
	node   string
	logger *logrus.Entry

	replicationFSM         *ShardReplicationFSM
	leaderClient           types.ReplicationFSMUpdater
	ongoingReplications    atomic.Int64
	ongoingReplicationLock sync.RWMutex
	ongoingReplicationOps  map[shardReplicationOp]struct{}
	opChan                 chan shardReplicationOp
	stopChan               chan bool
	// replicaCopier does the "data tranfer" work
	replicaCopier types.ReplicaCopier
}

func NewShardReplicationEngine(logger *logrus.Logger, replicationFSM *ShardReplicationFSM, leaderClient types.ReplicationFSMUpdater, replicaCopier types.ReplicaCopier) *ShardReplicationEngine {
	return &ShardReplicationEngine{
		logger:                logger.WithFields(logrus.Fields{"action": replicationEngineLogAction}),
		replicationFSM:        replicationFSM,
		leaderClient:          leaderClient,
		ongoingReplicationOps: make(map[shardReplicationOp]struct{}),
		opChan:                make(chan shardReplicationOp, 100),
		stopChan:              make(chan bool),
		replicaCopier:         replicaCopier,
	}
}

func (s *ShardReplicationEngine) Start() {
	eg := enterrors.NewErrorGroupWrapper(s.logger)
	eg.Go(func() error {
		s.replicationFSMMonitor()
		return nil
	})
	err := eg.Wait()
	if err != nil {
		s.logger.WithError(err).Errorf("failed to start replication engine")
	}
}

func (s *ShardReplicationEngine) Stop() {
	s.logger.Info("Stopping replication engine")
	s.stopChan <- true
}

func (s *ShardReplicationEngine) replicationFSMMonitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
	LOOP_RESET:
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:

			ongoingShardReplicationOps, newShardReplicationOps := s.getShardReplicationOps()
			if len(ongoingShardReplicationOps) == 0 && len(newShardReplicationOps) == 0 {
				s.logger.Info("No shard replication op found")
				continue
			}

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

func (s *ShardReplicationEngine) registerStartShardReplication(op shardReplicationOp) {
	s.ongoingReplicationLock.Lock()
	defer s.ongoingReplicationLock.Unlock()

	s.ongoingReplicationOps[op] = struct{}{}
}

func (s *ShardReplicationEngine) startShardReplication(op shardReplicationOp) {
	s.ongoingReplications.Add(1)
	s.registerStartShardReplication(op)

	// TODO: Handle shutdown/abort related routine to stop all ongoing replica movement
	eg := enterrors.NewErrorGroupWrapper(s.logger)
	eg.Go(func() error {
		return backoff.Retry(func() error {
			defer s.ongoingReplications.Add(-1)
			// TODO how to cancel this context if we need to stop (eg hook it up to stopChan?)
			ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer cancel()

			logger := s.logger.WithFields(logrus.Fields{
				"opId":   op.id,
				"source": op.sourceShard.String(),
				"target": op.targetShard.String(),
			})

			// Update FSM that we are starting to hydrate this replica
			if err := s.leaderClient.ReplicationUpdateReplicaOpStatus(op.id, api.HYDRATING); err != nil {
				logger.WithError(err).Errorf("failed to update replica op state to %s", api.HYDRATING)
			}

			logger.Info("starting replica replication file copy")

			// Copy the replica
			if err := s.replicaCopier.CopyReplica(ctx, op.sourceShard.nodeId, op.sourceShard.collectionId, op.targetShard.shardId); err != nil {
				logger.WithError(err).Warn("failed to file copy replica")
				// TODO: Handle failure and failure tracking in replicationFSM of replica ops
				return err
			}

			// Update FSM that we are done copying files and we can start final sync follower phase
			if err := s.leaderClient.ReplicationUpdateReplicaOpStatus(op.id, api.FINALIZING); err != nil {
				logger.WithError(err).Errorf("failed to update replica op state to %s", api.FINALIZING)
			}

			logger.Info("replica replication file copy completed")

			// TODO Handle finalizing step for replica movement
			return nil
		}, backoff.NewConstantBackOff(5*time.Second))
	})
	err := eg.Wait()
	if err != nil {
		s.logger.WithError(err).Errorf("failed to start shard replication")
		// TODO any other handling that needs to be done here?
	}
}

func (s *ShardReplicationEngine) recoverShardReplication(op shardReplicationOp) {
	s.ongoingReplications.Add(1)
	s.registerStartShardReplication(op)

	eg := enterrors.NewErrorGroupWrapper(s.logger)
	eg.Go(func() error {
		defer s.ongoingReplications.Add(-1)
		return nil
	})
	err := eg.Wait()
	if err != nil {
		s.logger.WithError(err).Errorf("failed to recover shard replication")
	}
}

func (s *ShardReplicationEngine) getShardReplicationOps() ([]shardReplicationOp, []shardReplicationOp) {
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
