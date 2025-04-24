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
	leaderClient           types.FSMUpdater
	ongoingReplications    atomic.Int64
	ongoingReplicationLock sync.RWMutex
	ongoingReplicationOps  map[shardReplicationOp]struct{}
	opChan                 chan shardReplicationOp
	stopChan               chan bool
	// replicaCopier does the "data tranfer" work
	replicaCopier types.ReplicaCopier
}

func NewShardReplicationEngine(logger *logrus.Logger, node string, replicationFSM *ShardReplicationFSM, leaderClient types.FSMUpdater, replicaCopier types.ReplicaCopier) *ShardReplicationEngine {
	return &ShardReplicationEngine{
		node:                  node,
		logger:                logger.WithFields(logrus.Fields{"action": replicationEngineLogAction, "node": node}),
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
				s.logger.Debug("No shard replication op found")
				continue
			}

			s.logger.WithFields(logrus.Fields{
				"ongoingShardReplicationOps": ongoingShardReplicationOps,
				"newShardReplicationOps":     newShardReplicationOps,
			}).Info("found shard replication ops to start/recover from FSM")

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
		defer s.ongoingReplications.Add(-1)
		return backoff.Retry(func() error {
			// TODO how to cancel this context if we need to stop (eg hook it up to stopChan?)
			ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
			defer cancel()
			logger := s.logger.WithFields(logrus.Fields{
				"opId":   op.id,
				"source": op.sourceShard.String(),
				"target": op.targetShard.String(),
			})

			logger.Info("starting replica replication")

			// Update FSM that we are starting to hydrate this replica
			if err := s.leaderClient.ReplicationUpdateReplicaOpStatus(op.id, api.HYDRATING); err != nil {
				logger.WithError(err).Errorf("failed to update replica op state to %s", api.HYDRATING)
			}

			logger.Info("starting replica replication file copy")

			// Copy the replica
			// TODO change name to hydrate
			if err := s.replicaCopier.CopyReplica(ctx, op.sourceShard.nodeId, op.sourceShard.collectionId, op.targetShard.shardId); err != nil {
				logger.WithError(err).Warn("failed to file copy replica")
				// TODO: Handle failure and failure tracking in replicationFSM of replica ops
				return err
			}

			logger.Info("completed replica replication file copy")

			// Update FSM that we are done copying files and we can start final sync follower phase
			if err := s.leaderClient.ReplicationUpdateReplicaOpStatus(op.id, api.FINALIZING); err != nil {
				logger.WithError(err).Errorf("failed to update replica op state to %s", api.FINALIZING)
			}

			// Update schema to register the shard
			logger.Info("starting to finalize replica copy")
			// TODO make sure/test reads sent to target node do not use target node until op is ready/done
			// TODO get the upper time bound for this movement from the source node (query/poll?)
			// for now, just pick a time 100s in the future
			upperTimeBoundUnixMillis := time.Now().Add(100 * time.Second).UnixMilli()
			// TODO best effort writes
			if _, err := s.leaderClient.StartFinalizingReplicaCopy(ctx, op.targetShard.collectionId, op.targetShard.shardId, op.sourceShard.nodeId, op.targetShard.nodeId, upperTimeBoundUnixMillis); err != nil {
				logger.WithError(err).Errorf("failed to add replica to shard")
				return err
			}

			// TODO some kind of timer, stop, timeout, etc
			finalizationSucceeded := false
			for {
				objectsPropagated, startDiffTimeUnixMillis, err := s.replicaCopier.AsyncReplicationStatus(ctx, op.sourceShard.nodeId, op.targetShard.nodeId, op.sourceShard.collectionId, op.sourceShard.shardId)
				if err == nil && objectsPropagated == 0 {
					if startDiffTimeUnixMillis >= upperTimeBoundUnixMillis {
						finalizationSucceeded = true
						break
					}
				}
				time.Sleep(5 * time.Second)
			}

			if !finalizationSucceeded {
				// TODO handle unhappy path
			}

			// TODO remove target node override from this movement
			logger.Info("finalized replica copy")

			if err := s.leaderClient.ReplicationUpdateReplicaOpStatus(op.id, api.READY); err != nil {
				logger.WithError(err).Errorf("failed to update replica op state to %s", api.READY)
			}

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
