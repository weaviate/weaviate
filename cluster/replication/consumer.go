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
	"fmt"
	"sync"
	"time"

	"github.com/weaviate/weaviate/cluster/replication/metrics"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// OpConsumer is an interface for consuming replication operations.
type OpConsumer interface {
	// Consume starts consuming operations from the provided channel.
	// The consumer processes operations, and a buffered channel is typically used to apply backpressure.
	// The consumer should return an error if it fails to process any operation.
	Consume(ctx context.Context, in <-chan ShardReplicationOpAndStatus) error
}

// CopyOpConsumer is an implementation of the OpConsumer interface that processes replication operations
// by executing copy operations from a source shard to a target shard. It uses a ReplicaCopier to actually
// carry out the copy operation. Moreover, it supports configurable backoff, timeout and concurrency limits.
type CopyOpConsumer struct {
	// logger is used for structured logging throughout the consumer's lifecycle.
	// It provides detailed logs for each replication operation and any errors encountered.
	logger *logrus.Entry

	// shouldSkipOp is a function that determines whether a given replication operation
	// should be skipped before attempting execution. This is typically used to prevent
	// reprocessing of operations that are already running, completed, or not in a valid
	// state to be picked up.
	shouldSkipOp func(ShardReplicationOpAndStatus) bool

	// leaderClient is responsible for interacting with the FSM to update the state of replication operations.
	// It is used to update the status of operations during the replication process (e.g. update to HYDRATING state).
	leaderClient types.FSMUpdater

	// replicaCopier is used to handle the actual copying of replica data from the source shard to the target shard.
	// It abstracts the mechanics of data replication and file copying.
	replicaCopier types.ReplicaCopier

	// backoffPolicy defines the retry mechanism for failed operations.
	// It allows the consumer to retry replication operations using a backoff strategy in case of failure.
	backoffPolicy backoff.BackOff

	// maxWorkers sets the maximum number of concurrent workers that will be used to process replication operations.
	// It controls the level of parallelism in the replication process allowing multiple replication operations to
	// run concurrently.
	maxWorkers int

	// opTimeout defines the timeout duration for each replication operation.
	// It ensures that operations do not hang indefinitely and are retried or terminated after the timeout period.
	opTimeout time.Duration

	// timeProvider abstracts time operations, allowing for easier testing and mocking of time-related functions.
	timeProvider TimeProvider

	// tokens controls the maximum number of concurrently running consumers
	tokens chan struct{}

	// nodeId uniquely identifies the node on which this consumer instance is running.
	nodeId string

	// engineOpCallbacks defines hooks invoked at various stages of a replication operation's lifecycle
	// (e.g., pending, start, complete, failure) to support metrics or custom observability logic.
	engineOpCallbacks *metrics.ReplicationEngineOpsCallbacks
}

// String returns a string representation of the CopyOpConsumer,
// including the node ID that uniquely identifies the consumer.
//
// The assumption is that each node runs one and only one replication engine,
// which means there is one consumer per node.
func (c *CopyOpConsumer) String() string {
	return fmt.Sprintf("replication engine copy consumer on node '%s'", c.nodeId)
}

// NewCopyOpConsumer creates a new CopyOpConsumer instance responsible for executing
// replication operations using a configurable worker pool.
//
// It uses a ReplicaCopier to perform the actual data copy.
func NewCopyOpConsumer(
	logger *logrus.Logger,
	shouldSkipOp func(ShardReplicationOpAndStatus) bool,
	leaderClient types.FSMUpdater,
	replicaCopier types.ReplicaCopier,
	timeProvider TimeProvider,
	nodeId string,
	backoffPolicy backoff.BackOff,
	opTimeout time.Duration,
	maxWorkers int,
	engineOpCallbacks *metrics.ReplicationEngineOpsCallbacks,
) *CopyOpConsumer {
	c := &CopyOpConsumer{
		logger:            logger.WithFields(logrus.Fields{"component": "replication_consumer", "action": replicationEngineLogAction, "node": nodeId, "workers": maxWorkers, "timeout": opTimeout}),
		shouldSkipOp:      shouldSkipOp,
		leaderClient:      leaderClient,
		replicaCopier:     replicaCopier,
		backoffPolicy:     backoffPolicy,
		opTimeout:         opTimeout,
		maxWorkers:        maxWorkers,
		nodeId:            nodeId,
		timeProvider:      timeProvider,
		tokens:            make(chan struct{}, maxWorkers),
		engineOpCallbacks: engineOpCallbacks,
	}
	return c
}

// Consume processes replication operations from the input channel, ensuring that only a limited number of consumers
// are active concurrently based on the maxWorkers value.
func (c *CopyOpConsumer) Consume(ctx context.Context, in <-chan ShardReplicationOpAndStatus) error {
	c.logger.Info("starting replication operation consumer")

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.engineOpCallbacks.OnPrepareProcessing(c.nodeId)

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			c.logger.WithFields(logrus.Fields{"consumer": c, "reason": ctx.Err()}).Info("context canceled, shutting down consumer")
			wg.Wait() // Waiting for pending operations before terminating
			return ctx.Err()

		case op, ok := <-in:
			if !ok {
				c.logger.WithFields(logrus.Fields{"consumer": c}).Info("operation channel closed, shutting down consumer")
				wg.Wait() // Waiting for pending operations before terminating
				return nil
			}

			c.engineOpCallbacks.OnOpPending(c.nodeId)
			select {
			// The 'tokens' channel limits the number of concurrent workers (`maxWorkers`).
			// Each worker acquires a token before processing an operation. If no tokens are available,
			// the worker blocks until one is released. After completing the task, the worker releases the token,
			// allowing another worker to proceed. This ensures only a limited number of workers is concurrently
			// running replication operations and avoids overloading the system.
			case c.tokens <- struct{}{}:

				// Avoid scheduling unnecessary work or incorrectly counting metrics
				// for operations that are already in progress or completed.
				if c.shouldSkipOp(op) {
					c.logger.WithFields(logrus.Fields{"consumer": c, "op": op}).Debug("replication op skipped as already running or completed")
					// Need to release the token to let other consumers process queued replication operations.
					<-c.tokens
					c.engineOpCallbacks.OnOpSkipped(c.nodeId)
					continue
				}

				wg.Add(1)
				c.engineOpCallbacks.OnOpStart(c.nodeId)

				// Here we capture the op argument used by the func below as the enterrors.GoWrapper requires calling
				// a function without arguments.
				operation := op

				enterrors.GoWrapper(func() {
					defer func() {
						<-c.tokens // Release token when completed
						wg.Done()
					}()

					opLogger := c.logger.WithFields(logrus.Fields{
						"consumer":          c,
						"op":                operation.Op.ID,
						"source_node":       operation.Op.SourceShard.NodeId,
						"target_node":       operation.Op.TargetShard.NodeId,
						"source_shard":      operation.Op.SourceShard.ShardId,
						"target_shard":      operation.Op.TargetShard.ShardId,
						"source_collection": operation.Op.SourceShard.CollectionId,
						"target_collection": operation.Op.TargetShard.CollectionId,
					})

					opLogger.Info("worker processing replication operation")

					// Start a replication operation with a timeout for completion to prevent replication operations
					// from running indefinitely
					opCtx, opCancel := context.WithTimeout(workerCtx, c.opTimeout)
					defer opCancel()

					err := c.dispatchReplicationOp(opCtx, operation)
					if err != nil && errors.Is(err, context.DeadlineExceeded) {
						c.engineOpCallbacks.OnOpFailed(c.nodeId)
						opLogger.WithError(err).Error("replication operation timed out")
					} else if err != nil {
						c.engineOpCallbacks.OnOpFailed(c.nodeId)
						opLogger.WithError(err).Error("replication operation failed")
					} else {
						c.engineOpCallbacks.OnOpComplete(c.nodeId)
					}
				}, c.logger)

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (c *CopyOpConsumer) dispatchReplicationOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	switch op.Status.State {
	case api.REGISTERED:
		return c.processRegisteredOp(ctx, op)
	case api.HYDRATING:
		return c.processHydratingOp(ctx, op)
	case api.DEHYDRATING:
		return c.processDehydratingOp(ctx, op)
	case api.FINALIZING:
		return c.processFinalizingOp(ctx, op)
	case api.ABORTED:
		return c.processAbortedOp(ctx, op)
	default:
		getLoggerForOp(c.logger.Logger, op.Op).WithFields(logrus.Fields{"consumer": c, "op_status": op.Status.State}).Error("unknown replication operation state")
		return fmt.Errorf("unknown replication operation state: %s", op.Status.State)
	}
}

func (c *CopyOpConsumer) processRegisteredOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	logger := getLoggerForOp(c.logger.Logger, op.Op).WithFields(logrus.Fields{"consumer": c})
	logger.Info("processing registered replication operation")

	return backoff.Retry(func() error {
		if ctx.Err() != nil {
			logger.WithField("consumer", c).WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
			return backoff.Permanent(ctx.Err())
		}

		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.Op.ID, api.HYDRATING); err != nil {
			logger.WithField("consumer", c).WithError(err).Error("failed to update replica status to 'HYDRATING'")
			return err
		}
		return nil
	}, c.backoffPolicy)
}

func (c *CopyOpConsumer) processHydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	logger := getLoggerForOp(c.logger.Logger, op.Op).WithFields(logrus.Fields{"consumer": c})
	logger.Info("processing hydrating replication operation")

	return backoff.Retry(func() error {
		if ctx.Err() != nil {
			logger.WithField("consumer", c).WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
			return backoff.Permanent(ctx.Err())
		}

		if err := c.replicaCopier.CopyReplica(ctx, op.Op.SourceShard.NodeId, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId); err != nil {
			logger.WithField("consumer", c).WithError(err).Error("failure while copying replica shard")
			return err
		}

		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.Op.ID, api.FINALIZING); err != nil {
			logger.WithField("consumer", c).WithError(err).Error("failed to update replica status to 'FINALIZING'")
			return err
		}
		return nil
	}, c.backoffPolicy)
}

func (c *CopyOpConsumer) processDehydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	// TODO: Implement
	return nil
}

func (c *CopyOpConsumer) processFinalizingOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	logger := getLoggerForOp(c.logger.Logger, op.Op).WithFields(logrus.Fields{"consumer": c})
	logger.Info("processing finalizing replication operation")

	return backoff.Retry(func() error {
		if ctx.Err() != nil {
			logger.WithField("consumer", c).WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
			return backoff.Permanent(ctx.Err())
		}

		if _, err := c.leaderClient.AddReplicaToShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId); err != nil {
			logger.WithField("consumer", c).WithError(err).Error("failure while updating sharding state")
			return err
		}

		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.Op.ID, api.FINALIZING); err != nil {
			logger.WithField("consumer", c).WithError(err).Error("failed to update replica status to 'FINALIZING'")
			return err
		}
		return nil
	}, c.backoffPolicy)
}

func (c *CopyOpConsumer) processAbortedOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	// TODO: Implement
	return nil
}
