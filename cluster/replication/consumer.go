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
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// upperTimeBoundDuration is the duration for the upper time bound for the hash beat.
// TODO better upper time bound? for now, we use 100s because we want to have enough time
// for in progress writes to finish (assuming that in progress writes time out in ~90s)
const upperTimeBoundDuration = 100 * time.Second

// asyncStatusInterval is the polling interval to check the status of the
// async replication of src->target
const asyncStatusInterval = 5 * time.Second

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

	// ongoingOps is a cache of ongoing operations.
	// It is used to prevent duplicate operations from being processed.
	ongoingOps *OpsCache

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

	// tokens controls the maximum number of concurrently running consumers
	tokens chan struct{}

	// nodeId uniquely identifies the node on which this consumer instance is running.
	nodeId string

	// engineOpCallbacks defines hooks invoked at various stages of a replication operation's lifecycle
	// (e.g., pending, start, complete, failure) to support metrics or custom observability logic.
	engineOpCallbacks *metrics.ReplicationEngineOpsCallbacks
}

// NewCopyOpConsumer creates a new CopyOpConsumer instance responsible for executing
// replication operations using a configurable worker pool.
//
// It uses a ReplicaCopier to perform the actual data copy.
func NewCopyOpConsumer(
	logger *logrus.Logger,
	leaderClient types.FSMUpdater,
	replicaCopier types.ReplicaCopier,
	nodeId string,
	backoffPolicy backoff.BackOff,
	ongoingOps *OpsCache,
	opTimeout time.Duration,
	maxWorkers int,
	engineOpCallbacks *metrics.ReplicationEngineOpsCallbacks,
) *CopyOpConsumer {
	c := &CopyOpConsumer{
		logger:            logger.WithFields(logrus.Fields{"component": "replication_consumer", "action": replicationEngineLogAction}),
		leaderClient:      leaderClient,
		replicaCopier:     replicaCopier,
		backoffPolicy:     backoffPolicy,
		ongoingOps:        ongoingOps,
		opTimeout:         opTimeout,
		maxWorkers:        maxWorkers,
		nodeId:            nodeId,
		tokens:            make(chan struct{}, maxWorkers),
		engineOpCallbacks: engineOpCallbacks,
	}
	return c
}

// Consume processes replication operations from the input channel, ensuring that only a limited number of consumers
// are active concurrently based on the maxWorkers value.
func (c *CopyOpConsumer) Consume(ctx context.Context, in <-chan ShardReplicationOpAndStatus) error {
	c.logger.WithFields(logrus.Fields{"node": c.nodeId, "max_workers": c.maxWorkers, "op_timeout": c.opTimeout}).Info("starting replication operation consumer")

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.engineOpCallbacks.OnPrepareProcessing(c.nodeId)

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			c.logger.WithError(ctx.Err()).Info("context canceled, shutting down consumer")
			wg.Wait() // Waiting for pending operations before terminating
			return ctx.Err()

		case op, ok := <-in:
			if !ok {
				c.logger.Info("operation channel closed, shutting down consumer")
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

				// Check if the operation is already in progress
				if c.ongoingOps.LoadOrStore(op.Op.ID) {
					// Avoid scheduling unnecessary work or incorrectly counting metrics
					// for operations that are already in progress or completed.
					c.logger.WithFields(logrus.Fields{"op": op}).Debug("replication op skipped as already running")
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
						// Delete the operation from the ongoingOps map when the operation processing is complete
						c.ongoingOps.Remove(op.Op.ID)
						wg.Done()
					}()

					opLogger := getLoggerForOp(c.logger.Logger, operation.Op)
					opLogger.Debug("worker processing replication operation")

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

// dispatchReplicationOp dispatches the replication operation to the appropriate state handler
// based on the current state of the operation.
// If the state handler returns success and a valid next state, the operation is transitioned to the next state.
// If the state handler returns an error, the operation is not transitioned and the error is returned.
func (c *CopyOpConsumer) dispatchReplicationOp(ctx context.Context, op ShardReplicationOpAndStatus) error {
	switch op.Status.GetCurrentState() {
	case api.REGISTERED:
		return c.processStateAndTransition(ctx, op, c.processRegisteredOp)
	case api.HYDRATING:
		return c.processStateAndTransition(ctx, op, c.processHydratingOp)
	case api.DEHYDRATING:
		return c.processStateAndTransition(ctx, op, c.processDehydratingOp)
	case api.FINALIZING:
		return c.processStateAndTransition(ctx, op, c.processFinalizingOp)
	case api.ABORTED:
		// TODO: In the future we should handle cleaning up aborted operations, for now just keep it in the FSM
		return nil
	case api.READY:
		// TODO: In the future we should handle cleaning up completed operations, for now just keep it in the FSM
		return nil
	default:
		getLoggerForOp(c.logger.Logger, op.Op).WithFields(logrus.Fields{"op_status": op.Status.GetCurrentState()}).Error("unknown replication operation state")
		return fmt.Errorf("unknown replication operation state: %s", op.Status.GetCurrentState())
	}
}

// stateFuncHandler is a function that processes a replication operation and returns the next state and an error.
type stateFuncHandler func(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error)

// processStateAndTransition processes a replication operation and transitions it to the next state.
// It retries the operation using a backoff policy if it returns an error.
// If the operation is successful, the operation is transitioned to the next state.
// Otherwise, the operation is transitioned to the next state and the process continues.
func (c *CopyOpConsumer) processStateAndTransition(ctx context.Context, op ShardReplicationOpAndStatus, stateFuncHandler stateFuncHandler) error {
	nextState, err := backoff.RetryWithData(func() (api.ShardReplicationState, error) {
		logger := getLoggerForOp(c.logger.Logger, op.Op)
		if ctx.Err() != nil {
			logger.WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
			return api.ShardReplicationState(""), backoff.Permanent(ctx.Err())
		}

		nextState, err := stateFuncHandler(ctx, op)
		// If we receive an error from the state handler make sure we store it and then stop processing
		if err != nil {
			err = c.leaderClient.ReplicationRegisterError(op.Op.ID, err.Error())
			if err != nil {
				logger.WithField("consumer", c).WithError(err).Error("failed to register error for replication operation")
			}
			return api.ShardReplicationState(""), err
		}

		// No error from the state handler, update the state to the next, if this errors we will stop processing
		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.Op.ID, nextState); err != nil {
			logger.WithField("consumer", c).WithError(err).Errorf("failed to update replica status to '%s'", nextState)
			return api.ShardReplicationState(""), err
		}
		return nextState, nil
	}, c.backoffPolicy)
	if err != nil {
		return err
	}

	op.Status.ChangeState(nextState)
	return c.dispatchReplicationOp(ctx, op)
}

// processRegisteredOp is the state handler for the REGISTERED state.
func (c *CopyOpConsumer) processRegisteredOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOp(c.logger.Logger, op.Op)
	logger.Info("processing registered replication operation")

	return api.HYDRATING, nil
}

// processHydratingOp is the state handler for the HYDRATING state.
// It copies the replica shard from the source node to the target node using file copy opetaitons and then transitions the operation to the FINALIZING state.
func (c *CopyOpConsumer) processHydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOp(c.logger.Logger, op.Op)
	logger.Info("processing hydrating replication operation")

	if err := c.replicaCopier.CopyReplica(ctx, op.Op.SourceShard.NodeId, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId); err != nil {
		logger.WithField("consumer", c).WithError(err).Error("failure while copying replica shard")
		return api.ShardReplicationState(""), err
	}

	return api.FINALIZING, nil
}

// processFinalizingOp is the state handler for the FINALIZING state.
// It updates the sharding state and then transitions the operation to the READY state.
func (c *CopyOpConsumer) processFinalizingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOp(c.logger.Logger, op.Op)
	logger.Info("processing finalizing replication operation")

	// ensure async replication is started on local (target) node
	if err := c.replicaCopier.InitAsyncReplicationLocally(ctx, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while initializing async replication on local node")
		return api.ShardReplicationState(""), err
	}
	// TODO start best effort writes before upper time bound is hit
	// TODO make sure/test reads sent to target node do not use target node until op is ready/done and that writes
	// received during movement work as expected
	upperTimeBoundUnixMillis := time.Now().Add(upperTimeBoundDuration).UnixMilli()

	// start async replication from source node to target node
	if err := c.replicaCopier.SetAsyncReplicationTargetNode(
		ctx,
		additional.AsyncReplicationTargetNodeOverride{
			CollectionID:   op.Op.SourceShard.CollectionId,
			ShardID:        op.Op.TargetShard.ShardId,
			TargetNode:     op.Op.TargetShard.NodeId,
			SourceNode:     op.Op.SourceShard.NodeId,
			UpperTimeBound: upperTimeBoundUnixMillis,
		}); err != nil {
		return api.ShardReplicationState(""), err
	}

	do := func() bool {
		asyncReplicationStatus, err := c.replicaCopier.AsyncReplicationStatus(ctx, op.Op.SourceShard.NodeId, op.Op.TargetShard.NodeId, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId)
		c.logger.WithFields(logrus.Fields{"err": err, "objects_propagated": asyncReplicationStatus.ObjectsPropagated, "start_diff_time_unix_millis": asyncReplicationStatus.StartDiffTimeUnixMillis, "upper_time_bound_unix_millis": upperTimeBoundUnixMillis}).Info("async replication status")
		return err == nil && asyncReplicationStatus.ObjectsPropagated == 0 && asyncReplicationStatus.StartDiffTimeUnixMillis >= upperTimeBoundUnixMillis
	}

	// we only check the status of the async replication every 5 seconds to avoid
	// spamming with too many requests too quickly
	err := backoff.Retry(func() error {
		if do() {
			return nil
		}
		return errors.New("async replication not done")
	}, backoff.WithContext(backoff.NewConstantBackOff(asyncStatusInterval), ctx))
	if err != nil {
		logger.WithError(err).Error("failure while waiting for async replication to complete")
		return api.ShardReplicationState(""), err
	}

	if _, err := c.leaderClient.AddReplicaToShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId); err != nil {
		logger.WithError(err).Error("failure while adding replica to shard")
		return api.ShardReplicationState(""), err
	}

	switch op.Op.TransferType {
	case api.COPY:
		return api.READY, nil
	case api.MOVE:
		return api.DEHYDRATING, nil
	default:
		return api.ShardReplicationState(""), fmt.Errorf("unknown transfer type: %s", op.Op.TransferType)
	}
}

// processDehydratingOp is the state handler for the DEHYDRATING state.
func (c *CopyOpConsumer) processDehydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOp(c.logger.Logger, op.Op)
	logger.Info("processing dehydrating replication operation")

	if _, err := c.leaderClient.DeleteReplicaFromShard(ctx, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, op.Op.SourceShard.NodeId); err != nil {
		logger.WithError(err).Error("failure while deleting replica from shard")
		return api.ShardReplicationState(""), err
	}
	return api.READY, nil
}
