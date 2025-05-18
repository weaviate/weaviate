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
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/config/runtime"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// asyncStatusInterval is the polling interval to check the status of the
// async replication of src->target
const (
	asyncStatusInterval = 5 * time.Second
	// if async status errors more than 30 times, stop retrying
	asyncStatusMaxErrors = 30
	// about `asyncStatusInterval` seconds per retry, 120 retries = 10 minutes for async replication
	// to complete
	asyncStatusMaxRetries = 120
)

// OpConsumer is an interface for consuming replication operations.
type OpConsumer interface {
	// Consume starts consuming operations from the provided channel.
	// The consumer processes operations, and a buffered channel is typically used to apply backpressure.
	// The consumer should return an error if it fails to process any operation.
	Consume(ctx context.Context, in <-chan ShardReplicationOpAndStatus) error
}

// DELETED is a constant representing a temporary deleted state of a replication operation that should not be stored in the FSM.
const DELETED = "deleted"

// errOpCancelled is an error indicating that the operation was cancelled.
var errOpCancelled = errors.New("operation cancelled")

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

	// schemaReader is used to read the schema
	schemaReader schema.SchemaReader

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

	// asyncReplicationMinimumWait is the duration for the upper time bound for the hash beat.
	asyncReplicationMinimumWait *runtime.DynamicValue[time.Duration]

	stopped atomic.Bool
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
	asyncReplicationMinimumWait *runtime.DynamicValue[time.Duration],
	engineOpCallbacks *metrics.ReplicationEngineOpsCallbacks,
	schemaReader schema.SchemaReader,
) *CopyOpConsumer {
	c := &CopyOpConsumer{
		logger:                      logger.WithFields(logrus.Fields{"component": "replication_consumer", "action": replicationEngineLogAction}),
		leaderClient:                leaderClient,
		replicaCopier:               replicaCopier,
		backoffPolicy:               backoffPolicy,
		ongoingOps:                  ongoingOps,
		opTimeout:                   opTimeout,
		maxWorkers:                  maxWorkers,
		nodeId:                      nodeId,
		tokens:                      make(chan struct{}, maxWorkers),
		engineOpCallbacks:           engineOpCallbacks,
		asyncReplicationMinimumWait: asyncReplicationMinimumWait,
		schemaReader:                schemaReader,
	}
	return c
}

// Consume processes replication operations from the input channel, ensuring that only a limited number of consumers
// are active concurrently based on the maxWorkers value.
func (c *CopyOpConsumer) Consume(ctx context.Context, in <-chan ShardReplicationOpAndStatus) error {
	c.logger.WithFields(logrus.Fields{"node": c.nodeId, "max_workers": c.maxWorkers, "op_timeout": c.opTimeout}).Info("starting replication operation consumer")

	c.stopped.Store(false)
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.engineOpCallbacks.OnPrepareProcessing(c.nodeId)

	var wg sync.WaitGroup

	shutdownRoutine := func() error {
		c.logger.Info("shutting down consumer")
		c.stopped.Store(true)
		// TODO: We can have a race between adding an op to the waigroup and adding it's cancel func to the map.
		// Find a way to fix this. For now the timeout will catch this and exit after anyway
		c.ongoingOps.CancelAll()

		// Wait for pending operations to complete
		ch := make(chan struct{})
		enterrors.GoWrapper(func() {
			defer close(ch)
			wg.Wait()
		}, c.logger)
		select {
		case <-ch:
			return nil
		case <-time.After(10 * time.Second):
			return ctx.Err()
		}
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.WithError(ctx.Err()).Info("context canceled, shutting down consumer")
			c.stopped.Store(true)
			c.ongoingOps.CancelAll()
			wg.Wait() // Waiting for pending operations before terminating
			return shutdownRoutine()

		case op, ok := <-in:
			if !ok {
				c.logger.Info("operation channel closed, shutting down consumer")
				return shutdownRoutine()
			}

			// If the consumer is stopped and somehow the channel is open, ignore the op
			if c.stopped.Load() {
				continue
			}

			// If the operation has been scheduled for cancellation or deletion
			// This is done outside of the worker goroutine, and therefore without acquiring a token, so that
			// we can cancel operations that have frozen or become unresponsive. If we were to acquire a token
			// we would block the worker pool and not be able to cancel the operation leading to resource starvation.
			if op.Status.ShouldCancel && !c.ongoingOps.HasBeenCancelled(op.Op.ID) {
				// Update the cache to mark the operation as cancelled
				c.ongoingOps.StoreHasBeenCancelled(op.Op.ID)
				c.logger.WithFields(logrus.Fields{"op": op}).Debug("cancelled the replication op")
				if c.ongoingOps.InFlight(op.Op.ID) {
					// Cancel the in-flight operation
					// Is a noop, returns false if the op doesn't exist
					c.ongoingOps.Cancel(op.Op.ID)
					// Continue to ensure we don't accidentally re-spawn the operation in a new worker
					continue
				}
				// Otherwise, the operation is not in-flight and should therefore be processed in a worker where clean-up happens
			}

			c.engineOpCallbacks.OnOpPending(c.nodeId)
			select {
			// The 'tokens' channel limits the number of concurrent workers (`maxWorkers`).
			// Each worker acquires a token before processing an operation. If no tokens are available,
			// the worker blocks until one is released. After completing the task, the worker releases the token,
			// allowing another worker to proceed. This ensures only a limited number of workers is concurrently
			// running replication operations and avoids overloading the system.
			case c.tokens <- struct{}{}:

				// Here we capture the op argument used by the func below as the enterrors.GoWrapper requires calling
				// a function without arguments.
				operation := op
				opLogger := getLoggerForOpAndStatus(c.logger.Logger, operation.Op, op.Status)
				shouldSkip := false
				if c.ongoingOps.LoadOrStore(op.Op.ID) {
					// Check if the operation is already in progress
					// Avoid scheduling unnecessary work or incorrectly counting metrics
					// for operations that are already in progress or completed.
					c.logger.Debug("replication op skipped as already running")
					shouldSkip = true
				} else {
					// Check if the operation has had its state changed between being added to the channel and being processed
					// This is chatty and will likely cause a lot of unnecessary load on the leader
					// For now, we need it to ensure eventual consistency between the FSM and the consumer
					state, err := c.leaderClient.ReplicationGetReplicaOpStatus(op.Op.ID)
					if err != nil {
						c.logger.WithFields(logrus.Fields{"op": op}).Error("error while checking status of replication op")
						shouldSkip = true
					} else if state.String() != op.Status.GetCurrent().State.String() {
						c.logger.WithFields(logrus.Fields{"op": op}).Debug("replication op skipped as state has changed")
						shouldSkip = true
					}
				}

				if op.Status.GetCurrent().State == "" {
					c.logger.Debug("replication op skipped as state is empty")
					shouldSkip = true
				}

				// TODO: Consider more optimal ways of checking that the state of the op has not changed between it being added to the channel
				// and being processed here. Could use in-memory solution, e.g. using cache, or refactor consumer-producer to be event/notification-based
				// For now, ensure consistency by checking the FSM through the leader

				// Need to release the token to let other consumers process queued replication operations.
				if shouldSkip {
					<-c.tokens
					c.ongoingOps.DeleteInFlight(op.Op.ID)
					c.engineOpCallbacks.OnOpSkipped(c.nodeId)
					continue
				}

				wg.Add(1)
				c.engineOpCallbacks.OnOpStart(c.nodeId)

				enterrors.GoWrapper(func() {
					defer func() {
						<-c.tokens // Release token when completed
						// Delete the operation from the ongoingOps map when the operation processing is complete
						c.ongoingOps.DeleteInFlight(op.Op.ID)
						wg.Done()
					}()

					opLogger.Debug("worker processing replication operation")

					// If the operation has been cancelled in the time between it being added to the channel and
					// being processed, we need to cancel it in the FSM and return
					if c.ongoingOps.HasBeenCancelled(op.Op.ID) {
						c.logger.WithFields(logrus.Fields{"op": operation}).Debug("replication op cancelled, stopping replication operation")
						c.cancelOp(operation, opLogger)
						return
					}

					// Start a replication operation with a timeout for completion to prevent replication operations
					// from running indefinitely
					opCtx, opCancel := context.WithTimeout(workerCtx, c.opTimeout)
					defer opCancel()
					c.ongoingOps.StoreCancel(op.Op.ID, opCancel)

					err := c.dispatchReplicationOp(opCtx, operation)
					if err == nil {
						c.engineOpCallbacks.OnOpComplete(c.nodeId)
						return
					}
					if errors.Is(err, context.DeadlineExceeded) {
						c.engineOpCallbacks.OnOpFailed(c.nodeId)
						opLogger.WithError(err).Error("replication operation timed out")
						return
					}
					if errors.Is(err, context.Canceled) && c.ongoingOps.HasBeenCancelled(op.Op.ID) {
						opLogger.WithError(err).Debug("replication operation cancelled")
						c.cancelOp(operation, opLogger)
						return
					}
					if errors.Is(err, errOpCancelled) {
						opLogger.WithError(err).Debug("replication operation cancelled")
						c.cancelOp(operation, opLogger)
						return
					}
					c.engineOpCallbacks.OnOpFailed(c.nodeId)
					opLogger.WithError(err).Error("replication operation failed")
				}, c.logger)

			// If main context is cancelled here we just continue so that we hit the shutdown logic on the next iteration
			case <-ctx.Done():
				continue
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
	case api.READY:
		return nil
	case api.CANCELLED:
		return c.processStateAndTransition(ctx, op, c.processCancelledOp)
	default:
		getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status).Error("unknown replication operation state")
		return fmt.Errorf("unknown replication operation state: %s", op.Status.GetCurrentState())
	}
}

// stateFuncHandler is a function that processes a replication operation and returns the next state and an error.
type stateFuncHandler func(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error)

func (c *CopyOpConsumer) checkCancelled(logger *logrus.Entry, op ShardReplicationOpAndStatus) error {
	if c.ongoingOps.HasBeenCancelled(op.Op.ID) {
		logger.WithFields(logrus.Fields{"op": op}).Debug("replication op cancelled, stopping replication operation")
		return errOpCancelled
	}
	return nil
}

// processStateAndTransition processes a replication operation and transitions it to the next state.
// It retries the operation using a backoff policy if it returns an error.
// If the operation is successful, the operation is transitioned to the next state.
// Otherwise, the operation is transitioned to the next state and the process continues.
func (c *CopyOpConsumer) processStateAndTransition(ctx context.Context, op ShardReplicationOpAndStatus, stateFuncHandler stateFuncHandler) error {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	nextState, err := backoff.RetryWithData(func() (api.ShardReplicationState, error) {
		if ctx.Err() != nil {
			logger.WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
			return api.ShardReplicationState(""), backoff.Permanent(ctx.Err())
		}
		if err := c.checkCancelled(logger, op); err != nil {
			return api.ShardReplicationState(""), backoff.Permanent(fmt.Errorf("error while checking if op is cancelled: %w", err))
		}

		nextState, err := stateFuncHandler(ctx, op)
		// If we receive an error from the state handler make sure we store it and then stop processing
		if err != nil {
			// If the op was cancelled for any reason, pass the error up the stack to be handled higher up
			if errors.Is(err, context.Canceled) {
				logger.Debug("context cancelled, stopping replication operation")
				return api.ShardReplicationState(""), backoff.Permanent(fmt.Errorf("context cancelled: %w", err))
			}
			if err := c.checkCancelled(logger, op); err != nil {
				return api.ShardReplicationState(""), backoff.Permanent(fmt.Errorf("error while checking if op is cancelled: %w", err))
			}
			logger.WithError(err).Warn("state transition handler failed")
			// Otherwise, register the error with the FSM
			err = c.leaderClient.ReplicationRegisterError(op.Op.ID, err.Error())
			if err != nil {
				logger.WithError(err).Error("failed to register error for replication operation")
			}
			return api.ShardReplicationState(""), err
		}

		if err := c.checkCancelled(logger, op); err != nil {
			return api.ShardReplicationState(""), backoff.Permanent(fmt.Errorf("error while checking if op is cancelled: %w", err))
		}
		// No error from the state handler, update the state to the next, if this errors we will stop processing
		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.Op.ID, nextState); err != nil {
			logger.WithError(err).Errorf("failed to update replica status to '%s'", nextState)
			return api.ShardReplicationState(""), fmt.Errorf("failed to update replica status to '%s': %w", nextState, err)
		}
		return nextState, nil
	}, c.backoffPolicy)
	if err != nil {
		return err
	}

	if nextState == DELETED {
		// Stop the recursion if we are in the DELETED state and don't update the state in the FSM
		return nil
	}

	op.Status.ChangeState(nextState)
	if nextState == api.READY {
		// No need to continue the recursion if we are in the READY state
		return nil
	}

	if err := c.checkCancelled(logger, op); err != nil {
		return err
	}
	return c.dispatchReplicationOp(ctx, op)
}

// cancelOp performs clean up for the cancelled operation and notifies the FSM of the cancellation.
//
// It removes the replica shard from the target node and updates the FSM with the cancellation status.
// If the operation is being cancelled, it notifies the FSM to complete the cancellation.
// If the operation is being deleted, it notifies the FSM to remove the operation from the FSM.
// It returns an error if any of the operations fail.
//
// It exists outside of the formal state machine to allow for cancellation of operations that are in progress
// or have been cancelled but not yet processed without introducing new intermediate states to the FSM.
func (c *CopyOpConsumer) cancelOp(op ShardReplicationOpAndStatus, logger *logrus.Entry) {
	defer func() {
		c.ongoingOps.DeleteHasBeenCancelled(op.Op.ID)
		c.engineOpCallbacks.OnOpCancelled(c.nodeId)
	}()

	// Ensure that the states of the shards on the nodes are in-sync with the state of the schema through a RAFT communication
	// This handles cleaning up for ghost shards that are in the store but not in the schema that may have been created by index.getOptInitShard
	// Both methods return early on error to avoid completing cancellation so that the op can be cleaned-up again on failure when it is cancelled again

	if err := c.sync(context.Background(), op); err != nil {
		logger.WithError(err).
			WithField("op", op).
			Error(fmt.Errorf("failure when syncing shards for op: %s", op.Op.UUID))
	}

	// If the operation is only being cancelled then notify the FSM so it can update its state
	if op.Status.OnlyCancellation() {
		if err := c.leaderClient.ReplicationCancellationComplete(op.Op.ID); err != nil {
			logger.WithError(err).Error("failure while completing cancellation of replica operation")
		}
		return
	}

	// If the operation is being deleted then remove it from the FSM
	if op.Status.ShouldDelete {
		if err := c.leaderClient.ReplicationRemoveReplicaOp(op.Op.ID); err != nil {
			logger.WithError(err).Error("failure while deleting replica operation")
		}
		return
	}
}

func (c *CopyOpConsumer) sync(ctx context.Context, op ShardReplicationOpAndStatus) error {
	if _, err := c.leaderClient.SyncShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId); err != nil {
		return fmt.Errorf("failure while syncing replica shard: %w", err)
	}
	if _, err := c.leaderClient.SyncShard(ctx, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, op.Op.SourceShard.NodeId); err != nil {
		return fmt.Errorf("failure while syncing replica shard: %w", err)
	}
	return nil
}

// processRegisteredOp is the state handler for the REGISTERED state.
func (c *CopyOpConsumer) processRegisteredOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	logger.Info("processing registered replication operation")

	return api.HYDRATING, nil
}

// processHydratingOp is the state handler for the HYDRATING state.
// It copies the replica shard from the source node to the target node using file copy opetaitons and then transitions the operation to the FINALIZING state.
func (c *CopyOpConsumer) processHydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	logger.Info("processing hydrating replication operation")

	if c.schemaReader.MultiTenancy(op.Op.TargetShard.CollectionId).Enabled {
		schemaVersion, err := c.leaderClient.UpdateTenants(ctx, op.Op.TargetShard.CollectionId, &api.UpdateTenantsRequest{
			Tenants: []*api.Tenant{
				{
					Name:   op.Op.SourceShard.ShardId,
					Status: models.TenantActivityStatusHOT,
				},
			},
		})
		if err != nil {
			logger.WithError(err).Error("failure while updating tenant to active state for hydrating operation")
			return api.ShardReplicationState(""), err
		}

		if err := c.leaderClient.ReplicationStoreSchemaVersion(op.Op.ID, schemaVersion); err != nil {
			logger.WithError(err).Error("failure while storing schema version for replication operation")
			return api.ShardReplicationState(""), err
		}
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.replicaCopier.CopyReplica(ctx, op.Op.SourceShard.NodeId, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId, op.Status.SchemaVersion); err != nil {
		logger.WithError(err).Error("failure while copying replica shard")
		return api.ShardReplicationState(""), err
	}

	return api.FINALIZING, nil
}

// processFinalizingOp is the state handler for the FINALIZING state.
// It updates the sharding state and then transitions the operation to the READY state.
func (c *CopyOpConsumer) processFinalizingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	logger.Info("processing finalizing replication operation")

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	// ensure async replication is started on local (target) node
	if err := c.replicaCopier.InitAsyncReplicationLocally(ctx, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while initializing async replication on local node while finalizing")
		return api.ShardReplicationState(""), err
	}

	// this time will be used to make sure async replication has propagated any writes which
	// were received during the hydrating phase
	asyncReplicationUpperTimeBoundUnixMillis := time.Now().UnixMilli()

	// start async replication from source node to target node
	targetNodeOverride := additional.AsyncReplicationTargetNodeOverride{
		CollectionID:   op.Op.SourceShard.CollectionId,
		ShardID:        op.Op.TargetShard.ShardId,
		TargetNode:     op.Op.TargetShard.NodeId,
		SourceNode:     op.Op.SourceShard.NodeId,
		UpperTimeBound: asyncReplicationUpperTimeBoundUnixMillis,
	}
	if err := c.replicaCopier.AddAsyncReplicationTargetNode(ctx, targetNodeOverride, op.Status.SchemaVersion); err != nil {
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("error while processing replication operation, shutting down")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.waitForAsyncReplication(ctx, op, asyncReplicationUpperTimeBoundUnixMillis, logger); err != nil {
		logger.WithError(err).Error("failure while waiting for async replication to complete while finalizing")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("error while processing replication operation, shutting down")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if _, err := c.leaderClient.AddReplicaToShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId); err != nil {
		logger.WithError(err).Error("failure while adding replica to shard")
		return api.ShardReplicationState(""), err
	}

	switch op.Op.TransferType {
	case api.COPY:
		if err := c.replicaCopier.RevertAsyncReplicationLocally(ctx, op.Op.TargetShard.CollectionId, op.Op.SourceShard.ShardId); err != nil {
			logger.WithError(err).Error("failure while reverting async replication on local node")
		}
		if err := c.replicaCopier.RemoveAsyncReplicationTargetNode(ctx, targetNodeOverride); err != nil {
			logger.WithError(err).Error("failure while removing async replication target node")
		}
		// sync the replica shard to ensure that the schema and store are consistent on each node
		// In a COPY this happens now, in a MOVE this happens in the DEHYDRATING state
		if err := c.sync(ctx, op); err != nil {
			logger.WithError(err).Error("failure while syncing replica shard")
			return api.ShardReplicationState(""), err
		}
		return api.READY, nil
	case api.MOVE:
		return api.DEHYDRATING, nil
	default:
		return api.ShardReplicationState(""), fmt.Errorf("unknown transfer type: %s", op.Op.TransferType)
	}
}

// processDehydratingOp is the state handler for the DEHYDRATING state.
func (c *CopyOpConsumer) processDehydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	logger.Info("processing dehydrating replication operation")

	asyncReplicationUpperTimeBoundUnixMillis := time.Now().Add(c.asyncReplicationMinimumWait.Get()).UnixMilli()

	// Async replication was started in processFinalizingOp, but here we want to "increase" the upper time bound
	// to make sure any writes received by the source node before the op entered the DEHYDRATING state are
	// propagated to the target node. We assume writes will complete or time out (default 90s) within the
	// asyncReplicationMinimumWait time (default 100s). The source node should not receive any writes after the op
	// enters the DEHYDRATING state.
	targetNodeOverride := additional.AsyncReplicationTargetNodeOverride{
		CollectionID:   op.Op.SourceShard.CollectionId,
		ShardID:        op.Op.TargetShard.ShardId,
		TargetNode:     op.Op.TargetShard.NodeId,
		SourceNode:     op.Op.SourceShard.NodeId,
		UpperTimeBound: asyncReplicationUpperTimeBoundUnixMillis,
	}

	// ensure async replication is started on local (target) node
	// note that this is a no-op if async replication was already started in the finalizing step,
	// but we're just trying to be defensive here and make sure it is actually started
	if err := c.replicaCopier.InitAsyncReplicationLocally(ctx, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while initializing async replication on local node while dehydrating")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.replicaCopier.AddAsyncReplicationTargetNode(ctx, targetNodeOverride, op.Status.SchemaVersion); err != nil {
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("error while processing replication operation, shutting down")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.waitForAsyncReplication(ctx, op, asyncReplicationUpperTimeBoundUnixMillis, logger); err != nil {
		logger.WithError(err).Error("failure while waiting for async replication to complete while dehydrating")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if _, err := c.leaderClient.DeleteReplicaFromShard(ctx, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, op.Op.SourceShard.NodeId); err != nil {
		logger.WithError(err).Error("failure while deleting replica from shard")
		return api.ShardReplicationState(""), err
	}

	if err := c.replicaCopier.RevertAsyncReplicationLocally(ctx, op.Op.TargetShard.CollectionId, op.Op.SourceShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while reverting async replication locally")
	}
	if err := c.replicaCopier.RemoveAsyncReplicationTargetNode(ctx, targetNodeOverride); err != nil {
		logger.WithError(err).Error("failure while removing async replication target node")
	}
	// sync the replica shard to ensure that the schema and store are consistent on each node
	// In a COPY this happens in the FINALIZING state, in a MOVE this happens now
	if err := c.sync(ctx, op); err != nil {
		logger.WithError(err).Error("failure while syncing replica shard")
		return api.ShardReplicationState(""), err
	}
	return api.READY, nil
}

func (c *CopyOpConsumer) processCancelledOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger.Logger, op.Op, op.Status)
	logger.Info("processing cancelled replication operation")

	if !op.Status.ShouldDelete {
		return api.ShardReplicationState(""), fmt.Errorf("replication operation with id %v is not in a state to be deleted", op.Op.ID)
	}

	// Cancel async replication and revert state if any
	targetNodeOverride := additional.AsyncReplicationTargetNodeOverride{
		CollectionID:   op.Op.SourceShard.CollectionId,
		ShardID:        op.Op.TargetShard.ShardId,
		TargetNode:     op.Op.TargetShard.NodeId,
		SourceNode:     op.Op.SourceShard.NodeId,
		UpperTimeBound: time.Now().UnixMilli(),
	}
	if err := c.replicaCopier.RemoveAsyncReplicationTargetNode(ctx, targetNodeOverride); err != nil {
		logger.WithError(err).Error("failure while removing async replication target node")
	}

	if err := c.replicaCopier.RevertAsyncReplicationLocally(ctx, op.Op.TargetShard.CollectionId, op.Op.SourceShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while reverting async replication locally")
	}

	if err := c.leaderClient.ReplicationRemoveReplicaOp(op.Op.ID); err != nil {
		logger.WithError(err).Error("failure while removing replica operation")
		return api.ShardReplicationState(""), err
	}
	return DELETED, nil
}

// waitForAsyncReplication waits for async replication to complete by checking the status of the async
// replication every `asyncStatusInterval` seconds.
// It returns an error if the async replication does not complete within `asyncStatusRetries` attempts.
// It returns nil if the async replication has completed.
func (c *CopyOpConsumer) waitForAsyncReplication(
	ctx context.Context,
	op ShardReplicationOpAndStatus,
	asyncReplicationUpperTimeBoundUnixMillis int64,
	logger *logrus.Entry,
) error {
	remainingErrorsAllowed := asyncStatusMaxErrors
	retryNum := -1
	return backoff.Retry(func() error {
		retryNum++
		asyncReplStatus, err := c.replicaCopier.AsyncReplicationStatus(
			ctx,
			op.Op.SourceShard.NodeId,
			op.Op.TargetShard.NodeId,
			op.Op.SourceShard.CollectionId,
			op.Op.SourceShard.ShardId,
		)
		asyncReplIsPastUpperTimeBound := asyncReplStatus.StartDiffTimeUnixMillis >= asyncReplicationUpperTimeBoundUnixMillis
		if err != nil {
			remainingErrorsAllowed--
			if remainingErrorsAllowed < 0 {
				// If we see this error, it means that something probably went wrong with
				// initializing the async replication on the source/target nodes.
				logger.WithFields(logrus.Fields{"num_errors": asyncStatusMaxErrors, "num_retries": retryNum}).WithError(err).Error("errored on all attempts to get async replication status")
				return backoff.Permanent(err)
			}
			// We expect to see this warning a few times while the hashtree's are being initialized
			// on the source/target nodes, but if this errors for longer than ~asyncStatusRetries * asyncStatusInterval
			// then either the hashtree is taking forever to init or something has gone wrong
			logger.WithFields(logrus.Fields{"num_errors_allowed": asyncStatusMaxErrors, "num_errors_left": remainingErrorsAllowed, "num_retries_so_far": retryNum}).WithError(err).Warn("errored when getting async replication status, hashtrees may still be initializing, retrying")
			return err
		}

		// It can take a few minutes for async replication to complete, this log is here to
		// help monitor the progress.
		logger.WithFields(logrus.Fields{
			"objects_propagated":                      asyncReplStatus.ObjectsPropagated,
			"start_diff_time_unix_millis":             asyncReplStatus.StartDiffTimeUnixMillis,
			"upper_time_bound_unix_millis":            asyncReplicationUpperTimeBoundUnixMillis,
			"async_replication_past_upper_time_bound": asyncReplIsPastUpperTimeBound,
			"num_retries_so_far":                      retryNum,
			"remaining_errors_allowed":                remainingErrorsAllowed,
		}).Info("async replication status")
		if asyncReplStatus.ObjectsPropagated == 0 && asyncReplIsPastUpperTimeBound {
			return nil
		}

		return errors.New("async replication not done")
	}, backoff.WithContext(
		backoff.WithMaxRetries(backoff.NewConstantBackOff(asyncStatusInterval), asyncStatusMaxRetries),
		ctx),
	)
}
