//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/cluster/replication/metrics"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
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

// finalizeAndTail seals the source CCL while an uncapped tailer drains it
// onto target. Idempotent on retry: "log gone" from either RPC means
// already-drained.
func (c *CopyOpConsumer) finalizeAndTail(ctx context.Context, logger *logrus.Entry, src, coll, shard, opID string) error {
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()

	tailErrCh := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := c.replicaCopier.TailAndApply(tailCtx, src, coll, shard, opID, math.MaxUint64)
		if isCCLAlreadyGone(err) {
			err = nil
		}
		tailErrCh <- err
	}, c.logger)

	if _, err := c.replicaCopier.FinalizeChangeLog(ctx, src, coll, shard, opID); err != nil {
		if !isCCLAlreadyGone(err) {
			cancelTail()
			<-tailErrCh
			logger.WithError(err).Error("failure while finalizing change log")
			return err
		}
		logger.WithError(err).Info("FinalizeChangeLog: log already gone, treating as already finalized")
	}
	if err := <-tailErrCh; err != nil {
		logger.WithError(err).Error("failure while draining change log to final LSN")
		return err
	}
	if err := c.replicaCopier.StopChangeCapture(ctx, src, coll, shard, opID); err != nil {
		logger.WithError(err).Warn("StopChangeCapture failed (non-fatal)")
	}
	return nil
}

// isCCLAlreadyGone matches the source's post-StopChangeCapture "log gone"
// signal — the already-drained marker on retry.
func isCCLAlreadyGone(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, changelog.ErrMsgNoActiveLog) ||
		strings.Contains(msg, changelog.ErrMsgNoActiveChangeCaptureLog)
}

// errOpCancelled is an error indicating that the operation was cancelled.
var errOpCancelled = errors.New("operation cancelled")

// waitForAllNodesAtLeast blocks until every node has reported PerNodeState[peer] >= target
func (c *CopyOpConsumer) waitForAllNodesAtLeast(
	ctx context.Context, opID uint64, target api.ShardReplicationState,
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := c.leaderClient.ReplicationAllPeersAtLeast(opID, target)
		if err != nil {
			return fmt.Errorf("failed to check replication state of peers: %w", err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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

	// opsGateway is used to keep track of when task were executed and when we can retry or continue their execution next
	// It is used to ensure we backoff when retrying ops and avoid thundering herd problems
	opsGateway *OpsGateway

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
	schemaReader schema.SchemaReader,
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
		schemaReader:      schemaReader,
		opsGateway:        NewOpsGateway(),
	}
	return c
}

// Consume processes replication operations from the input channel, ensuring that only a limited number of consumers
// are active concurrently based on the maxWorkers value.
func (c *CopyOpConsumer) Consume(workerCtx context.Context, in <-chan ShardReplicationOpAndStatus) error {
	c.logger.WithFields(logrus.Fields{"node": c.nodeId, "max_workers": c.maxWorkers, "op_timeout": c.opTimeout}).Info("starting replication operation consumer")

	c.engineOpCallbacks.OnPrepareProcessing(c.nodeId)

	var wg sync.WaitGroup
	for {
		select {
		case <-workerCtx.Done():
			c.logger.WithError(workerCtx.Err()).Info("worker context canceled, shutting down consumer")
			// We can start waiting for ops because their context depend on the worker context that just got cancelled
			wg.Wait()
			return workerCtx.Err()

		case op, ok := <-in:
			if !ok {
				c.logger.Info("operation channel closed, shutting down consumer and waiting for ops to finish")
				c.ongoingOps.CancelAll()
				wg.Wait()
				return nil
			}
			logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)

			// If the operation has been scheduled for cancellation or deletion
			// This is done outside of the worker goroutine, and therefore without acquiring a token, so that
			// we can cancel operations that have frozen or become unresponsive. If we were to acquire a token
			// we would block the worker pool and not be able to cancel the operation leading to resource starvation.
			if op.Status.ShouldCancel && !c.ongoingOps.HasBeenCancelled(op.Op.ID) {
				// Update the cache to mark the operation as cancelled
				c.ongoingOps.StoreHasBeenCancelled(op.Op.ID)
				logger.Debug("cancelled the replication op")
				if c.ongoingOps.InFlight(op.Op.ID) {
					// Cancel the in-flight operation
					// Is a noop, returns false if the op doesn't exist
					c.ongoingOps.Cancel(op.Op.ID)
					// Continue to ensure we don't accidentally re-spawn the operation in a new worker
					continue
				}
				// Otherwise, the operation is not in-flight and should therefore be processed in a worker where clean-up happens
			}

			if ok, next := c.opsGateway.CanSchedule(op.Op.ID); !ok {
				logger.WithFields(logrus.Fields{"next": next}).Debug("replication op skipped as not ready to schedule")
				continue
			}

			c.engineOpCallbacks.OnOpPending(c.nodeId)
			select {
			// If main context is cancelled here we just continue so that we hit the shutdown logic on the next iteration
			case <-workerCtx.Done():
				continue
			// The 'tokens' channel limits the number of concurrent workers (`maxWorkers`).
			// Each worker acquires a token before processing an operation. If no tokens are available,
			// the worker blocks until one is released. After completing the task, the worker releases the token,
			// allowing another worker to proceed. This ensures only a limited number of workers is concurrently
			// running replication operations and avoids overloading the system.
			case c.tokens <- struct{}{}:
				// Here we capture the op argument used by the func below as the enterrors.GoWrapper requires calling
				// a function without arguments.
				operation := op
				opLogger := getLoggerForOpAndStatus(c.logger, operation.Op, op.Status)
				shouldSkip := false
				opAlreadyInFlight := c.ongoingOps.LoadOrStore(op.Op.ID)
				if opAlreadyInFlight {
					// Check if the operation is already in progress
					// Avoid scheduling unnecessary work or incorrectly counting metrics
					// for operations that are already in progress or completed.
					c.logger.Debug("replication op skipped as already running")
					shouldSkip = true
				} else {
					// Check if the operation has had its state changed between being added to the channel and being processed
					// This is chatty and will likely cause a lot of unnecessary load on the leader
					// For now, we need it to ensure eventual consistency between the FSM and the consumer
					state, err := c.leaderClient.ReplicationGetReplicaOpStatus(workerCtx, op.Op.ID)
					if err != nil {
						c.logger.Error("error while checking status of replication op")
						shouldSkip = true
					} else if state.String() != op.Status.GetCurrent().State.String() {
						c.logger.Debug("replication op skipped as state has changed")
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
					opLogger.Debug("replication op skipped as already running")
					// Need to release the token to let other consumers process queued replication operations.
					<-c.tokens
					c.engineOpCallbacks.OnOpSkipped(c.nodeId)
					if !opAlreadyInFlight {
						c.ongoingOps.DeleteInFlight(op.Op.ID)
					}
					continue
				}

				// Start a replication operation with a timeout for completion to prevent replication operations
				// from running indefinitely
				opCtx, opCancel := context.WithTimeout(workerCtx, c.opTimeout)
				c.engineOpCallbacks.OnOpStart(c.nodeId)
				c.ongoingOps.StoreCancel(op.Op.ID, opCancel)
				c.opsGateway.ScheduleNow(op.Op.ID)
				wg.Add(1)
				enterrors.GoWrapper(func() {
					defer func() {
						<-c.tokens // Release token when completed
						// Delete the operation from the ongoingOps map when the operation processing is complete
						c.ongoingOps.DeleteInFlight(op.Op.ID)
						wg.Done() //nolint:SA2000
						opCancel()
					}()

					// If the operation has been cancelled in the time between it being added to the channel and
					// being processed, we need to cancel it in the FSM and return
					if c.ongoingOps.HasBeenCancelled(op.Op.ID) {
						c.logger.Info("replication op cancelled, stopping replication operation")
						c.cancelOp(operation, opLogger)
						return
					}

					opLogger.Debug("worker processing replication operation")
					err := c.dispatchReplicationOp(opCtx, operation)
					if err == nil {
						opLogger.Debug("worker completed replication operation")
						c.opsGateway.RegisterFinished(op.Op.ID)
						c.engineOpCallbacks.OnOpComplete(c.nodeId)
						return
					}

					c.opsGateway.RegisterFailure(op.Op.ID)
					if errors.Is(err, context.DeadlineExceeded) {
						c.engineOpCallbacks.OnOpFailed(c.nodeId)
						opLogger.WithError(err).Error("replication operation timed out")
						return
					}
					// TODO: Refactor this error handling
					if errors.Is(err, context.Canceled) && c.ongoingOps.HasBeenCancelled(op.Op.ID) {
						opLogger.WithError(err).Info("replication operation cancelled")
						c.cancelOp(operation, opLogger)
						return
					}
					if errors.Is(err, errOpCancelled) {
						opLogger.WithError(err).Info("replication operation cancelled")
						c.cancelOp(operation, opLogger)
						return
					}
					c.engineOpCallbacks.OnOpFailed(c.nodeId)
					opLogger.WithError(err).Error("replication operation failed")
				}, c.logger)
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
	case api.INTEGRATING:
		return c.processStateAndTransition(ctx, op, c.processIntegratingOp)
	case api.READY:
		return nil
	case api.CANCELLED:
		return c.processStateAndTransition(ctx, op, c.processCancelledOp)
	default:
		getLoggerForOpAndStatus(c.logger, op.Op, op.Status).Error("unknown replication operation state")
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
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
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
			if err := c.leaderClient.ReplicationRegisterError(ctx, op.Op.ID, err.Error()); err != nil {
				logger.WithError(err).Error("failed to register error for replication operation")
			}
			return api.ShardReplicationState(""), err
		}

		if err := c.checkCancelled(logger, op); err != nil {
			return api.ShardReplicationState(""), backoff.Permanent(fmt.Errorf("error while checking if op is cancelled: %w", err))
		}
		if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(ctx, op.Op.ID, nextState); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // Ensure sync shards timesout reasonbly in case of hang
	defer cancel()

	// Safe for ops that never reached HYDRATING: idempotent for unknown opIDs.
	if err := c.replicaCopier.StopChangeCapture(ctx, op.Op.SourceShard.NodeId,
		op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, strconv.FormatUint(op.Op.ID, 10)); err != nil {
		logger.WithError(err).Warn("StopChangeCapture failed during cancel (non-fatal)")
	}

	// Covers the case where CopyReplicaFiles' defer never registered — a
	// leaked staging dir would otherwise pin compaction.
	if err := c.replicaCopier.ReleaseReplicaSnapshot(ctx, op.Op.SourceShard.NodeId,
		op.Op.SourceShard.CollectionId, strconv.FormatUint(op.Op.ID, 10)); err != nil {
		logger.WithError(err).Warn("ReleaseReplicaSnapshot failed during cancel (non-fatal)")
	}

	// Ensure that the states of the shards on the nodes are in-sync with the state of the schema through a RAFT communication
	// This handles cleaning up for ghost shards that are in the store but not in the schema that may have been created by index.getOptInitShard
	if err := c.sync(ctx, op); err != nil {
		logger.WithError(err).
			WithField("op", op).
			Error(fmt.Errorf("failure while syncing replica shard when cancelling the op"))
	}

	// If the operation is only being cancelled then notify the FSM so it can update its state
	if op.Status.OnlyCancellation() {
		if err := c.leaderClient.ReplicationCancellationComplete(ctx, op.Op.ID); err != nil {
			logger.WithError(err).Error("failure while completing cancellation of replica operation")
		}
		return
	}

	// If the operation is being deleted then remove it from the FSM
	if op.Status.ShouldDelete {
		if err := c.leaderClient.ReplicationRemoveReplicaOp(ctx, op.Op.ID); err != nil {
			logger.WithError(err).Error("failure while deleting replica operation")
		}
		return
	}
}

func (c *CopyOpConsumer) sync(ctx context.Context, op ShardReplicationOpAndStatus) error {
	if _, err := c.leaderClient.SyncShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId); err != nil {
		return err
	}
	if _, err := c.leaderClient.SyncShard(ctx, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, op.Op.SourceShard.NodeId); err != nil {
		return err
	}
	return nil
}

// processRegisteredOp is the state handler for the REGISTERED state.
func (c *CopyOpConsumer) processRegisteredOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing registered replication operation")

	return api.HYDRATING, nil
}

// processHydratingOp is the state handler for the HYDRATING state. On error
// after a successful StartChangeCapture it tears the source log down so the
// retry starts from a clean file slate.
func (c *CopyOpConsumer) processHydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (nextState api.ShardReplicationState, retErr error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing hydrating replication operation")

	// For multi-tenant collections, we need to activate the tenant and store the resulting schema version.
	// The schema version ensures schema consistency across nodes during replication.
	// For non-multi-tenant collections, op.Status.SchemaVersion remains at its initial value (typically 0),
	// which is acceptable as schema version synchronization is not required.
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

		if err := c.leaderClient.ReplicationStoreSchemaVersion(ctx, op.Op.ID, schemaVersion); err != nil {
			logger.WithError(err).Error("failure while storing schema version for replication operation")
			return api.ShardReplicationState(""), err
		}

		if err := c.leaderClient.WaitForUpdate(ctx, schemaVersion); err != nil {
			logger.WithError(err).Error("failure while waiting for schema version to be applied to local node")
			return api.ShardReplicationState(""), err
		}
		// Update the local operation status with the stored schema version so it's available
		// when processFinalizingOp is called in the next state transition
		if schemaVersion > op.Status.SchemaVersion {
			op.Status.SchemaVersion = schemaVersion
		}
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	opID := strconv.FormatUint(op.Op.ID, 10)
	src := op.Op.SourceShard.NodeId
	coll := op.Op.SourceShard.CollectionId
	shard := op.Op.SourceShard.ShardId

	// Must precede CopyReplicaFiles: every write after this point lands in
	// the log and is replayed during FINALIZING/DEHYDRATING.
	if err := c.replicaCopier.StartChangeCapture(ctx, src, coll, shard, opID, op.Status.SchemaVersion); err != nil {
		logger.WithError(err).Error("failure while starting change capture")
		return api.ShardReplicationState(""), err
	}
	defer func() {
		if retErr == nil {
			return
		}
		// Bounded background ctx — the worker ctx is already cancelled by the
		// time we get here. If Stop fails, the orphan-sweep in
		// Shard.ActivateChangeLog cleans up before the next retry's Start.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.replicaCopier.StopChangeCapture(cleanupCtx, src, coll, shard, opID); err != nil {
			logger.WithError(err).Warn("StopChangeCapture failed during HYDRATING cleanup (non-fatal)")
		}
	}()

	if err := c.replicaCopier.CopyReplicaFiles(ctx, op.Op.UUID, op.Op.SourceShard.NodeId, op.Op.SourceShard.CollectionId, op.Op.TargetShard.ShardId, op.Status.SchemaVersion); err != nil {
		logger.WithError(err).Error("failure while copying replica shard")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	return api.FINALIZING, nil
}

// processFinalizingOp is the state handler for the FINALIZING state. The
// cap'd drain runs before RAFT-add so a consistency=ONE read routed to the
// target right after it joins the sharding state cannot observe a state older
// than the FINALIZING phase boundary.
func (c *CopyOpConsumer) processFinalizingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing finalizing replication operation")

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.leaderClient.WaitForUpdate(ctx, op.Status.SchemaVersion); err != nil {
		logger.WithError(err).Error("failure while waiting for schema version to be applied to local node")
		return api.ShardReplicationState(""), err
	}

	// Must precede the cap'd drain: replay writes to the local target shard.
	if err := c.replicaCopier.LoadLocalShard(ctx, op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId); err != nil {
		logger.WithError(err).Error("failure while loading shard")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	// Sanity check: directly query the local schema to see if the replica already exists.
	// If it does we are probably recoving from a previous failure and can skip adding the replica to the sharding state again
	nodes, err := c.schemaReader.ShardReplicas(op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId)
	if err != nil {
		logger.WithError(err).Error("failure while getting shard replicas")
		return api.ShardReplicationState(""), err
	}
	replicaExists := slices.Contains(nodes, op.Op.TargetShard.NodeId)

	opID := strconv.FormatUint(op.Op.ID, 10)
	src := op.Op.SourceShard.NodeId
	coll := op.Op.SourceShard.CollectionId
	shard := op.Op.SourceShard.ShardId

	snap, err := c.replicaCopier.SnapshotChangeLogLSN(ctx, src, coll, shard, opID)
	if err != nil {
		logger.WithError(err).Error("failure while snapshotting change log LSN")
		return api.ShardReplicationState(""), err
	}

	if _, err := c.replicaCopier.TailAndApply(ctx, src, coll, shard, opID, snap); err != nil {
		logger.WithError(err).Error("failure while draining change log up to snapshot LSN")
		return api.ShardReplicationState(""), err
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("error while processing replication operation, shutting down")
		return api.ShardReplicationState(""), ctx.Err()
	}

	// RAFT-add only after the target is caught up to snap. CCL stays alive;
	// the seal happens in INTEGRATING after the transition converges.
	if !replicaExists {
		if _, err := c.leaderClient.ReplicationAddReplicaToShard(ctx, op.Op.TargetShard.CollectionId, op.Op.TargetShard.ShardId, op.Op.TargetShard.NodeId, op.Op.ID); err != nil {
			if strings.Contains(err.Error(), sharding.ErrReplicaAlreadyExists.Error()) {
				// The replica already exists, this is not an error and it got updated after our sanity check
				// due to eventual consistency of the sharding state.
				logger.Debug("replica already exists, skipping")
			} else {
				logger.WithError(err).Error("failure while adding replica to shard")
				return api.ShardReplicationState(""), err
			}
		}
	}

	return api.INTEGRATING, nil
}

// processIntegratingOp is the shared seal+drain phase for COPY and MOVE.
// It waits for INTEGRATING to converge across every node — making the target
// a counted write replica everywhere — then does a capped drain and seals the
// source CCL. Idempotent: re-running after a prior seal sees "log gone" and
// finishes via sync.
//
// Returns READY for COPY (source stays), DEHYDRATING for MOVE (source removed
// next by processDehydratingOp).
func (c *CopyOpConsumer) processIntegratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing integrating replication operation")

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("context cancelled, stopping replication operation")
		return api.ShardReplicationState(""), ctx.Err()
	}

	opID := strconv.FormatUint(op.Op.ID, 10)
	src := op.Op.SourceShard.NodeId
	coll := op.Op.SourceShard.CollectionId
	shard := op.Op.SourceShard.ShardId

	// Wait for INTEGRATING to converge — until then the target is a counted
	// write replica only where the transition has applied.
	if err := c.waitForAllNodesAtLeast(ctx, op.Op.ID, api.INTEGRATING); err != nil {
		logger.WithError(err).Error("failure waiting for INTEGRATING op-state to converge across nodes")
		return api.ShardReplicationState(""), err
	}

	// Capped drain shrinks the gap before the final seal. "Log gone" on retry
	// means a prior attempt already sealed; sync and finish.
	snap, err := c.replicaCopier.SnapshotChangeLogLSN(ctx, src, coll, shard, opID)
	if err != nil {
		if isCCLAlreadyGone(err) {
			logger.Info("change log already sealed, integration already complete")
			if err := c.sync(ctx, op); err != nil {
				logger.WithError(err).Error("failure while syncing replica shard in integrating state")
				return api.ShardReplicationState(""), err
			}
			return nextStateAfterIntegrating(op.Op.TransferType), nil
		}
		logger.WithError(err).Error("failure while snapshotting change log LSN")
		return api.ShardReplicationState(""), err
	}

	if _, err := c.replicaCopier.TailAndApply(ctx, src, coll, shard, opID, snap); err != nil {
		if !isCCLAlreadyGone(err) {
			logger.WithError(err).Error("failure while draining change log up to snapshot LSN")
			return api.ShardReplicationState(""), err
		}
	}

	if ctx.Err() != nil {
		logger.WithError(ctx.Err()).Debug("error while processing replication operation, shutting down")
		return api.ShardReplicationState(""), ctx.Err()
	}

	if err := c.finalizeAndTail(ctx, logger, src, coll, shard, opID); err != nil {
		return api.ShardReplicationState(""), err
	}

	if err := c.sync(ctx, op); err != nil {
		logger.WithError(err).Error("failure while syncing replica shard in integrating state")
		return api.ShardReplicationState(""), err
	}
	return nextStateAfterIntegrating(op.Op.TransferType), nil
}

func nextStateAfterIntegrating(tt api.ShardReplicationTransferType) api.ShardReplicationState {
	if tt == api.MOVE {
		return api.DEHYDRATING
	}
	return api.READY
}

// processDehydratingOp is the MOVE-only post-seal handler: wait for
// DEHYDRATING to converge (so the source-side fence is rejecting stale writes
// everywhere), then remove the source from the sharding state. Retry-safe via
// the "still in replica set" guard.
func (c *CopyOpConsumer) processDehydratingOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing dehydrating replication operation")

	if err := c.leaderClient.WaitForUpdate(ctx, op.Status.SchemaVersion); err != nil {
		logger.WithError(err).Error("failure while waiting for schema version to be applied to local node")
		return api.ShardReplicationState(""), err
	}

	nodes, err := c.schemaReader.ShardReplicas(op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId)
	if err != nil {
		logger.WithError(err).Error("failure while getting shard replicas")
		return api.ShardReplicationState(""), err
	}

	src := op.Op.SourceShard.NodeId
	coll := op.Op.SourceShard.CollectionId
	shard := op.Op.SourceShard.ShardId

	if slices.Contains(nodes, op.Op.SourceShard.NodeId) {
		// Wait for DEHYDRATING to converge so the source-side fence is active
		// everywhere before we remove the source.
		if err := c.waitForAllNodesAtLeast(ctx, op.Op.ID, api.DEHYDRATING); err != nil {
			logger.WithError(err).Error("failure waiting for DEHYDRATING op-state to converge across nodes")
			return api.ShardReplicationState(""), err
		}

		if _, err := c.leaderClient.DeleteReplicaFromShard(ctx, coll, shard, src); err != nil {
			logger.WithError(err).Error("failure while deleting replica from shard")
			return api.ShardReplicationState(""), err
		}
	}

	if err := c.sync(ctx, op); err != nil {
		logger.WithError(err).Error("failure while syncing replica shard in dehydrating state")
		return api.ShardReplicationState(""), err
	}
	return api.READY, nil
}

func (c *CopyOpConsumer) processCancelledOp(ctx context.Context, op ShardReplicationOpAndStatus) (api.ShardReplicationState, error) {
	logger := getLoggerForOpAndStatus(c.logger, op.Op, op.Status)
	logger.Info("processing cancelled replication operation")

	if !op.Status.ShouldDelete {
		return api.ShardReplicationState(""), fmt.Errorf("replication operation with id %v is not in a state to be deleted", op.Op.ID)
	}

	// Safe for ops that never reached HYDRATING: idempotent for unknown opIDs.
	if err := c.replicaCopier.StopChangeCapture(ctx, op.Op.SourceShard.NodeId,
		op.Op.SourceShard.CollectionId, op.Op.SourceShard.ShardId, strconv.FormatUint(op.Op.ID, 10)); err != nil {
		logger.WithError(err).Warn("StopChangeCapture failed during cancelled-op cleanup (non-fatal)")
	}

	// Same cleanup as cancelOp, for the FSM-dispatched cancel path.
	if err := c.replicaCopier.ReleaseReplicaSnapshot(ctx, op.Op.SourceShard.NodeId,
		op.Op.SourceShard.CollectionId, strconv.FormatUint(op.Op.ID, 10)); err != nil {
		logger.WithError(err).Warn("ReleaseReplicaSnapshot failed during cancelled-op cleanup (non-fatal)")
	}

	if err := c.leaderClient.ReplicationRemoveReplicaOp(ctx, op.Op.ID); err != nil {
		logger.WithError(err).Error("failure while removing replica operation")
		return api.ShardReplicationState(""), err
	}
	return DELETED, nil
}
