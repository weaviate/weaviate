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
	"errors"
	"fmt"
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
	replicationEngineLogAction = "replication_engine"
)

// ShardReplicationEngine coordinates the replication of shard data between nodes in a distributed system.
//
// It uses a producer-consumer pattern where replication operations are pulled from a source (e.g., FSM)
// and dispatched to workers for execution, enabling parallel processing with built-in backpressure implemented by means
// of a limited channel.
//
// The engine ensures that operations are processed concurrently, but with limits to avoid overloading the system. It also
// provides mechanisms for graceful shutdown and error handling. The replication engine is responsible for managing the
// lifecycle of both producer and consumer goroutines that work together to execute replication tasks.
//
// Key responsibilities of this engine include managing buffered channels for backpressure, starting and stopping
// the replication operation lifecycle, and ensuring that the engine handles concurrent workers without resource exhaustion.
//
// This engine is expected to run in a single node within a cluster, where it processes replication operations relevant
// to the node with a pull mechanism where an engine running on a certain node is responsible for running replication
// operations with that node as a target.
type ShardReplicationEngine struct {
	// nodeId uniquely identifies the node on which this engine instance is running.
	// It is used to filter replication operations that are relevant to this node, as each
	// replication engine works in pull mode and is responsible for a subset of replication
	// operations assigned to the node.
	nodeId string

	// logger provides structured logging throughout the lifecycle of the engine,
	// including producer, consumer, and worker activity. The logger tracks operations,
	// errors, and state transitions.
	logger *logrus.Entry

	// producer is responsible for generating replication operations that this node should execute.
	// These operations are typically retrieved from the cluster’s FSM stored in RAFT.
	// The producer pulls operations from the source and sends them to the opsChan for the consumer to process.
	producer OpProducer

	// consumer handles the execution of replication operations by processing them with a pool of workers.
	// It ensures bounded concurrent execution of multiple workers, performing the actual data replication.
	// The consumer listens on the opsChan and processes operations as they arrive.
	consumer OpConsumer

	// opBufferSize determines the size of the buffered channel between the producer and consumer.
	// It controls how many operations can be in-flight or waiting for processing, enabling backpressure.
	// If the channel is full, the producer will be blocked until space is available, resulting in proparagting
	// backpressure from the consumer up to the producer.
	opBufferSize int

	// opsChan is the buffered channel used to pass operations from the producer to the consumer.
	// A bounded channel ensures that backpressure is applied when the consumer is overwhelmed or when
	// a certain number of concurrent workers are already busy processing replication operations.
	opsChan chan ShardReplicationOp

	// stopChan is a signal-only channel used to trigger graceful shutdown of the engine.
	// It is closed when Stop() is invoked, prompting shutdown of producer and consumer goroutines.
	// This allows for a controlled and graceful shutdown of all active components.
	stopChan chan struct{}

	// isRunning is a flag that indicates whether the engine is currently running.
	// It prevents concurrent starts (multiple instances of the replication engine running simultaneously) or stops.
	// Ensures that the engine runs only once per each node.
	isRunning atomic.Bool

	// wg is a wait group that tracks producer and consumer goroutines.
	// It ensures graceful shutdown by waiting for all background goroutines to exit cleanly.
	// The wait group helps ensure that the engine doesn't terminate prematurely before all goroutines have finished.
	wg sync.WaitGroup

	// cancel is a function that cancels the context associated with the replication engine's main execution loop.
	// It is used to gracefully stop the operation of the engine by canceling the context passed to the producer
	// and consumer goroutines. The context cancellation triggers the shutdown sequence for the engine, allowing
	// the producer and consumer to stop gracefully.
	cancel context.CancelFunc

	// maxWorkers controls the maximum number of concurrent workers in the consumer pool.
	// It is used to limit the parallelism of replication operations, preventing the system from being overwhelmed by
	// too many concurrent tasks performing replication operations.
	maxWorkers int

	// shutdownTimeout is the maximum amount of time to wait for a graceful shutdown.
	// If the engine takes longer than this timeout to shut down, a warning is logged, and the process is forcibly stopped.
	// This ensures that the system doesn't hang indefinitely during shutdown.
	shutdownTimeout time.Duration
}

// NewShardReplicationEngine creates a new replication engine
func NewShardReplicationEngine(
	logger *logrus.Logger,
	nodeId string,
	producer OpProducer,
	consumer OpConsumer,
	opBufferSize int,
	maxWorkers int,
	shutdownTimeout time.Duration,
) *ShardReplicationEngine {
	return &ShardReplicationEngine{
		nodeId:          nodeId,
		logger:          logger.WithFields(logrus.Fields{"action": replicationEngineLogAction, "node": nodeId}),
		producer:        producer,
		consumer:        consumer,
		opBufferSize:    opBufferSize,
		maxWorkers:      maxWorkers,
		shutdownTimeout: shutdownTimeout,
		stopChan:        make(chan struct{}),
	}
}

// Start runs the replication engine's main loop, including the operation producer and consumer.
//
// It starts two goroutines: one for the OpProducer and one for the OpConsumer. These goroutines
// communicate through a buffered channel, and the engine coordinates their lifecycle. This method
// is safe to call only once; if the engine is already running, it logs a warning and returns.
//
// It returns an error if either the producer or consumer fails unexpectedly, or if the context is cancelled.
//
// It is, safe to restart the replication engin using this method, after it has been stopped.
func (e *ShardReplicationEngine) Start(ctx context.Context) error {
	if !e.isRunning.CompareAndSwap(false, true) {
		e.logger.Warnf("replication engine already running: %v", e)
		return nil
	}

	// Channels are creating while starting the replication engine to allow start/stop.
	e.opsChan = make(chan ShardReplicationOp, e.opBufferSize)
	e.stopChan = make(chan struct{})

	engineCtx, engineCancel := context.WithCancel(ctx)
	e.cancel = engineCancel
	e.logger.WithFields(logrus.Fields{"engine": e}).Info("starting replication engine")

	// Channels for error reporting used by producer and consumer.
	producerErrChan := make(chan error, 1)
	consumerErrChan := make(chan error, 1)

	// Start one replication operations producer.
	e.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer e.wg.Done()
		e.logger.WithField("producer", e.producer).Info("starting replication engine producer")
		err := e.producer.Produce(engineCtx, e.opsChan)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.WithField("producer", e.producer).WithError(err).Error("stopping producer after failure")
			producerErrChan <- err
		}
		e.logger.WithField("producer", e.producer).Info("replication engine producer stopped")
	}, e.logger)

	// Start one replication operations consumer.
	e.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer e.wg.Done()
		e.logger.WithField("consumer", e.consumer).Info("starting replication engine consumer")
		err := e.consumer.Consume(engineCtx, e.opsChan)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.WithField("consumer", e.consumer).WithError(err).Error("stopping consumer after failure")
			consumerErrChan <- err
		}
		e.logger.WithField("consumer", e.consumer).Info("replication engine consumer stopped")
	}, e.logger)

	// Coordinate replication engine execution with producer and consumer lifecycle.
	var err error
	select {
	case <-ctx.Done():
		e.logger.WithField("engine", e).Info("replication engine cancel request, shutting down")
		err = ctx.Err()
	case <-e.stopChan:
		e.logger.WithField("engine", e).Info("replication engine stop request, shutting down")
		// Graceful shutdown executed when stopping the replication engine
	case producerErr := <-producerErrChan:
		if !errors.Is(producerErr, context.Canceled) {
			e.logger.WithField("engine", e).WithError(producerErr).Error("stopping replication engine producer after failure")
			err = fmt.Errorf("replication engine producer failed with: %w", producerErr)
		}
	case consumerErr := <-consumerErrChan:
		e.logger.WithField("engine", e).WithError(consumerErr).Error("stopping replication engine consumer after failure")
		err = fmt.Errorf("replication engine consumer failed with: %w", consumerErr)
	}

	// Always cancel the replication engine context and wait for the producer and consumers to terminate to gracefully
	// shut down the replication engine the both the producer and consumer.
	engineCancel()
	e.wg.Wait()
	close(e.opsChan)
	e.isRunning.Store(false)
	return err
}

// Stop signals the replication engine to shut down gracefully.
//
// It safely transitions the engine's running state to false and closes the internal stop channel,
// which unblocks the main loop in Start() and initiates the shutdown sequence.
// Calling Stop multiple times is safe; only the first call has an effect.
// Note that the ops channel is closed in the Start method after waiting for both the producer and consumers to
// terminate.
func (e *ShardReplicationEngine) Stop() {
	if !e.isRunning.CompareAndSwap(true, false) {
		return
	}

	// Closing the stop channel notifies both the producer and consumer to shut down gracefully coordinating with the
	// replication engine.
	close(e.stopChan)
	if e.cancel != nil {
		e.cancel()
	}

	// We use a timeout mechanism to wait for the replication engine to shut down and prevent it from running
	// indefinitely.
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), e.shutdownTimeout)
	defer timeoutCancel()

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		e.wg.Wait()
		close(done)
	}, e.logger)

	select {
	case <-done:
		e.logger.WithField("engine", e).Info("replication engine shutdown completed successfully")
	case <-timeoutCtx.Done():
		e.logger.WithField("engine", e).WithField("timeout", e.shutdownTimeout).Warn("replication engine shutdown timed out")
	}
}

// IsRunning reports whether the replication engine is currently running.
//
// It returns true if the engine has been started and has not yet shut down.
func (e *ShardReplicationEngine) IsRunning() bool {
	return e.isRunning.Load()
}

// GetOpChannelCap returns the capacity of the internal operation channel.
//
// This reflects the total number of replication operations the channel can queue
// before blocking the producer implementing a backpressure mechanism.
func (e *ShardReplicationEngine) GetOpChannelCap() int {
	return cap(e.opsChan)
}

// GetOpChannelLen returns the current number of operations buffered in the internal channel.
//
// This can be used to monitor the backpressure between the producer and the consumer.
func (e *ShardReplicationEngine) GetOpChannelLen() int {
	return len(e.opsChan)
}

// String returns a string representation of the ShardReplicationEngine,
// including the node ID that uniquely identifies the engine for a specific node.
//
// The expectation is that each node runs one and only one replication engine,
// so the string output is helpful for logging or diagnostics to easily identify
// the engine associated with the node.
func (e *ShardReplicationEngine) String() string {
	return fmt.Sprintf("replication engine on node '%s'", e.nodeId)
}

// OpProducer is an interface for producing replication operations.
type OpProducer interface {
	// Produce starts producing replication operations and sends them to the provided channel.
	// A buffered channel is typically used for backpressure, but an unbounded channel may cause
	// memory growth if the consumer falls behind. Errors during production should be returned.
	Produce(ctx context.Context, out chan<- ShardReplicationOp) error
}

// OpConsumer is an interface for consuming replication operations.
type OpConsumer interface {
	// Consume starts consuming operations from the provided channel.
	// The consumer processes operations, and a buffered channel is typically used to apply backpressure.
	// The consumer should return an error if it fails to process any operation.
	Consume(ctx context.Context, in <-chan ShardReplicationOp) error
}

// FSMOpProducer is an implementation of the OpProducer interface that reads replication
// operations from a ShardReplicationFSM, which tracks the state of replication operations.
type FSMOpProducer struct {
	logger          *logrus.Entry
	fsm             *ShardReplicationFSM
	pollingInterval time.Duration
	nodeId          string
}

// String returns a string representation of the FSMOpProducer,
// including the node ID that uniquely identifies the producer.
//
// The assumption is that each node runs one and only one replication engine,
// which means there is one producer per node.
func (p *FSMOpProducer) String() string {
	return fmt.Sprintf("replication engine FSM producer on node '%s'", p.nodeId)
}

// NewFSMOpProducer creates a new FSMOpProducer instance, which periodically polls the
// ShardReplicationFSM for operations assigned to the given node and pushes them to
// a channel for consumption by the replication engine.The polling interval controls
// how often the FSM is queried for replication operations.
//
// Additional configuration can be applied using optional FSMProducerOption functions.
func NewFSMOpProducer(logger *logrus.Entry, fsm *ShardReplicationFSM, pollingInterval time.Duration, nodeId string) *FSMOpProducer {
	return &FSMOpProducer{
		logger:          logger,
		fsm:             fsm,
		pollingInterval: pollingInterval,
		nodeId:          nodeId,
	}
}

// Produce implements the OpProducer interface and starts producing operations for the given node.
func (p *FSMOpProducer) Produce(ctx context.Context, out chan<- ShardReplicationOp) error {
	p.logger.WithFields(logrus.Fields{"node": p.nodeId, "pollingInterval": p.pollingInterval}).Info("starting replication engine FSM producer")

	ticker := time.NewTicker(p.pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.WithFields(logrus.Fields{
				"producer": p,
			}).Info("replication engine producer cancel request, stopping FSM producer")
			return ctx.Err()
		case <-ticker.C:
			// Here we get ALL operations for a certain node which the replication engine is responsible for running.
			// We write all of them to a channel for the OpConsumer to consume them, but it is responsibility of the
			// consumer keeping track of ongoing operations and avoid multiple copies of the same operation are executed.
			ops := p.allOpsForNode(p.nodeId)
			if len(ops) > 0 {
				p.logger.WithField("opCount", len(ops)).Debug("preparing op replication")

				for _, op := range ops {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- op: // Write operation to channel
					}
				}
			}
		}
	}
}

// allOpsForNode filters and returns replication operations assigned to the specified node.
//
// This method implements the core of the pull-based replication mechanism:
//
// 1. Pull Model: Each node is responsible for pulling data TO itself FROM other nodes
//
// 2. Node Responsibility:
//   - Target node: Handles all replication operations which are in REGISTERED or HYDRATING
//   - Source node: Only handles DEHYDRATING operations as that state needs data to be deleted
//
// 3. Operation States:
//   - REGISTERED: Initial state, operation waiting to start
//   - HYDRATING: Operation in progress, target node is pulling data
//   - DEHYDRATING: The only state handled by source node, for cleanup after successful replication
//   - **all other states**: Not reprocessed, require a new operation
//
// Returns only operations that should be actively processed by this node.
func (p *FSMOpProducer) allOpsForNode(nodeId string) []ShardReplicationOp {
	allNodeOps := p.fsm.GetOpsForNode(nodeId)

	nodeOpsSubset := make([]ShardReplicationOp, 0, len(allNodeOps))
	for _, op := range allNodeOps {
		opState := p.fsm.GetOpState(op)

		if opState.ShouldRestartOp() {
			nodeOpsSubset = append(nodeOpsSubset, ShardReplicationOp{
				ID: op.ID,
				sourceShard: shardFQDN{
					nodeId:       op.sourceShard.nodeId,
					collectionId: op.sourceShard.collectionId,
					shardId:      op.sourceShard.shardId,
				},
				targetShard: shardFQDN{
					nodeId:       op.targetShard.nodeId,
					collectionId: op.targetShard.collectionId,
					shardId:      op.targetShard.shardId,
				},
			})
		}
	}

	return nodeOpsSubset
}

// TimeProvider abstracts time operations to enable testing without time dependencies.
type TimeProvider interface {
	Now() time.Time
}

// RealTimeProvider implements the TimeProvider interface using the standard time package
type RealTimeProvider struct{}

// Now returns the current time
func (p RealTimeProvider) Now() time.Time {
	return time.Now()
}

// RealTimer implements the Timer interface using the standard time package
type RealTimer struct{}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine
func (t RealTimer) AfterFunc(d time.Duration, f func()) *time.Timer {
	return time.AfterFunc(d, f)
}

// OpRetentionPolicy defines the interface for retention policies
// that manage the lifecycle of completed operations in the consumer.
// Retention policies are responsible for scheduling the cleanup of operations
// once they are no longer needed and support implementation of a deduplication
// mechanism that allows the replication engine to track already running or completed
// replication operations
type OpRetentionPolicy interface {
	// ScheduleCleanUp schedules the removal of a completed operation after a certain period of time.
	// It takes the operation ID as an argument.
	ScheduleCleanUp(opId uint64)
}

// OpCleanUpCallback extends OpRetentionPolicy with the ability to register a callback handler
// that will be invoked when an operation is scheduled for cleanup.
type OpCleanUpCallback interface {
	// RegisterOpCleanUpCallback allows a handler to be registered that will be called when an operation
	// is ready for cleanup.
	RegisterOpCleanUpCallback(handler OpCleanupHandler)
}

// Timer defines an interface for scheduling tasks with a delay.
type Timer interface {
	// AfterFunc waits for the specified duration to elapse and then calls the provided function
	// in its own goroutine.
	AfterFunc(duration time.Duration, fn func()) *time.Timer
}

// TTLOpRetentionPolicy is a retention policy that discards completed operations after a fixed TTL (time-to-live).
// Once the TTL has passed the cleanup handler is invoked.
type TTLOpRetentionPolicy struct {
	ttl            time.Duration
	timer          Timer
	cleanUpHandler OpCleanupHandler
}

// OpCleanupHandler is a function type that is used to handle the cleanup of an operation once it's ready for removal.
type OpCleanupHandler func(opId uint64)

// NewTTLOpRetentionPolicy creates a new TTL-based operation retention policy.
// The policy uses the provided TTL to define how long completed operations are retained
// before they are discarded and the cleanup handler is invoked.
func NewTTLOpRetentionPolicy(ttl time.Duration, timer Timer) *TTLOpRetentionPolicy {
	return &TTLOpRetentionPolicy{
		ttl:   ttl,
		timer: timer,
	}
}

// RegisterOpCleanUpCallback sets the callback function that will be invoked
// when an operation is ready for cleanup after its TTL has expired.
func (p *TTLOpRetentionPolicy) RegisterOpCleanUpCallback(opCleanUpHandler OpCleanupHandler) {
	p.cleanUpHandler = opCleanUpHandler
}

// ScheduleCleanUp schedules the removal of a completed operation after the TTL has passed.
// The operation is discarded, and the cleanup handler is called to handle the operation removal.
func (p *TTLOpRetentionPolicy) ScheduleCleanUp(opId uint64) {
	if p.cleanUpHandler == nil {
		return
	}
	// After the TTL has expired, the cleanup handler is invoked to remove the operation.
	p.timer.AfterFunc(p.ttl, func() {
		p.cleanUpHandler(opId)
	})
}

// CopyOpConsumer is an implementation of the OpConsumer interface that processes replication operations
// by executing copy operations from a source shard to a target shard. It uses a ReplicaCopier to actually
// carry out the copy operation. Moreover, it supports configurable backoff, timeout, concurrency limits, and
// operation retention to track duplicates for long enough.
type CopyOpConsumer struct {
	// logger is used for structured logging throughout the consumer's lifecycle.
	// It provides detailed logs for each replication operation and any errors encountered.
	logger *logrus.Entry

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

	// opTracker is responsible for tracking the state of ongoing replication operations.
	// It ensures that duplicate operations are not processed concurrently or more than once.
	opTracker *OpTracker

	// timeProvider abstracts time operations, allowing for easier testing and mocking of time-related functions.
	timeProvider TimeProvider

	// tokens controls the maximum number of concurrently running consumers
	tokens chan struct{}

	// opRetentionPolicy defines the policy for cleaning up completed replication operations.
	// It is used to manage the lifecycle of completed operations, ensuring they are eventually discarded after a specified retention period.
	opRetentionPolicy OpRetentionPolicy

	// nodeId uniquely identifies the node on which this consumer instance is running.
	nodeId string
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
//
// If no retention policy is set, a default TTL-based policy will be used.
func NewCopyOpConsumer(
	logger *logrus.Entry,
	leaderClient types.FSMUpdater,
	replicaCopier types.ReplicaCopier,
	timeProvider TimeProvider,
	nodeId string,
	opTracker *OpTracker,
	backoffPolicy backoff.BackOff,
	retentionPolicy *TTLOpRetentionPolicy,
	opTimeout time.Duration,
	maxWorkers int,
) *CopyOpConsumer {
	c := &CopyOpConsumer{
		logger:            logger,
		leaderClient:      leaderClient,
		replicaCopier:     replicaCopier,
		backoffPolicy:     backoffPolicy,
		opTimeout:         opTimeout,
		maxWorkers:        maxWorkers,
		nodeId:            nodeId,
		opTracker:         opTracker,
		timeProvider:      timeProvider,
		opRetentionPolicy: retentionPolicy,
		tokens:            make(chan struct{}, maxWorkers),
	}
	retentionPolicy.RegisterOpCleanUpCallback(c.opTracker.CleanUpOp)
	return c
}

// Consume processes replication operations from the input channel, ensuring that only a limited number of consumers
// are active concurrently based on the maxWorkers value.
func (c *CopyOpConsumer) Consume(ctx context.Context, in <-chan ShardReplicationOp) error {
	c.logger.WithFields(logrus.Fields{
		"node":    c.nodeId,
		"workers": c.maxWorkers,
	}).Info("starting replication operation consumer")

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			c.logger.WithField("reason", ctx.Err()).Info("context canceled, shutting down consumer")
			wg.Wait() // Waiting for pending operations before terminating
			return ctx.Err()

		case op, ok := <-in:
			if !ok {
				c.logger.Info("operation channel closed, shutting down consumer")
				wg.Wait() // Waiting for pending operations before terminating
				return nil
			}

			select {
			// The 'tokens' channel limits the number of concurrent workers (`maxWorkers`).
			// Each worker acquires a token before processing an operation. If no tokens are available,
			// the worker blocks until one is released. After completing the task, the worker releases the token,
			// allowing another worker to proceed. This ensures only a limited number of workers is concurrently
			// running replication operations and avoids overloading the system.
			case c.tokens <- struct{}{}:
				if c.opTracker.IsOpInProgress(op.ID) || c.opTracker.IsOpCompleted(op.ID) {
					c.logger.WithFields(logrus.Fields{
						"op":                op.ID,
						"source_node":       op.sourceShard.nodeId,
						"target_node":       op.targetShard.nodeId,
						"source_shard":      op.sourceShard.shardId,
						"target_shard":      op.targetShard.shardId,
						"source_collection": op.sourceShard.collectionId,
						"target_collection": op.targetShard.collectionId,
					}).Debug("operation in progress or already completed")
					<-c.tokens // Release token if operation is already in progress or completed.
					continue
				}

				c.opTracker.AddOp(op.ID)

				wg.Add(1)

				// Here we capture the op argument used by the func below as the enterrors.GoWrapper requires calling
				// a function without arguments.
				operation := op

				enterrors.GoWrapper(func() {
					defer func() {
						<-c.tokens // Release token when completed
						wg.Done()
					}()

					opLogger := c.logger.WithFields(logrus.Fields{
						"op":                operation.ID,
						"source_node":       operation.sourceShard.nodeId,
						"target_node":       operation.targetShard.nodeId,
						"source_shard":      operation.sourceShard.shardId,
						"target_shard":      operation.targetShard.shardId,
						"source_collection": operation.sourceShard.collectionId,
						"target_collection": operation.targetShard.collectionId,
					})

					opLogger.Info("worker processing replication operation")

					// Start a replication operation with a timeout for completion to prevent replication operations
					// from running indefinitely
					opCtx, opCancel := context.WithTimeout(workerCtx, c.opTimeout)
					defer opCancel()

					err := c.processReplicationOp(opCtx, operation.ID, operation)
					if err != nil && errors.Is(err, context.DeadlineExceeded) {
						opLogger.WithError(err).Error("replication operation timed out")
						// Remove the operation from tracking to allow retry
						c.opTracker.CleanUpOp(operation.ID)
					} else if err != nil {
						opLogger.WithError(err).Error("replication operation failed")
						// Remove the operation from tracking to allow retry
						c.opTracker.CleanUpOp(operation.ID)
					} else {
						c.opTracker.CompleteOp(operation.ID)
						c.opRetentionPolicy.ScheduleCleanUp(operation.ID)
					}
				}, c.logger)

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// processReplicationOp performs the full replication flow for a single operation.
//
// It performs of the following steps:
//  1. Updates the operation status to HYDRATING using the leader FSM updater.
//  2. Initiates the copy of replica data from the source node to the target shard.
//  3. Once the copy succeeds, updates the sharding state to reflect the added replica.
//
// If any step fails, the operation is retried using the configured backoff policy.
// Errors are logged and wrapped using the structured error group wrapper.
func (c *CopyOpConsumer) processReplicationOp(ctx context.Context, workerId uint64, op ShardReplicationOp) error {
	logger := c.logger.WithFields(logrus.Fields{
		"op":                op.ID,
		"source_node":       op.sourceShard.nodeId,
		"target_node":       op.targetShard.nodeId,
		"source_shard":      op.sourceShard.shardId,
		"target_shard":      op.targetShard.shardId,
		"source_collection": op.sourceShard.collectionId,
		"target_collection": op.targetShard.collectionId,
	})

	startTime := c.timeProvider.Now()

	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.Go(func() error {
		return backoff.Retry(func() error {
			if ctx.Err() != nil {
				logger.WithError(ctx.Err()).Error("error while processing replication operation, shutting down")
				return backoff.Permanent(ctx.Err())
			}

			if err := c.leaderClient.ReplicationUpdateReplicaOpStatus(op.ID, api.HYDRATING); err != nil {
				logger.WithError(err).Error("failed to update replica status to 'HYDRATING'")
				return err
			}

			logger.Info("starting replication copy operation")

			if err := c.replicaCopier.CopyReplica(ctx, op.sourceShard.nodeId, op.sourceShard.collectionId, op.targetShard.shardId); err != nil {
				logger.WithError(err).Error("failure while copying replica shard")
				return err
			}

			if _, err := c.leaderClient.AddReplicaToShard(ctx, op.targetShard.collectionId, op.targetShard.shardId, op.targetShard.nodeId); err != nil {
				logger.WithError(err).Error("failure while updating sharding state")
				return err
			}

			c.logCompletedReplicationOp(workerId, startTime, c.timeProvider.Now(), op)

			return nil
		}, c.backoffPolicy)
	})

	return eg.Wait()
}

func (c *CopyOpConsumer) logCompletedReplicationOp(workerId uint64, startTime time.Time, endTime time.Time, op ShardReplicationOp) {
	duration := endTime.Sub(startTime)

	c.logger.WithFields(logrus.Fields{
		"worker":            workerId,
		"op":                op.ID,
		"duration":          duration.String(),
		"start_time":        startTime.Format(time.RFC1123),
		"completed_since":   c.timeProvider.Now().Sub(endTime),
		"source_node":       op.sourceShard.nodeId,
		"target_node":       op.targetShard.nodeId,
		"source_shard":      op.sourceShard.shardId,
		"target_shard":      op.targetShard.shardId,
		"source_collection": op.sourceShard.collectionId,
		"target_collection": op.targetShard.collectionId,
	}).Info("Replication operation completed successfully")
}
