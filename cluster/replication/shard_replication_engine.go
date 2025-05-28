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

	"github.com/weaviate/weaviate/cluster/replication/metrics"

	"github.com/sirupsen/logrus"
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
	opsChan chan ShardReplicationOpAndStatus

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

	// maxWorkers controls the maximum number of concurrent workers in the consumer pool.
	// It is used to limit the parallelism of replication operations, preventing the system from being overwhelmed by
	// too many concurrent tasks performing replication operations.
	maxWorkers int

	// shutdownTimeout is the maximum amount of time to wait for a graceful shutdown.
	// If the engine takes longer than this timeout to shut down, a warning is logged, and the process is forcibly stopped.
	// This ensures that the system doesn't hang indefinitely during shutdown.
	shutdownTimeout time.Duration

	// engineMetricCallbacks defines optional hooks for tracking engine lifecycle events
	// like start/stop of the engine, producer, and consumer via Prometheus metrics or custom logic.
	engineMetricCallbacks *metrics.ReplicationEngineCallbacks
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
	engineMetricCallbacks *metrics.ReplicationEngineCallbacks,
) *ShardReplicationEngine {
	return &ShardReplicationEngine{
		nodeId:                nodeId,
		logger:                logger.WithFields(logrus.Fields{"action": replicationEngineLogAction, "node": nodeId}),
		producer:              producer,
		consumer:              consumer,
		opBufferSize:          opBufferSize,
		maxWorkers:            maxWorkers,
		shutdownTimeout:       shutdownTimeout,
		stopChan:              make(chan struct{}),
		engineMetricCallbacks: engineMetricCallbacks,
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

	e.engineMetricCallbacks.OnEngineStart(e.nodeId)
	// Channels are creating while starting the replication engine to allow start/stop.
	e.opsChan = make(chan ShardReplicationOpAndStatus, e.opBufferSize)
	e.stopChan = make(chan struct{})

	engineCtx, engineCancel := context.WithCancel(ctx)
	e.logger.WithFields(logrus.Fields{"engine": e}).Info("starting replication engine")

	// Channels for error reporting used by producer and consumer.
	producerErrChan := make(chan error, 1)
	consumerErrChan := make(chan error, 1)

	// Start one replication operations producer.
	e.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer e.wg.Done()
		defer e.engineMetricCallbacks.OnProducerStop(e.nodeId)
		e.engineMetricCallbacks.OnProducerStart(e.nodeId)
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
		defer e.engineMetricCallbacks.OnConsumerStop(e.nodeId)
		e.engineMetricCallbacks.OnConsumerStart(e.nodeId)
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
	close(e.opsChan)
	e.wg.Wait()
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
	if !e.isRunning.Load() {
		return
	}

	// Closing the stop channel notifies both the producer and consumer to shut down gracefully coordinating with the
	// replication engine.
	close(e.stopChan)
	e.wg.Wait()
	e.isRunning.Store(false)
	e.engineMetricCallbacks.OnEngineStop(e.nodeId)
}

// IsRunning reports whether the replication engine is currently running.
//
// It returns true if the engine has been started and has not yet shut down.
func (e *ShardReplicationEngine) IsRunning() bool {
	return e.isRunning.Load()
}

// OpChannelCap returns the capacity of the internal operation channel.
//
// This reflects the total number of replication operations the channel can queue
// before blocking the producer implementing a backpressure mechanism.
func (e *ShardReplicationEngine) OpChannelCap() int {
	return cap(e.opsChan)
}

// OpChannelLen returns the current number of operations buffered in the internal channel.
//
// This can be used to monitor the backpressure between the producer and the consumer.
func (e *ShardReplicationEngine) OpChannelLen() int {
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
