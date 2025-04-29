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
	"time"

	"github.com/sirupsen/logrus"
)

// OpProducer is an interface for producing replication operations.
type OpProducer interface {
	// Produce starts producing replication operations and sends them to the provided channel.
	// A buffered channel is typically used for backpressure, but an unbounded channel may cause
	// memory growth if the consumer falls behind. Errors during production should be returned.
	Produce(ctx context.Context, out chan<- ShardReplicationOp) error
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
func NewFSMOpProducer(logger *logrus.Logger, fsm *ShardReplicationFSM, pollingInterval time.Duration, nodeId string) *FSMOpProducer {
	return &FSMOpProducer{
		logger:          logger.WithFields(logrus.Fields{"component": "replication_producer", "action": replicationEngineLogAction, "node": nodeId, "polling_interval": pollingInterval}),
		fsm:             fsm,
		pollingInterval: pollingInterval,
		nodeId:          nodeId,
	}
}

// Produce implements the OpProducer interface and starts producing operations for the given node.
//
// It uses a polling mechanism based on time.Ticker to periodically fetch all replication operations
// that should be executed on the current node. These operations are then sent to the provided output
// channel to be consumed by the OpConsumer.
//
// The function respects backpressure by using a bounded output channel. If the channel is full
// (i.e., the consumer is slow or blocked), the producer blocks while trying to send operations.
// While blocked, any additional ticks from the time.Ticker are dropped, as time.Ticker does not
// buffer ticks. This means the polling interval is effectively paused while the system is under load.
//
// This behavior is intentional: the producer only generates new work when the system has capacity
// to process it. Missing some ticks during backpressure is acceptable and avoids accumulating
// unprocessed work or overloading the system.
func (p *FSMOpProducer) Produce(ctx context.Context, out chan<- ShardReplicationOp) error {
	p.logger.WithField("producer", p).Info("starting replication engine FSM producer")

	ticker := time.NewTicker(p.pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.WithField("producer", p).Info("replication engine producer cancel request, stopping FSM producer")
			return ctx.Err()
		case <-ticker.C:
			ops := p.allOpsForNode(p.nodeId)
			if len(ops) > 0 {
				p.logger.WithFields(logrus.Fields{"producer": p, "number_of_ops": len(ops)}).Debug("preparing op replication")

				for _, op := range ops {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- op: // Write replication operation to channel.
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
	allNodeOps := p.fsm.GetOpsForTarget(nodeId)

	nodeOpsSubset := make([]ShardReplicationOp, 0, len(allNodeOps))
	for _, op := range allNodeOps {
		opState := p.fsm.GetOpState(op)

		if opState.ShouldRestartOp() {
			nodeOpsSubset = append(nodeOpsSubset, ShardReplicationOp{
				ID: op.ID,
				SourceShard: shardFQDN{
					NodeId:       op.SourceShard.NodeId,
					CollectionId: op.SourceShard.CollectionId,
					ShardId:      op.SourceShard.ShardId,
				},
				TargetShard: shardFQDN{
					NodeId:       op.TargetShard.NodeId,
					CollectionId: op.TargetShard.CollectionId,
					ShardId:      op.TargetShard.ShardId,
				},
			})
		}
	}

	return nodeOpsSubset
}
