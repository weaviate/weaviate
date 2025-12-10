//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const TwoPhaseCommitTimeout = 30 * time.Second

// twoPhaseCommit performs a two-phase commit (2PC) operation:
// Phase 1 (Prepare): Broadcast "ready" requests to replicas - they prepare but don't commit
// Phase 2 (Commit): Once enough replicas are prepared, commit the changes
//
// The function creates timeout contexts internally to ensure:
// 1. Parent context cancellation doesn't abort writes mid-flight (data consistency)
// 2. Each phase has adequate time even if the other phase consumed most of its timeout
// 3. Writes complete even if the client disconnects (critical for 2PC consistency)
// Returns a channel that will receive commit results from all prepared replicas.
func (c *coordinator[T]) twoPhaseCommit(
	hosts []string,
	level int,
	ask readyOp,
	com commitOp[T],
	callback func(successful int),
) <-chan _Result[T] {
	//nolint:govet // Cannot defer cancel() here: function returns immediately with a channel.
	// The context must outlive this function so async operations can complete.
	// The timeout will clean up the context automatically when it expires.
	ctxPhase1, _ := context.WithTimeout(context.Background(), TwoPhaseCommitTimeout)

	// Phase 1: Prepare - broadcast "ready" requests to replicas
	// Replicas prepare the transaction but don't commit yet
	preparedReplicasCh := c.broadcast(ctxPhase1, hosts, ask, level)

	//nolint:govet // Cannot defer cancel() here: function returns immediately with a channel.
	// The context must outlive this function so async operations can complete.
	// The timeout will clean up the context automatically when it expires.
	ctxPhase2, _ := context.WithTimeout(context.Background(), TwoPhaseCommitTimeout)

	// Phase 2: Commit - tell prepared replicas to commit
	// This must complete even if parent context is cancelled to maintain 2PC consistency
	return c.commitPreparedReplicas(ctxPhase2, preparedReplicasCh, com, callback)
}

// commitPreparedReplicas executes Phase 2 of the two-phase commit: commits all prepared replicas.
// It processes all prepared replicas from the channel, executes the commit operation concurrently,
// and invokes the callback with the number of successful commits for metrics.
func (c *coordinator[T]) commitPreparedReplicas(
	ctx context.Context,
	preparedReplicasCh <-chan _Result[string],
	commitOp commitOp[T],
	callback func(successful int),
) <-chan _Result[T] {
	replyCh := make(chan _Result[T], cap(preparedReplicasCh))

	enterrors.GoWrapper(func() {
		defer close(replyCh)

		var successful atomic.Int32
		var wg sync.WaitGroup

		// Process all prepared replicas and commit them concurrently
		for res := range preparedReplicasCh {
			if res.Err != nil {
				replyCh <- _Result[T]{Err: res.Err}
				continue
			}

			wg.Add(1)
			replica := res.Value
			enterrors.GoWrapper(func() {
				defer wg.Done()
				resp, err := commitOp(ctx, replica, c.TxID)
				if err == nil {
					successful.Add(1)
				}
				replyCh <- _Result[T]{resp, err}
			}, c.log)
		}

		// Wait for all commit operations to complete
		wg.Wait()

		// Invoke callback with successful count for metrics
		if callback != nil {
			callback(int(successful.Load()))
		}
	}, c.log)

	return replyCh
}

// Push pushes updates to all replicas of a specific shard
func (c *coordinator[T]) Push(ctx context.Context,
	cl types.ConsistencyLevel,
	ask readyOp,
	com commitOp[T],
) (<-chan _Result[T], int, error) {
	options := c.Router.BuildRoutingPlanOptions(c.Shard, c.Shard, cl, "")
	writeRoutingPlan, err := c.Router.BuildWriteRoutingPlan(options)
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}

	// create callback for metrics
	// the use of an immediately invoked function expression (IIFE) captures the start time
	// and returns the actual callback function.
	// The returned function is then called by twoPhaseCommit once it knows how many
	// replicas have successfully committed
	callback := func() func(successful int) {
		start := time.Now()

		return func(successful int) {
			numReplicas := len(writeRoutingPlan.Replicas())

			if numReplicas == successful {
				c.metrics.IncWritesSucceedAll()
			} else if successful > 0 {
				c.metrics.IncWritesSucceedSome()
			} else {
				c.metrics.IncWritesFailed()
			}

			c.metrics.ObserveWriteDuration(time.Since(start))
		}
	}()

	level := writeRoutingPlan.IntConsistencyLevel

	// Execute 2PC for main replicas (required for consistency level)
	commitCh := c.twoPhaseCommit(
		writeRoutingPlan.HostAddresses(),
		level,
		ask,
		com,
		callback,
	)

	// Best-effort writes to additional hosts (not part of consistency level)
	// These are kept separate because:
	// 1. Metrics callback only counts main replicas (line 78: len(writeRoutingPlan.Replicas()))
	// 2. Caller expects commitCh to contain only main replica results (based on consistency level)
	// 3. Additional hosts are fire-and-forget - we don't wait for their responses
	// Combining channels would require filtering/tagging results, adding complexity without benefit
	additionalHosts := writeRoutingPlan.AdditionalHostAddresses()
	if len(additionalHosts) > 0 {
		level := len(additionalHosts)
		c.twoPhaseCommit(
			additionalHosts,
			level,
			ask,
			com,
			nil, // No callback for additional hosts (metrics only track main replicas)
		)
	}

	return commitCh, level, nil
}
