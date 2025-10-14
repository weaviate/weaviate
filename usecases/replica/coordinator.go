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

package replica

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	defaultPullBackOffInitialInterval = time.Millisecond * 250
	defaultPullBackOffMaxElapsedTime  = time.Second * 128
)

type (
	// readyOp asks a replica if it is ready to commit
	readyOp func(_ context.Context, host, requestID string) error

	// readyOp asks a replica to execute the actual operation
	commitOp[T any] func(_ context.Context, host, requestID string) (T, error)

	// readOp defines a generic read operation
	readOp[T any] func(_ context.Context, host string, fullRead bool) (T, error)

	// coordinator coordinates replication of write and read requests
	coordinator[T any] struct {
		Client
		Router  router
		metrics *Metrics
		log     logrus.FieldLogger
		Class   string
		Shard   string
		TxID    string // transaction ID
		// wait twice this duration for the first Pull backoff for each host
		pullBackOffPreInitialInterval time.Duration
		pullBackOffMaxElapsedTime     time.Duration // stop retrying after this long
		deletionStrategy              string
		localHostAddr                 string
	}
)

// newCoordinator used by the replicator
func newCoordinator[T any](r *Replicator, shard, requestID string, l logrus.FieldLogger,
) *coordinator[T] {
	return &coordinator[T]{
		Client:                        r.client,
		Router:                        r.router,
		metrics:                       r.metrics,
		log:                           l,
		Class:                         r.class,
		Shard:                         shard,
		TxID:                          requestID,
		pullBackOffPreInitialInterval: defaultPullBackOffInitialInterval / 2,
		pullBackOffMaxElapsedTime:     defaultPullBackOffMaxElapsedTime,
		localHostAddr: func() string {
			if addr, ok := r.router.NodeHostname(r.nodeName); ok {
				return strings.Split(addr, ":")[0]
			}
			return ""
		}(),
	}
}

// newCoordinator used by the Finder to read objects from replicas
func newReadCoordinator[T any](f *Finder, shard string,
	pullBackOffInitivalInterval time.Duration,
	pullBackOffMaxElapsedTime time.Duration,
	deletionStrategy string,
) *coordinator[T] {
	return &coordinator[T]{
		Router:                        f.router,
		Class:                         f.class,
		Shard:                         shard,
		metrics:                       f.metrics,
		log:                           f.log,
		pullBackOffPreInitialInterval: pullBackOffInitivalInterval / 2,
		pullBackOffMaxElapsedTime:     pullBackOffMaxElapsedTime,
		deletionStrategy:              deletionStrategy,
		localHostAddr: func() string {
			if addr, ok := f.router.NodeHostname(f.nodeName); ok {
				return strings.Split(addr, ":")[0]
			}
			return ""
		}(),
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context,
	replicas []string,
	op readyOp, level int,
) <-chan _Result[string] {
	// prepareCtx allows us to cancel in-flight prepares once we reach consistency
	prepareCtx, prepareCancel := context.WithCancel(ctx)
	// prepare tells replicas to be ready
	prepare := func() <-chan _Result[string] {
		resChan := make(chan _Result[string], len(replicas))
		f := func() { // broadcast
			defer close(resChan)
			var wg sync.WaitGroup
			wg.Add(len(replicas))
			for _, replica := range replicas {
				replica := replica
				g := func() {
					defer wg.Done()
					//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
					opctx, _ := context.WithTimeout(prepareCtx, 30*time.Second)
					err := op(opctx, replica, c.TxID)
					resChan <- _Result[string]{replica, err}
				}
				enterrors.GoWrapper(g, c.log)
			}
			wg.Wait()
		}
		enterrors.GoWrapper(f, c.log)
		return resChan
	}

	// handle responses to prepare requests
	resChan := make(chan _Result[string], len(replicas))
	f := func() {
		defer close(resChan)
		actives := make([]_Result[string], 0, level) // cache for active replicas
		for r := range prepare() {
			if r.Err != nil { // connection error
				c.log.WithField("op", "broadcast").Error(r.Err)
				continue
			}

			level--
			if level > 0 { // cache since level has not been reached yet
				actives = append(actives, r)
				continue
			}
			if level == 0 { // consistency level has been reached
				// Cancel any remaining prepare workers to avoid extra work
				prepareCancel()
				for _, x := range actives {
					resChan <- x
				}
			}
			resChan <- r
		}
		if level > 0 { // abort: nothing has been sent to the caller
			fs := logrus.Fields{"op": "broadcast", "active": len(actives), "total": len(replicas)}
			c.log.WithFields(fs).Error("abort")
			for _, node := range replicas {
				c.Abort(ctx, node, c.Class, c.Shard, c.TxID)
			}
			resChan <- _Result[string]{Err: fmt.Errorf("broadcast: %w", ErrReplicas)}
		}
	}
	enterrors.GoWrapper(f, c.log)
	return resChan
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context,
	broadcastCh <-chan _Result[string],
	op commitOp[T],
	callback func(successful int),
) <-chan _Result[T] {
	replyCh := make(chan _Result[T], cap(broadcastCh))
	f := func() { // tells active replicas to commit
		var successful atomic.Int32

		defer func() {
			if callback != nil {
				callback(int(successful.Load()))
			}
			close(replyCh)
		}()

		var wg sync.WaitGroup
		for res := range broadcastCh {
			if res.Err != nil {
				replyCh <- _Result[T]{Err: res.Err}
				continue
			}
			replica := res.Value
			wg.Add(1)
			g := func() {
				defer wg.Done()
				//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
				opctx, _ := context.WithTimeout(ctx, 30*time.Second)
				resp, err := op(opctx, replica, c.TxID)
				if err == nil {
					successful.Add(1)
				}
				replyCh <- _Result[T]{resp, err}
			}
			enterrors.GoWrapper(g, c.log)
		}

		wg.Wait()
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh
}

// Push pushes updates to all replicas of a specific shard
func (c *coordinator[T]) Push(ctx context.Context,
	cl types.ConsistencyLevel,
	ask readyOp,
	com commitOp[T],
) (<-chan _Result[T], int, error) {
	routingPlan, err := c.Router.BuildWriteRoutingPlan(types.RoutingPlanBuildOptions{
		Collection:       c.Class,
		Shard:            c.Shard,
		ConsistencyLevel: cl,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}

	level := routingPlan.IntConsistencyLevel

	// Use the original context directly to prevent goroutine explosion
	// Timeout handling is done in the HTTP client, not via context nesting
	sharedCtx := ctx
	c.log.WithFields(logrus.Fields{
		"action": "coordinator_push",
		"level":  level,
	}).Info("using shared context for push operation")

	// create callback for metrics
	// the use of an immediately invoked function expression (IIFE) captures the start time
	// and returns the actual callback function.
	// The returned function is then called by commitAll once it knows how many
	// replicas have successfully committed
	callback := func() func(successful int) {
		start := time.Now()

		return func(successful int) {
			numReplicas := len(routingPlan.Replicas)

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

	// Use node IDs for broadcast to align with tests/mocks
	nodeCh := c.broadcast(sharedCtx, routingPlan.Replicas, ask, level)

	commitCh := c.commitAll(sharedCtx, nodeCh, com, callback)

	// if there are additional hosts, we do a "best effort" write to them
	// where we don't wait for a response because they are not part of the
	// replicas used to reach level consistency
	if len(routingPlan.AdditionalHostAddrs) > 0 {
		additionalHostsBroadcast := c.broadcast(sharedCtx, routingPlan.AdditionalHostAddrs, ask, len(routingPlan.AdditionalHostAddrs))
		c.commitAll(sharedCtx, additionalHostsBroadcast, com, nil)
	}

	return commitCh, level, nil
}

// Pull data from replica depending on consistency level, trying to reach level successful calls
// to op, while cycling through replicas for the coordinator's shard.
//
// Some invariants of this method (some callers depend on these):
// - Try the first fullread op on the directCandidate (if directCandidate is non-empty)
// - Only one successful fullread op will be performed
// - Query level replicas concurrently, and avoid querying more than level unless there are failures
// - Only send up to level messages onto replyCh
// - Only send error messages on replyCh once it's unlikely we'll ever reach level successes
//
// Note that the first retry for a given host, may happen before c.pullBackOff.initial has passed
func (c *coordinator[T]) Pull(ctx context.Context,
	cl types.ConsistencyLevel,
	op readOp[T], directCandidate string,
	timeout time.Duration,
) (<-chan _Result[T], int, error) {
	routingPlan, err := c.Router.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Collection:             c.Class,
		Shard:                  c.Shard,
		ConsistencyLevel:       cl,
		DirectCandidateReplica: directCandidate,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := routingPlan.IntConsistencyLevel
	hosts := routingPlan.ReplicasHostAddrs

	// Log routing plan details to debug rollout behavior
	if c.log != nil {
		c.log.WithField("op", "pull").
			WithField("routing_plan_replicas", routingPlan.Replicas).
			WithField("routing_plan_hosts", routingPlan.ReplicasHostAddrs).
			WithField("consistency_level", cl).
			WithField("int_consistency_level", level).
			WithField("total_hosts", len(hosts)).
			Info("pull routing plan details")
	}
	replyCh := make(chan _Result[T], level)
	f := func() {
		start := time.Now()
		var successful atomic.Int32

		defer func() {
			if int(successful.Load()) == level {
				c.metrics.IncReadsSucceedAll()
			} else if successful.Load() > 0 {
				c.metrics.IncReadsSucceedSome()
			} else {
				c.metrics.IncReadsFailed()
			}

			c.metrics.ObserveReadDuration(time.Since(start))
		}()

		// Use a simple done channel to signal workers to stop when level is reached
		doneCh := make(chan struct{})

		hostRetryQueue := make(chan hostRetry, len(hosts))

		// put the "backups/fallbacks" on the retry queue
		for i := level; i < len(hosts); i++ {
			hostRetryQueue <- hostRetry{
				hosts[i],
				backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
			}
		}

		// Decide which index should perform the full read: prefer local if present, else first host
		fullReadIndex := 0
		if c.localHostAddr != "" {
			for i := range hosts {
				if strings.Split(hosts[i], ":")[0] == c.localHostAddr {
					fullReadIndex = i
					break
				}
			}
		}

		// kick off workers for all available hosts
		// Use channels to track success and enable early termination
		successCh := make(chan _Result[T], len(hosts))
		errorCh := make(chan _Result[T], len(hosts))

		for i := 0; i < len(hosts); i++ {
			hostIndex := i
			// Perform full read on the chosen index (currently first), others do digest
			isFullReadWorker := hostIndex == fullReadIndex
			c.log.WithField("op", "pull").
				WithField("host", hosts[hostIndex]).
				WithField("local_host_addr", c.localHostAddr).
				WithField("is_full_read_worker", isFullReadWorker).
				Info("pull worker starting")
			enterrors.GoWrapper(func() {
				// early exit if already done
				select {
				case <-doneCh:
					return
				default:
				}

				// First try assigned host (full read only on the designated worker)
				fullRead := isFullReadWorker
				resp, err := op(ctx, hosts[hostIndex], fullRead)
				if err == nil {
					successful.Add(1)
					result := _Result[T]{resp, err}
					select {
					case successCh <- result:
					default:
					}
					return
				}
				// enqueue for retries
				hostRetryQueue <- hostRetry{
					hosts[hostIndex],
					backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
				}

				// Retry from queue until done or backoff stops
				for hr := range hostRetryQueue {
					// stop if done
					select {
					case <-doneCh:
						return
					default:
					}
					// On retries, always perform digest (never full read again)
					resp, err := op(ctx, hr.host, false)
					if err == nil {
						successful.Add(1)
						result := _Result[T]{resp, err}
						select {
						case successCh <- result:
						default:
						}
						return
					}
					nextBackOff := hr.currentBackOff.NextBackOff()
					if nextBackOff == backoff.Stop {
						result := _Result[T]{resp, err}
						select {
						case errorCh <- result:
						default:
						}
						return
					}
					timer := time.NewTimer(nextBackOff)
					select {
					case <-doneCh:
						timer.Stop()
						// exit quietly on cancellation
						return
					case <-timer.C:
						hostRetryQueue <- hostRetry{hr.host, hr.currentBackOff}
					}
					timer.Stop()
				}
			}, c.log)
		}

		// Wait for required consistency level or all workers to complete
		var successCount atomic.Int32
		var errorCount atomic.Int32
		totalWorkers := len(hosts)

		for successCount.Load()+errorCount.Load() < int32(totalWorkers) {
			select {
			case result := <-successCh:
				replyCh <- result
				if successCount.Add(1) >= int32(level) {
					close(doneCh)
					close(replyCh)
					return
				}
			case result := <-errorCh:
				errorCount.Add(1)
				replyCh <- result
			case <-ctx.Done():
				close(doneCh)
				close(replyCh)
				return
			}
		}

		// All workers completed
		close(doneCh)
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, level, nil
}

// hostRetry tracks how long we should wait to retry this host again
type hostRetry struct {
	host           string
	currentBackOff backoff.BackOff
}
