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
	defaultPullBackOffMaxElapsedTime  = time.Second * 5
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
		// localNodeName is used to detect when we're calling the local node
		localNodeName string
		// localReplicaIncoming provides direct access to local storage
		localReplicaIncoming *RemoteReplicaIncoming
		// localOpFunc provides a function to perform local operations
		localOpFunc func(ctx context.Context, host string, fullRead bool) (T, error)
	}
)

// isLocalNode checks if the given host corresponds to the local node
func (c *coordinator[T]) isLocalNode(host string) bool {
	if c.localNodeName == "" {
		return false
	}
	localHostAddr, ok := c.Router.NodeHostname(c.localNodeName)
	if !ok {
		return false
	}
	return host == localHostAddr
}

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
		localNodeName:                 r.nodeName,
		localReplicaIncoming:          nil, // Will be set by the caller if available
		localOpFunc:                   nil, // Will be set by the caller if available
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
		localNodeName:                 f.nodeName,
		localReplicaIncoming:          nil, // Will be set by the caller if available
		localOpFunc:                   nil, // Will be set by the caller if available
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context,
	replicas []string,
	op readyOp, level int,
) <-chan _Result[string] {
	c.log.WithField("op", "broadcast").WithField("replicas", len(replicas)).WithField("level", level).Info("Starting broadcast to replicas")

	// prepare tells replicas to be ready
	prepare := func() <-chan _Result[string] {
		resChan := make(chan _Result[string], len(replicas))
		f := func() { // broadcast
			defer close(resChan)
			c.log.WithField("op", "broadcast").Info("Starting parallel broadcast requests")

			var wg sync.WaitGroup
			wg.Add(len(replicas))
			for _, replica := range replicas {
				replica := replica
				g := func() {
					defer wg.Done()
					c.log.WithField("op", "broadcast").WithField("replica", replica).WithField("txid", c.TxID).Info("Starting broadcast request to replica")

					//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
					opctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
					c.log.WithField("op", "broadcast").WithFields(logrus.Fields{"replica": replica, "txid": c.TxID}).Info("context.WithTimeout")

					start := time.Now()
					err := op(opctx, replica, c.TxID)
					duration := time.Since(start)

					if err != nil {
						c.log.WithField("op", "broadcast").WithField("replica", replica).WithField("error", err).WithField("duration", duration).Info("Broadcast request failed")
					} else {
						c.log.WithField("op", "broadcast").WithField("replica", replica).WithField("duration", duration).Info("Broadcast request succeeded")
					}

					resChan <- _Result[string]{replica, err}
				}
				enterrors.GoWrapper(g, c.log)
			}
			wg.Wait()
			c.log.WithField("op", "broadcast").Info("All broadcast requests completed")
		}
		enterrors.GoWrapper(f, c.log)
		return resChan
	}

	// handle responses to prepare requests
	resChan := make(chan _Result[string], len(replicas))
	f := func() {
		defer close(resChan)
		c.log.WithField("op", "broadcast").Info("Processing broadcast responses")

		actives := make([]_Result[string], 0, level) // cache for active replicas
		successCount := 0
		failureCount := 0

		for r := range prepare() {
			if r.Err != nil { // connection error
				failureCount++
				c.log.WithField("op", "broadcast").WithField("replica", r.Value).WithField("error", r.Err).Error("Broadcast response error")
				continue
			}

			successCount++
			c.log.WithField("op", "broadcast").WithField("replica", r.Value).Info("Broadcast response success")

			level--
			if level > 0 { // cache since level has not been reached yet
				actives = append(actives, r)
				c.log.WithField("op", "broadcast").WithField("remaining_level", level).Info("Caching successful replica, waiting for more")
				continue
			}
			if level == 0 { // consistency level has been reached
				c.log.WithField("op", "broadcast").WithField("success_count", successCount).Info("Consistency level reached, sending cached replicas")
				for _, x := range actives {
					resChan <- x
				}
			}
			resChan <- r
		}

		c.log.WithField("op", "broadcast").WithField("success_count", successCount).WithField("failure_count", failureCount).Info("Broadcast response processing completed")

		if level > 0 { // abort: nothing has been sent to the caller
			fs := logrus.Fields{"op": "broadcast", "active": len(actives), "total": len(replicas), "required_level": level}
			c.log.WithFields(fs).Error("Broadcast abort - insufficient successful replicas")

			c.log.WithField("op", "broadcast").Info("Starting abort operations for all replicas")
			for _, node := range replicas {
				c.log.WithField("op", "broadcast").WithField("replica", node).Info("Sending abort to replica")
				//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
				abortCtx, _ := context.WithTimeout(context.Background(), 30*time.Second)
				c.Abort(abortCtx, node, c.Class, c.Shard, c.TxID)
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
	c.log.WithField("op", "commitAll").Info("Starting commit phase")

	replyCh := make(chan _Result[T], cap(broadcastCh))
	f := func() { // tells active replicas to commit
		// tells active replicas to commit

		var successful atomic.Int32
		var totalReplicas int32

		defer func() {
			successCount := int(successful.Load())
			totalCount := int(totalReplicas)
			c.log.WithField("op", "commitAll").WithField("successful", successCount).WithField("total", totalCount).Info("Commit phase completed")

			if callback != nil {
				callback(successCount)
			}
		}()

		wg := sync.WaitGroup{}

		for res := range broadcastCh {
			if res.Err != nil {
				c.log.WithField("op", "commitAll").WithField("error", res.Err).Error("Skipping commit due to broadcast error")
				replyCh <- _Result[T]{Err: res.Err}
				continue
			}
			replica := res.Value
			totalReplicas++
			c.log.WithField("op", "commitAll").WithField("replica", replica).WithField("txid", c.TxID).Info("Starting commit request to replica")

			wg.Add(1)
			g := func() {
				defer wg.Done()
				start := time.Now()
				resp, err := op(ctx, replica, c.TxID)
				duration := time.Since(start)

				if err == nil {
					successful.Add(1)
					c.log.WithField("op", "commitAll").WithField("replica", replica).WithField("duration", duration).Info("Commit request succeeded")
				} else {
					c.log.WithField("op", "commitAll").WithField("replica", replica).WithField("error", err).WithField("duration", duration).Info("Commit request failed")
				}

				replyCh <- _Result[T]{resp, err}
			}
			enterrors.GoWrapper(g, c.log)
		}

		c.log.WithField("op", "commitAll").Info("Waiting for all commit requests to complete")
		wg.Wait()
		close(replyCh)
		c.log.WithField("op", "commitAll").Info("All commit requests completed")
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
	c.log.WithField("op", "Push").WithField("class", c.Class).WithField("shard", c.Shard).WithField("consistency_level", cl).Info("Starting Push operation")

	routingPlan, err := c.Router.BuildWriteRoutingPlan(types.RoutingPlanBuildOptions{
		Collection:       c.Class,
		Shard:            c.Shard,
		ConsistencyLevel: cl,
	})
	if err != nil {
		c.log.WithField("op", "Push").WithField("error", err).Error("Failed to build write routing plan")
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}

	level := routingPlan.IntConsistencyLevel
	c.log.WithField("op", "Push").WithField("level", level).WithField("replicas", len(routingPlan.ReplicasHostAddrs)).WithField("additional_hosts", len(routingPlan.AdditionalHostAddrs)).Info("Write routing plan created")

	// create callback for metrics
	// the use of an immediately invoked function expression (IIFE) captures the start time
	// and returns the actual callback function.
	// The returned function is then called by commitAll once it knows how many
	// replicas have successfully committed
	callback := func() func(successful int) {
		start := time.Now()

		return func(successful int) {
			numReplicas := len(routingPlan.Replicas)
			duration := time.Since(start)

			c.log.WithField("op", "Push").WithField("successful", successful).WithField("total", numReplicas).WithField("duration", duration).Info("Push operation completed")

			if numReplicas == successful {
				c.metrics.IncWritesSucceedAll()
			} else if successful > 0 {
				c.metrics.IncWritesSucceedSome()
			} else {
				c.metrics.IncWritesFailed()
			}

			c.metrics.ObserveWriteDuration(duration)
		}
	}()

	// During shutdown, use shorter timeouts for faster completion
	prepareTimeout := 5 * time.Second
	finalizeTimeout := 5 * time.Second

	c.log.WithField("op", "Push").WithField("prepare_timeout", prepareTimeout).WithField("finalize_timeout", finalizeTimeout).Info("Starting two-phase commit")

	//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
	ctxPrepare, _ := context.WithTimeout(context.Background(), prepareTimeout)
	nodeCh := c.broadcast(ctxPrepare, routingPlan.ReplicasHostAddrs, ask, level)

	//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
	ctxFinalize, _ := context.WithTimeout(context.Background(), finalizeTimeout)
	commitCh := c.commitAll(ctxFinalize, nodeCh, com, callback)

	// if there are additional hosts, we do a "best effort" write to them
	// where we don't wait for a response because they are not part of the
	// replicas used to reach level consistency
	if len(routingPlan.AdditionalHostAddrs) > 0 {
		c.log.WithField("op", "Push").WithField("additional_hosts", len(routingPlan.AdditionalHostAddrs)).Info("Starting best-effort write to additional hosts")
		additionalHostsBroadcast := c.broadcast(ctxPrepare, routingPlan.AdditionalHostAddrs, ask, len(routingPlan.AdditionalHostAddrs))
		c.commitAll(ctxFinalize, additionalHostsBroadcast, com, nil)
	}

	c.log.WithField("op", "Push").Info("Push operation initiated successfully")
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

		hostRetryQueue := make(chan hostRetry, len(hosts))
		retryResults := make(chan _Result[T], len(hosts))
		var retryWg sync.WaitGroup

		// put the "backups/fallbacks" on the retry queue
		for i := level; i < len(hosts); i++ {
			c.log.WithField("op", "Pull").WithFields(logrus.Fields{"host": hosts[i], "level": level}).Info("hostRetryQueue")
			hostRetryQueue <- hostRetry{
				hosts[i],
				backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
			}
		}

		// Single retry worker processes all retry hosts
		retryWg.Add(1)
		go func() {
			defer retryWg.Done()
			defer close(retryResults)

			c.log.WithField("op", "retry_worker").Info("Starting centralized retry worker")

			retryTimeout := time.Duration(float64(timeout) * 0.3) // 30% of original timeout for faster retries
			retryCtx, retryCancel := context.WithTimeout(ctx, retryTimeout)
			defer retryCancel()

			// Collect all hosts from retry queue
			var retryHosts []hostRetry
			for hr := range hostRetryQueue {
				retryHosts = append(retryHosts, hr)
				c.log.WithField("op", "retry_worker").WithField("host", hr.host).Info("Collected host for retry")
			}

			if len(retryHosts) == 0 {
				c.log.WithField("op", "retry_worker").Info("No hosts to retry, exiting")
				return
			}

			c.log.WithField("op", "retry_worker").WithField("count", len(retryHosts)).Info("Starting parallel retry attempts")

			// Try all retry hosts in parallel
			var parallelWg sync.WaitGroup
			for _, hr := range retryHosts {
				parallelWg.Add(1)
				go func(hr hostRetry) {
					defer parallelWg.Done()
					c.log.WithField("op", "retry_attempt").WithField("host", hr.host).Info("Starting retry attempt")

					// Optimize local node calls: use direct access instead of HTTP
					var resp T
					var err error
					if c.localOpFunc != nil && c.isLocalNode(hr.host) {
						c.log.WithField("op", "retry_attempt").WithField("host", hr.host).Info("Using local retry operation")
						// Use direct local call for better performance during rollouts
						// Always do full read for local operations to avoid digest overhead
						resp, err = c.localOpFunc(retryCtx, hr.host, true)
						// If local operation fails, fall back to HTTP
						if err != nil {
							c.log.WithField("host", hr.host).
								WithField("error", err).
								Info("Local retry operation failed, falling back to HTTP")
							resp, err = op(retryCtx, hr.host, true)
						}
					} else {
						c.log.WithField("op", "retry_attempt").WithField("host", hr.host).Info("Using HTTP retry operation")
						// Use regular HTTP call for remote nodes
						resp, err = op(retryCtx, hr.host, true)
					}

					if err != nil {
						c.log.WithField("op", "retry_attempt").WithField("host", hr.host).WithField("error", err).Info("Retry attempt failed")
					} else {
						c.log.WithField("op", "retry_attempt").WithField("host", hr.host).Info("Retry attempt succeeded")
					}

					retryResults <- _Result[T]{resp, err}
				}(hr)
			}

			// Wait for all parallel retry attempts to complete
			parallelWg.Wait()
			c.log.WithField("op", "retry_worker").Info("All retry attempts completed")
		}()

		// kick off only level workers so that we avoid querying nodes unnecessarily
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ {
			hostIndex := i
			isFullReadWorker := true // all workers perform full reads for better rollout resilience
			workerFunc := func() {
				defer wg.Done()
				c.log.WithField("op", "worker").WithField("host", hosts[hostIndex]).WithField("index", hostIndex).Info("Starting worker")

				workerCtx, workerCancel := context.WithTimeout(ctx, timeout)
				defer workerCancel()
				// each worker will first try its corresponding host (eg worker0 tries hosts[0],
				// worker1 tries hosts[1], etc). We want the fullRead to be tried on hosts[0]
				// because that will be the direct candidate (if a direct candidate was provided),
				// if we only used the retry queue then we would not have the guarantee that the
				// fullRead will be tried on hosts[0] first.
				// Optimize local node calls: use direct access instead of HTTP
				var resp T
				var err error
				if c.localOpFunc != nil && c.isLocalNode(hosts[hostIndex]) {
					c.log.WithField("op", "worker").WithField("host", hosts[hostIndex]).Info("Using local operation")
					// Use direct local call for better performance during rollouts
					// Always do full read for local operations to avoid digest overhead
					resp, err = c.localOpFunc(workerCtx, hosts[hostIndex], true)
					// If local operation fails, fall back to HTTP
					if err != nil {
						c.log.WithField("host", hosts[hostIndex]).
							WithField("error", err).
							Info("Local operation failed, falling back to HTTP")
						resp, err = op(workerCtx, hosts[hostIndex], isFullReadWorker)
					}
				} else {
					c.log.WithField("op", "worker").WithField("host", hosts[hostIndex]).Info("Using HTTP operation")
					// Use regular HTTP call for remote nodes
					resp, err = op(workerCtx, hosts[hostIndex], isFullReadWorker)
				}

				if err == nil {
					c.log.WithField("op", "worker").WithField("host", hosts[hostIndex]).Info("Worker succeeded")
					successful.Add(1)
					replyCh <- _Result[T]{resp, err}
					return
				}

				c.log.WithField("op", "worker").WithField("host", hosts[hostIndex]).WithField("error", err).Info("Worker failed, attempting fallback")

				// Fast failover: try all remaining available hosts in parallel
				// Skip immediate fallback for ConsistencyLevelAll since all nodes must succeed
				if level != len(hosts) {
					c.log.WithField("op", "fallback").WithField("host", hosts[hostIndex]).Info("Starting parallel fallback")

					fallbackTimeout := time.Duration(float64(timeout) * 0.2) // 20% of original timeout for faster failover
					fallbackCtx, fallbackCancel := context.WithTimeout(ctx, fallbackTimeout)
					defer fallbackCancel()

					// Try all remaining hosts in parallel
					fallbackCh := make(chan _Result[T], len(hosts)-hostIndex-1)
					var fallbackWg sync.WaitGroup

					for i := hostIndex + 1; i < len(hosts); i++ {
						fallbackHostIndex := i
						fallbackWg.Add(1)
						c.log.WithField("op", "fallback").WithField("host", hosts[fallbackHostIndex]).Info("Starting fallback attempt")

						go func() {
							defer fallbackWg.Done()
							// Optimize local node calls: use direct access instead of HTTP
							var resp T
							var err error
							if c.localOpFunc != nil && c.isLocalNode(hosts[fallbackHostIndex]) {
								c.log.WithField("op", "fallback").WithField("host", hosts[fallbackHostIndex]).Info("Using local fallback operation")
								// Use direct local call for better performance during rollouts
								// Always do full read for local operations to avoid digest overhead
								resp, err = c.localOpFunc(fallbackCtx, hosts[fallbackHostIndex], true)
								// If local operation fails, fall back to HTTP
								if err != nil {
									c.log.WithField("host", hosts[fallbackHostIndex]).
										WithField("error", err).
										Info("Local fallback operation failed, falling back to HTTP")
									resp, err = op(fallbackCtx, hosts[fallbackHostIndex], isFullReadWorker)
								}
							} else {
								c.log.WithField("op", "fallback").WithField("host", hosts[fallbackHostIndex]).Info("Using HTTP fallback operation")
								// Use regular HTTP call for remote nodes
								resp, err = op(fallbackCtx, hosts[fallbackHostIndex], isFullReadWorker)
							}

							if err != nil {
								c.log.WithField("op", "fallback").WithField("host", hosts[fallbackHostIndex]).WithField("error", err).Info("Fallback attempt failed")
							} else {
								c.log.WithField("op", "fallback").WithField("host", hosts[fallbackHostIndex]).Info("Fallback attempt succeeded")
							}

							fallbackCh <- _Result[T]{resp, err}
						}()
					}

					// Wait for first successful fallback or all to fail
					go func() {
						fallbackWg.Wait()
						close(fallbackCh)
						c.log.WithField("op", "fallback").Info("All fallback attempts completed")
					}()

					// Check for successful fallback
					for result := range fallbackCh {
						if result.Err == nil {
							c.log.WithField("op", "fallback").Info("Fallback succeeded, cancelling remaining attempts")
							successful.Add(1)
							replyCh <- result
							fallbackCancel() // Cancel remaining fallback attempts
							return
						}
					}
					c.log.WithField("op", "fallback").Info("All fallback attempts failed")
				} else {
					c.log.WithField("op", "fallback").Info("Skipping fallback for ConsistencyLevelAll")
				}

				// this host failed op on the first try, put it on the retry queue
				c.log.WithField("op", "retry_queue").WithField("host", hosts[hostIndex]).Info("Adding host to retry queue")
				hostRetryQueue <- hostRetry{
					hosts[hostIndex],
					backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
				}

				// Check for successful retry from centralized retry worker
				c.log.WithField("op", "retry_check").WithField("host", hosts[hostIndex]).Info("Waiting for retry results")
				for result := range retryResults {
					if result.Err == nil {
						c.log.WithField("op", "retry_check").WithField("host", hosts[hostIndex]).Info("Retry succeeded")
						replyCh <- result
						return
					}
				}

				// All retries failed, send error result
				c.log.WithField("op", "retry_check").WithField("host", hosts[hostIndex]).Info("All retry attempts failed")
				replyCh <- _Result[T]{Err: fmt.Errorf("all retry attempts failed")}
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		wg.Wait()
		c.log.WithField("op", "Pull").Info("All workers completed")

		// Close retry queue and wait for retry worker to finish
		c.log.WithField("op", "Pull").Info("Closing retry queue and waiting for retry worker")
		close(hostRetryQueue)
		retryWg.Wait()
		c.log.WithField("op", "Pull").Info("Retry worker completed")

		// callers of this function rely on replyCh being closed
		close(replyCh)
		c.log.WithField("op", "Pull").Info("Pull operation completed")
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, level, nil
}

// hostRetry tracks how long we should wait to retry this host again
type hostRetry struct {
	host           string
	currentBackOff backoff.BackOff
}
