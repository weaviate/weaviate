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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
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

			c.log.WithField("op", "broadcast").Info("Starting parallel abort operations for all replicas")
			var abortWg sync.WaitGroup
			for _, node := range replicas {
				abortWg.Add(1)
				go func(node string) {
					defer abortWg.Done()
					c.log.WithField("op", "broadcast").WithField("replica", node).Info("Sending abort to replica")
					//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
					abortCtx, _ := context.WithTimeout(context.Background(), 3*time.Second)
					c.Abort(abortCtx, node, c.Class, c.Shard, c.TxID)
				}(node)
			}
			abortWg.Wait()
			c.log.WithField("op", "broadcast").Info("All abort operations completed")
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
	replyCh := make(chan _Result[T], len(hosts)) // Buffer for all hosts
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

		// Try level hosts initially (respecting invariants)
		var wg sync.WaitGroup
		wg.Add(level)

		// Create cancellable context for early exit
		workerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Try directCandidate first if provided
		if directCandidate != "" {
			go func() {
				defer wg.Done()

				// Check if we should exit early
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				// Try directCandidate first
				resp, err := c.tryHost(workerCtx, directCandidate, timeout, op)
				if err == nil {
					currentSuccess := successful.Add(1)
					replyCh <- _Result[T]{resp, err}

					// Early exit: cancel remaining workers if consistency level reached
					if int(currentSuccess) >= level {
						c.log.WithField("op", "early_exit").WithField("successful", currentSuccess).WithField("level", level).Info("Consistency level reached, cancelling remaining workers")
						cancel()
					}
					return
				}
			}()
		}

		// Try remaining hosts up to level
		startIndex := 0
		if directCandidate != "" {
			startIndex = 1 // Skip directCandidate if already tried
		}

		for i := startIndex; i < level; i++ {
			go func(hostIndex int, host string) {
				defer wg.Done()

				// Check if we should exit early
				select {
				case <-workerCtx.Done():
					return
				default:
				}

				// Try this host
				resp, err := c.tryHost(workerCtx, host, timeout, op)
				if err == nil {
					currentSuccess := successful.Add(1)
					replyCh <- _Result[T]{resp, err}

					// Early exit: cancel remaining workers if consistency level reached
					if int(currentSuccess) >= level {
						c.log.WithField("op", "early_exit").WithField("successful", currentSuccess).WithField("level", level).Info("Consistency level reached, cancelling remaining workers")
						cancel()
					}
					return
				}
			}(i, hosts[i])
		}

		wg.Wait()

		// Send error if we didn't reach consistency level
		if int(successful.Load()) < level {
			c.log.WithField("op", "Pull").WithField("successful", successful.Load()).WithField("level", level).Error("Failed to reach consistency level")
			replyCh <- _Result[T]{Err: fmt.Errorf("cannot achieve consistency level %d: only %d successful", level, successful.Load())}
		}

		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, level, nil
}

// tryHost attempts to perform the operation on a single host
func (c *coordinator[T]) tryHost(ctx context.Context, host string, timeout time.Duration, op readOp[T]) (T, error) {
	hostCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Try local operation first if available
	if c.localOpFunc != nil && c.isLocalNode(host) {
		resp, err := c.localOpFunc(hostCtx, host, true)
		if err == nil {
			return resp, nil
		}
		// Fall back to HTTP if local operation fails
	}

	// Use regular HTTP call
	return op(hostCtx, host, true)
}
