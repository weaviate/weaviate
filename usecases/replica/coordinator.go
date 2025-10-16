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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
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
				return addr
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
	// Create a cancellable context for early termination when consistency is achieved
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resChan := make(chan _Result[string], len(replicas))
	successCh := make(chan _Result[string], len(replicas))
	errorCh := make(chan _Result[string], len(replicas))

	// Launch all replica operations concurrently
	var wg sync.WaitGroup
	wg.Add(len(replicas))
	for _, replica := range replicas {
		replica := replica
		go func() {
			defer wg.Done()
			err := op(cancelCtx, replica, c.TxID)
			result := _Result[string]{replica, err}
			if err != nil {
				select {
				case errorCh <- result:
				case <-cancelCtx.Done():
					// Context cancelled, don't send error
				}
			} else {
				select {
				case successCh <- result:
				case <-cancelCtx.Done():
					// Context cancelled, don't send success
				}
			}
		}()
	}

	// Collector goroutine to handle results and implement early termination
	go func() {
		defer close(resChan)
		defer wg.Wait() // Ensure all workers complete before closing

		var successCount, errorCount int
		actives := make([]_Result[string], 0, level) // cache for active replicas
		total := len(replicas)

		for successCount+errorCount < total {
			select {
			case <-cancelCtx.Done():
				// Context cancelled, abort remaining operations
				c.log.WithFields(logrus.Fields{
					"op":            "broadcast",
					"success_count": successCount,
					"error_count":   errorCount,
					"level":         level,
				}).Info("broadcast context cancelled")
				return
			case result := <-successCh:
				successCount++
				actives = append(actives, result)

				if successCount >= level {
					// Consistency level achieved - send all successful results and cancel remaining
					c.log.WithFields(logrus.Fields{
						"op":            "broadcast",
						"success_count": successCount,
						"level":         level,
					}).Info("broadcast achieved required consistency, cancelling remaining operations")

					// Send all successful results
					for _, active := range actives {
						resChan <- active
					}

					// Cancel remaining operations
					cancel()
					return
				}
			case result := <-errorCh:
				errorCount++
				c.log.WithFields(logrus.Fields{
					"op":      "broadcast",
					"replica": result.Value,
					"error":   result.Err,
				}).Error("broadcast replica failed")
			}
		}

		// All operations completed - check if we achieved consistency
		if successCount >= level {
			// Send all successful results
			for _, active := range actives {
				resChan <- active
			}
		} else {
			// Abort: consistency level not achieved
			c.log.WithFields(logrus.Fields{
				"op":            "broadcast",
				"success_count": successCount,
				"error_count":   errorCount,
				"level":         level,
				"total":         total,
			}).Error("broadcast failed to achieve consistency level")

			// Abort all replicas
			for _, replica := range replicas {
				c.Abort(ctx, replica, c.Class, c.Shard, c.TxID)
			}
			resChan <- _Result[string]{Err: fmt.Errorf("broadcast: %w", ErrReplicas)}
		}
	}()

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
		// tells active replicas to commit

		var successful atomic.Int32

		defer func() {
			if callback != nil {
				callback(int(successful.Load()))
			}
		}()

		wg := sync.WaitGroup{}

		for res := range broadcastCh {
			if res.Err != nil {
				replyCh <- _Result[T]{Err: res.Err}
				continue
			}
			replica := res.Value
			wg.Add(1)
			g := func() {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
				if err == nil {
					successful.Add(1)
				}
				replyCh <- _Result[T]{resp, err}
			}
			enterrors.GoWrapper(g, c.log)
		}

		wg.Wait()
		close(replyCh)
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

	//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
	ctxWithTimeout, _ := context.WithTimeout(context.Background(), 20*time.Second)
	c.log.WithFields(logrus.Fields{
		"action":   "coordinator_push",
		"duration": 20 * time.Second,
		"level":    level,
	}).Debug("context.WithTimeout")

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

	nodeCh := c.broadcast(ctxWithTimeout, routingPlan.ReplicasHostAddrs, ask, level)

	commitCh := c.commitAll(context.Background(), nodeCh, com, callback)

	// if there are additional hosts, we do a "best effort" write to them
	// where we don't wait for a response because they are not part of the
	// replicas used to reach level consistency
	if len(routingPlan.AdditionalHostAddrs) > 0 {
		additionalHostsBroadcast := c.broadcast(ctxWithTimeout, routingPlan.AdditionalHostAddrs, ask, len(routingPlan.AdditionalHostAddrs))
		c.commitAll(context.Background(), additionalHostsBroadcast, com, nil)
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
	pullStart := time.Now()
	routingPlan, err := c.Router.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Collection:       c.Class,
		Shard:            c.Shard,
		ConsistencyLevel: cl,
		// DirectCandidateReplica: directCandidate,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	c.log.WithFields(logrus.Fields{
		"op":         "pull",
		"phase":      "plan_built",
		"duration":   time.Since(pullStart).String(),
		"collection": c.Class,
		"shard":      c.Shard,
	}).Info("pull routing plan built")
	level := routingPlan.IntConsistencyLevel
	hosts := routingPlan.ReplicasHostAddrs
	// deduplicate hosts to avoid launching multiple workers for the same node
	if len(hosts) > 1 {
		seen := make(map[string]struct{}, len(hosts))
		unique := make([]string, 0, len(hosts))
		duplicates := make([]string, 0)
		for _, h := range hosts {
			if _, ok := seen[h]; ok {
				duplicates = append(duplicates, h)
				continue
			}
			seen[h] = struct{}{}
			unique = append(unique, h)
		}
		if len(duplicates) > 0 {
			c.log.WithFields(logrus.Fields{
				"op":           "pull",
				"duplicates":   duplicates,
				"original_cnt": len(routingPlan.ReplicasHostAddrs),
				"unique_cnt":   len(unique),
			}).Info("deduplicated replica hosts for pull")
		}
		hosts = unique
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

		// shared cancellable context to abort in-flight ops once consistency is reached
		cancelCtx, cancel := context.WithCancel(ctx)

		// start a worker per available replica and stop early once consistency reached
		doneCh := make(chan struct{})
		var stopOnce sync.Once
		successCh := make(chan _Result[T], len(hosts))
		errorCh := make(chan _Result[T], len(hosts))
		// track whether a full-read succeeded (required for read-repair correctness)
		fullSuccessCh := make(chan struct{}, 1)
		// first result timing
		var firstResultOnce sync.Once

		wg := sync.WaitGroup{}
		wg.Add(len(hosts))
		// determine local index among all hosts (prefer local for full read)
		fullReadIndex := 0
		if c.localHostAddr != "" {
			for i := 0; i < len(hosts); i++ {
				if hosts[i] == c.localHostAddr {
					fullReadIndex = i
					break
				}
			}
		}
		// log routing details and which host will do full read
		c.log.WithFields(logrus.Fields{
			"op":                 "pull",
			"level":              level,
			"num_hosts":          len(hosts),
			"full_read_host":     hosts[fullReadIndex],
			"local_host_address": c.localHostAddr,
		}).Info("pull starting with computed routing plan")
		for i := 0; i < len(hosts); i++ {
			hostIndex := i
			isFullReadWorker := hostIndex == fullReadIndex // first worker will perform the fullRead
			workerFunc := func() {
				defer wg.Done()
				workerCtx, workerCancel := context.WithTimeout(cancelCtx, timeout)
				defer workerCancel()
				// early exit if already satisfied
				select {
				case <-doneCh:
					c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex]}).Info("pull worker cancelled before start")
					return
				default:
				}
				// verify target host is still part of memberlist before attempting
				isMember := false
				for _, live := range c.Router.AllHostnames() {
					if live == hosts[hostIndex] {
						isMember = true
						break
					}
				}
				if !isMember {
					c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex]}).Info("skipping non-member host during pull")
					// report as error so standby logic/collector can progress
					select {
					case <-doneCh:
						return
					case errorCh <- _Result[T]{Err: fmt.Errorf("host not in memberlist: %s", hosts[hostIndex])}:
					}
					return
				}
				// each worker will first try its corresponding host (eg worker0 tries hosts[0],
				// worker1 tries hosts[1], etc). We want the fullRead to be tried on hosts[0]
				// because that will be the direct candidate (if a direct candidate was provided),
				// if we only used the retry queue then we would not have the guarantee that the
				// fullRead will be tried on hosts[0] first.
				c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex], "full_read": isFullReadWorker}).Info("pull worker start")
				resp, err := op(workerCtx, hosts[hostIndex], isFullReadWorker)
				// TODO return retryable info here, for now should be fine since most errors are considered retryable
				// TODO have increasing timeout passed into each op (eg 1s, 2s, 4s, 8s, 16s, 32s, with some max) similar to backoff? future PR? or should we just set timeout once per worker in Pull?
				if err == nil {
					// Do not cancel here; let the collector own cancellation once it has observed enough successes
					successCh <- _Result[T]{resp, err}
					if isFullReadWorker {
						// note a full-read success (best-effort; buffered channel prevents blocking)
						select {
						case fullSuccessCh <- struct{}{}:
						default:
						}
					}
					c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex], "full_read": isFullReadWorker}).Info("pull worker success")
					return
				}
				// on error, suppress expected cancellation noise; otherwise report unless already done
				if errors.Is(err, context.Canceled) || workerCtx.Err() == context.Canceled {
					c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex]}).Debug("pull worker canceled by context")
					return
				}
				select {
				case <-doneCh:
					c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex]}).Info("pull worker cancelled after error")
					return
				case errorCh <- _Result[T]{resp, err}:
				}
				c.log.WithFields(logrus.Fields{"op": "pull", "host": hosts[hostIndex]}).WithError(err).Info("pull worker error")
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		// collect results until we have enough successes or all workers finished
		go func() {
			defer close(replyCh)
			var successCount, errorCount int
			total := len(hosts)
			fullReadSeen := false
			for successCount+errorCount < total {
				select {
				case r := <-successCh:
					firstResultOnce.Do(func() {
						c.log.WithFields(logrus.Fields{
							"op":       "pull",
							"phase":    "first_result",
							"duration": time.Since(pullStart).String(),
						}).Info("pull first result received")
					})
					replyCh <- r
					successCount++
					// Only stop early when quorum is achieved AND we have at least one full-read result
					if successCount >= level && fullReadSeen {
						stopOnce.Do(func() { close(doneCh); cancel() })
						c.log.WithFields(logrus.Fields{"op": "pull", "successes": successCount, "level": level}).Info("pull achieved required consistency, stopping early")
						return
					}
				case <-fullSuccessCh:
					firstResultOnce.Do(func() {
						c.log.WithFields(logrus.Fields{
							"op":       "pull",
							"phase":    "first_result",
							"duration": time.Since(pullStart).String(),
						}).Info("pull first result received")
					})
					fullReadSeen = true
					if successCount >= level {
						stopOnce.Do(func() { close(doneCh); cancel() })
						c.log.WithFields(logrus.Fields{"op": "pull", "successes": successCount, "level": level, "full_read": true}).Info("pull achieved required consistency (full read present), stopping early")
						return
					}
				case r := <-errorCh:
					firstResultOnce.Do(func() {
						c.log.WithFields(logrus.Fields{
							"op":       "pull",
							"phase":    "first_result",
							"duration": time.Since(pullStart).String(),
						}).Info("pull first result received")
					})
					replyCh <- r
					errorCount++
				case <-ctx.Done():
					stopOnce.Do(func() { close(doneCh); cancel() })
					c.log.WithField("op", "pull").WithError(ctx.Err()).Info("pull context done")
					return
				}
			}
		}()
		// let workers exit; no explicit wait needed since we close on early stop
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, level, nil
}

// hostRetry tracks how long we should wait to retry this host again
type hostRetry struct {
	host           string
	currentBackOff backoff.BackOff
}
