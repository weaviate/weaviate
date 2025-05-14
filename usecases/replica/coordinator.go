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
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
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
		Router router
		log    logrus.FieldLogger
		Class  string
		Shard  string
		TxID   string // transaction ID
		// wait twice this duration for the first Pull backoff for each host
		pullBackOffPreInitialInterval time.Duration
		pullBackOffMaxElapsedTime     time.Duration // stop retrying after this long
		deletionStrategy              string
	}
)

// newCoordinator used by the replicator
func newCoordinator[T any](r *Replicator, shard, requestID string, l logrus.FieldLogger,
) *coordinator[T] {
	return &coordinator[T]{
		Client:                        r.client,
		Router:                        r.router,
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
		pullBackOffPreInitialInterval: pullBackOffInitivalInterval / 2,
		pullBackOffMaxElapsedTime:     pullBackOffMaxElapsedTime,
		deletionStrategy:              deletionStrategy,
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context,
	replicas []string,
	op readyOp, level int,
) <-chan string {
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
					err := op(ctx, replica, c.TxID)
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
	replicaCh := make(chan string, len(replicas))
	f := func() {
		defer close(replicaCh)
		actives := make([]string, 0, level) // cache for active replicas
		for r := range prepare() {
			if r.Err != nil { // connection error
				c.log.WithField("op", "broadcast").Error(r.Err)
				continue
			}

			level--
			if level > 0 { // cache since level has not been reached yet
				actives = append(actives, r.Value)
				continue
			}
			if level == 0 { // consistency level has been reached
				for _, x := range actives {
					replicaCh <- x
				}
			}
			replicaCh <- r.Value
		}
		if level > 0 { // abort: nothing has been sent to the caller
			fs := logrus.Fields{"op": "broadcast", "active": len(actives), "total": len(replicas)}
			c.log.WithFields(fs).Error("abort")
			for _, node := range replicas {
				c.Abort(ctx, node, c.Class, c.Shard, c.TxID)
			}
		}
	}
	enterrors.GoWrapper(f, c.log)
	return replicaCh
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context,
	replicaCh <-chan string,
	op commitOp[T],
) <-chan _Result[T] {
	replyCh := make(chan _Result[T], cap(replicaCh))
	f := func() { // tells active replicas to commit
		wg := sync.WaitGroup{}
		for replica := range replicaCh {
			wg.Add(1)
			replica := replica
			g := func() {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
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
	nodeCh := c.broadcast(ctxWithTimeout, routingPlan.ReplicasHostAddrs, ask, level)
	commitCh := c.commitAll(context.Background(), nodeCh, com)

	// if there are additional hosts, we do a "best effort" write to them
	// where we don't wait for a response because they are not part of the
	// replicas used to reach level consistency
	if len(routingPlan.AdditionalHostAddrs) > 0 {
		additionalHostsBroadcast := c.broadcast(ctxWithTimeout, routingPlan.AdditionalHostAddrs, ask, len(routingPlan.AdditionalHostAddrs))
		c.commitAll(context.Background(), additionalHostsBroadcast, com)
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
	replyCh := make(chan _Result[T], level)
	f := func() {
		hostRetryQueue := make(chan hostRetry, len(hosts))

		// put the "backups/fallbacks" on the retry queue
		for i := level; i < len(hosts); i++ {
			hostRetryQueue <- hostRetry{
				hosts[i],
				backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
			}
		}

		// kick off only level workers so that we avoid querying nodes unnecessarily
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ {
			hostIndex := i
			isFullReadWorker := hostIndex == 0 // first worker will perform the fullRead
			workerFunc := func() {
				defer wg.Done()
				workerCtx, workerCancel := context.WithTimeout(ctx, timeout)
				defer workerCancel()
				// each worker will first try its corresponding host (eg worker0 tries hosts[0],
				// worker1 tries hosts[1], etc). We want the fullRead to be tried on hosts[0]
				// because that will be the direct candidate (if a direct candidate was provided),
				// if we only used the retry queue then we would not have the guarantee that the
				// fullRead will be tried on hosts[0] first.
				resp, err := op(workerCtx, hosts[hostIndex], isFullReadWorker)
				// TODO return retryable info here, for now should be fine since most errors are considered retryable
				// TODO have increasing timeout passed into each op (eg 1s, 2s, 4s, 8s, 16s, 32s, with some max) similar to backoff? future PR? or should we just set timeout once per worker in Pull?
				if err == nil {
					replyCh <- _Result[T]{resp, err}
					return
				}
				// this host failed op on the first try, put it on the retry queue
				hostRetryQueue <- hostRetry{
					hosts[hostIndex],
					backoff.WithContext(utils.NewExponentialBackoff(c.pullBackOffPreInitialInterval, c.pullBackOffMaxElapsedTime), ctx),
				}

				// let's fallback to the backups in the retry queue
				for hr := range hostRetryQueue {
					resp, err := op(workerCtx, hr.host, isFullReadWorker)
					if err == nil {
						replyCh <- _Result[T]{resp, err}
						return
					}
					nextBackOff := hr.currentBackOff.NextBackOff()
					if nextBackOff == backoff.Stop {
						// this host has run out of retries, send the result and note that
						// we have the worker exit here with the assumption that once we've reached
						// this many failures for this host, we've tried all other hosts enough
						// that we're not going to reach level successes
						replyCh <- _Result[T]{resp, err}
						return
					}

					timer := time.NewTimer(nextBackOff)
					select {
					case <-workerCtx.Done():
						timer.Stop()
						replyCh <- _Result[T]{resp, err}
						return
					case <-timer.C:
						hostRetryQueue <- hostRetry{hr.host, hr.currentBackOff}
					}
					timer.Stop()
				}
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		wg.Wait()
		// callers of this function rely on replyCh being closed
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
