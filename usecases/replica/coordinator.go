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
	"math/rand"
	"sync"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
)

const (
	defaultPullInitialBackOff = time.Millisecond * 250
	defaultPullMaxBackOff     = time.Second * 32
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
		Resolver    *resolver // node_name -> host_address
		log         logrus.FieldLogger
		Class       string
		Shard       string
		TxID        string // transaction ID
		pullBackOff pullBackOffConfig
	}
	// pullBackOffConfig controls the op/worker backoffs in coordinator.Pull
	pullBackOffConfig struct {
		initial time.Duration
		max     time.Duration
	}
)

// newCoordinator used by the replicator
func newCoordinator[T any](r *Replicator, shard, requestID string, l logrus.FieldLogger,
) *coordinator[T] {
	return &coordinator[T]{
		Client:      r.client,
		Resolver:    r.resolver,
		log:         l,
		Class:       r.class,
		Shard:       shard,
		TxID:        requestID,
		pullBackOff: pullBackOffConfig{defaultPullInitialBackOff, defaultPullMaxBackOff},
	}
}

// newCoordinator used by the Finder to read objects from replicas
func newReadCoordinator[T any](f *Finder, shard string,
	pullBackOff pullBackOffConfig,
) *coordinator[T] {
	return &coordinator[T]{
		Resolver:    f.resolver,
		Class:       f.class,
		Shard:       shard,
		pullBackOff: pullBackOff,
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
	cl ConsistencyLevel,
	ask readyOp,
	com commitOp[T],
) (<-chan _Result[T], int, error) {
	state, err := c.Resolver.State(c.Shard, cl, "")
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	//nolint:govet // we expressely don't want to cancel that context as the timeout will take care of it
	ctxWithTimeout, _ := context.WithTimeout(context.Background(), 20*time.Second)
	c.log.WithFields(logrus.Fields{
		"action":   "coordinator_push",
		"duration": 20 * time.Second,
		"level":    level,
	}).Debug("context.WithTimeout")
	nodeCh := c.broadcast(ctxWithTimeout, state.Hosts, ask, level)
	return c.commitAll(context.Background(), nodeCh, com), level, nil
}

// Pull data from replica depending on consistency level, trying to reach level successful calls
// to op, while cycling through replicas for the coordinator's shard.
//
// Some invariants of this method (some callers depend on these):
// - Try the first fullread op on the directCandidate (if directCandidate is non-empty)
// - Only one successful fullread op will be performed
// - Query level replicas concurrently, and avoid querying more than level unless there are failures
// - Only send up to level messages onto replyCh
// - Only send error messages on replyCh once all successes have been sent
//
// Note that the first retry for a given host, may happen before c.pullBackOff.initial has passed
func (c *coordinator[T]) Pull(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	replyCh := make(chan _Result[T], level)
	hosts := state.Hosts
	f := func() {
		hostRetryQueue := make(chan hostRetry, len(hosts))
		errorResults := make(chan _Result[T], level)

		// put the "backups/fallbacks" on the retry queue
		for i := level; i < len(hosts); i++ {
			hostRetryQueue <- hostRetry{hosts[i], c.pullBackOff.initial}
		}

		// kick off only level workers so that we avoid querying nodes unnecessarily
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ {
			hostIndex := i
			isFullReadWorker := hostIndex == 0 // first worker will perform the fullRead
			workerFunc := func() {
				defer wg.Done()
				// each worker will first try its corresponding host (eg worker0 tries hosts[0],
				// worker1 tries hosts[1], etc). We want the fullRead to be tried on hosts[0]
				// because that will be the direct candidate (if a direct candidate was provided),
				// if we only used the retry queue then we would not have the guarantee that the
				// fullRead will be tried on hosts[0] first.
				resp, err := op(ctx, hosts[hostIndex], isFullReadWorker)
				if err == nil {
					replyCh <- _Result[T]{resp, err}
					return
				}
				// this host failed op on the first try, put it on the retry queue
				hostRetryQueue <- hostRetry{hosts[hostIndex], c.pullBackOff.max}

				// let's fallback to the backups in the retry queue
				for hr := range hostRetryQueue {
					resp, err := op(ctx, hr.host, isFullReadWorker)
					if err == nil {
						replyCh <- _Result[T]{resp, err}
						return
					}
					nextBackOff := nextBackOff(hr.currentBackOff)
					if nextBackOff > c.pullBackOff.max {
						// this host has run out of retries, save the result to be returned later
						errorResults <- _Result[T]{resp, err}
						return
					}
					select {
					case <-ctx.Done():
						// forward the error on if context expires
						errorResults <- _Result[T]{resp, fmt.Errorf("%v: %w", err, ctx.Err())}
						return
					case <-time.After(hr.currentBackOff):
						// pause this worker for currentBackOff, then put this host back onto the
						// retry queue (with the updated next backoff) and have this worker pick up
						// the next job
						hostRetryQueue <- hostRetry{hr.host, nextBackOff}
					}
				}
			}
			enterrors.GoWrapper(workerFunc, c.log)
		}
		wg.Wait()

		// once all workers are done, no more messages should be published to errorResults, so
		// drain any errors and put them onto replyCh
	drainErrorResults:
		for {
			select {
			case reply := <-errorResults:
				replyCh <- reply
			default:
				break drainErrorResults
			}
		}
		// callers of this function rely on replyCh being closed
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}

// hostRetry tracks how long we should wait to retry this host again
type hostRetry struct {
	host           string
	currentBackOff time.Duration
}

// nextBackOff returns a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func nextBackOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}
