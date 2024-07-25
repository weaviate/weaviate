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
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/sirupsen/logrus"
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
		Resolver *resolver // node_name -> host_address
		log      logrus.FieldLogger
		Class    string
		Shard    string
		TxID     string // transaction ID
	}
)

// newCoordinator used by the replicator
func newCoordinator[T any](r *Replicator, shard, requestID string, l logrus.FieldLogger,
) *coordinator[T] {
	return &coordinator[T]{
		Client:   r.client,
		Resolver: r.resolver,
		log:      l,
		Class:    r.class,
		Shard:    shard,
		TxID:     requestID,
	}
}

// newCoordinator used by the Finder to read objects from replicas
func newReadCoordinator[T any](f *Finder, shard string) *coordinator[T] {
	return &coordinator[T]{
		Resolver: f.resolver,
		Class:    f.class,
		Shard:    shard,
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
	nodeCh := c.broadcast(ctxWithTimeout, state.Hosts, ask, level)
	return c.commitAll(context.Background(), nodeCh, com), level, nil
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)

// TODO backoffConfig into coordinator?
// TODO try worker pool package (or learn/read/understand and steal relevant parts) https://github.com/alitto/pond
func (c *coordinator[T]) Pull(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string, backoffConfig backoff.ExponentialBackOff,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	replyCh := make(chan _Result[T], level)

	candidates := state.Hosts
	successfulReplies := atomic.Int32{} // TODO have backoff end once success we reaach level successful replies?
	fullReadWasSuccessful := atomic.Bool{}
	errors := make(chan _Result[T], len(candidates))
	f := func() {
		wg := sync.WaitGroup{}
		wg.Add(len(candidates))
		for i := range candidates { // Ask direct candidate first
			idx := i
			f := func() {
				defer wg.Done()

				resp, err := backoff.RetryWithData(
					func() (T, error) {
						// fmt.Println("NATEE op starting", idx)
						resp, err := op(ctx, candidates[idx], idx == 0)
						// fmt.Println("NATEE op done", idx, resp, err)
						if err == nil {
							// fmt.Println("NATEE reply", idx, resp, err)
							replyCh <- _Result[T]{resp, err}
							successfulReplies.Add(1)
							if idx == 0 {
								fullReadWasSuccessful.Store(true)
							}
						}
						// TODO is idx check needed for finder's GetOne, checkShardConsistency, CollectShardDifferences, FindUUIDs, Exists? In the current implementation does Pull always fail if the direct candidate (self node) errors/times out?
						if idx != 0 && successfulReplies.Load() >= int32(level) {
							return resp, backoff.Permanent(err)
						}
						return resp, err
					},
					utils.CloneExponentialBackoff(backoffConfig),
				)
				// fmt.Println("NATEE backoff done", idx, resp, err)
				if err != nil {
					// fmt.Println("NATEE backoff err", idx, resp, err)
					errors <- _Result[T]{resp, err}
				}
			}
			enterrors.GoWrapper(f, c.log)
		}
		// fmt.Println("NATEE waiting started")
		wg.Wait()
		// fmt.Println("NATEE waiting done")
		if !fullReadWasSuccessful.Load() {
			// fmt.Println("NATEE fullread failed")
			for j := 1; j < len(candidates); j++ {
				// fmt.Println("NATEE fullread idx", j)
				err = backoff.Retry(
					func() error {
						// fmt.Println("NATEE fullread op starting", j)
						resp, err := op(ctx, candidates[j], true)
						// fmt.Println("NATEE fullread op done", j, resp, err)
						if err == nil {
							// fmt.Println("NATEE fullread reply", j, resp, err)
							replyCh <- _Result[T]{resp, err}
							successfulReplies.Add(1)
							// fullReadWasSuccessful.Store(true) // unneeded?
						}
						return err
					},
					utils.CloneExponentialBackoff(backoffConfig),
				)
				// fmt.Println("NATEE fullread backoff done", j, err)
				if err == nil {
					break
				}
			}
		}
		// fmt.Println("NATEE errors closing")
		close(errors)
		// fmt.Println("NATEE errors closed")
		if successfulReplies.Load() < int32(level) {
			// fmt.Println("NATEE lt level")
			replyCh <- (<-errors)
		}
		// fmt.Println("NATEE starting closing replych")
		close(replyCh)
		// fmt.Println("NATEE done closing replych")
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}

// TODO doc
// type pullOpArgs struct {
// 	hostIndex   int
// 	retryNumber int // starts at 0, first retry is 1
// }

// // TODO doc
// type pullOpArgs struct {
// 	hostIndex   int
// 	retryNumber int // starts at 0, first retry is 1
// }

// // Pull data from replica depending on consistency level
// // Pull involves just as many replicas to satisfy the consistency level.
// //
// // directCandidate when specified a direct request is set to this node (default to this node)
// func (c *coordinator[T]) Pull(ctx context.Context,
// 	cl ConsistencyLevel,
// 	op readOp[T], directCandidate string,
// ) (<-chan _Result[T], rState, error) {
// 	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
// 	if err != nil {
// 		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
// 	}

// 	hosts := state.Hosts
// 	numHosts := len(hosts)
// 	level := state.Level
// 	numWorkers := level // TODO configurable?
// 	maxRetries := 0
// 	tempReplyCh := make(chan _Result[T], level)
// 	replyCh := make(chan _Result[T], level)
// 	errCh := make(chan error, numHosts)

// 	if numWorkers > numHosts {
// 		// TODO explain and error strs
// 		c.log.Errorf("")
// 		return nil, state, fmt.Errorf("")
// 	}

// 	opJobQueue := make(chan pullOpArgs, numHosts)
// 	done := make(chan bool)
// 	for i := range hosts {
// 		// Put direct candidate onto job queue first
// 		opJobQueue <- pullOpArgs{
// 			hostIndex:   i,
// 			retryNumber: 0,
// 		}
// 	}

// 	f := func() {
// 		wg := sync.WaitGroup{}
// 		wg.Add(numWorkers)
// 		go func() {
// 			for i := 0; i < level; i++ {
// 				replyCh <- <-tempReplyCh
// 			}
// 			done <- true
// 			close(done)
// 		}()
// 		for i := 0; i < numWorkers; i++ {
// 			workerFunc := func() {
// 				defer wg.Done()
// 			workerLoop:
// 				for {
// 					select {
// 					case <-ctx.Done():
// 						break workerLoop
// 					case <-done:
// 						break workerLoop
// 					case opJob := <-opJobQueue:
// 						hostIndex := opJob.hostIndex
// 						retryNum := opJob.retryNumber
// 						resp, err := op(ctx, state.Hosts[hostIndex], hostIndex == 0)
// 						if err == nil {
// 							tempReplyCh <- _Result[T]{resp, err}
// 						}
// 						if retryNum < maxRetries {
// 							// try again
// 							nextAvgBackoff := math.Pow(2, float64(retryNum))
// 							jitterFactor := 0.5 + rand.Float64()
// 							nextJitteredBackoff := time.Duration(nextAvgBackoff * jitterFactor)
// 							go func() { // TODO replace with gowrapper?
// 								// Wait for nextJitteredBackoff, then put this host back in the job queue
// 								<-time.After(nextJitteredBackoff)
// 								opJobQueue <- pullOpArgs{
// 									hostIndex:   i,
// 									retryNumber: retryNum + 1,
// 								}
// 							}()
// 						} else {
// 							// give up
// 							errCh <- err
// 						}
// 					}
// 				}
// 			}
// 			enterrors.GoWrapper(workerFunc, c.log)
// 		}
// 		fmt.Println("NATEE waiting")
// 		wg.Wait()
// 		fmt.Println("NATEE closing replyCh")
// 		close(replyCh)
// 		fmt.Println("NATEE closed replyCh")
// 	}
// 	enterrors.GoWrapper(f, c.log)

// 	return replyCh, state, nil
// }
