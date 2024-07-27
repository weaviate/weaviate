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
	"math/rand/v2"
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
func (c *coordinator[T]) PullAdapted(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string, backoffConfig backoff.ExponentialBackOff,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	hosts := state.Hosts
	replyCh := make(chan _Result[T], level)

	enterrors.GoWrapper(c.pullAdapter(ctx, hosts, level, op, replyCh, backoffConfig), c.log)

	return replyCh, state, nil
}

type adapterReply[T any] struct {
	result   _Result[T]
	hostIdx  int
	fullRead bool
	hostDone bool
}

func (c *coordinator[T]) pullAdapter(ctx context.Context, hosts []string, level int, op readOp[T], replyCh chan<- _Result[T], backoffConfig backoff.ExponentialBackOff) func() {
	// TODO ctx.Done throughout?
	return func() {
		numHosts := len(hosts)
		tempReplyCh := make(chan adapterReply[T])
		fullReadDone := make(chan bool, 2)
		workersDone := make(chan bool, 2)
		wg := sync.WaitGroup{}

		if level < 1 {
			panic("TODO")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			// TODO run fullRead/worker in goroutines and funcify
			// fullReadPool := pond.New(1, 0, pond.MinWorkers(1))
			// defer fullReadPool.StopAndWait()
			// currentFullReadHostIdx := 0
			fullReadJobs := make(chan interface{}, 1)
			go func() {
				for {
					select {
					case <-fullReadDone:
						close(fullReadJobs)
						return
					default:
						fullReadJobs <- struct{}{}
					}
				}
			}()
			fullReadHostsDone := sync.Map{} // replace with mutex to avoid race?
			fullReadBackoffConfig := utils.CloneExponentialBackoff(backoffConfig)
			currentFullReadBackoff := fullReadBackoffConfig.NextBackOff()
			for {
				// TODO we fire off 2 full reads before the done signal arrives? not good
				for currentFullReadHostIdx := 0; currentFullReadHostIdx < numHosts; currentFullReadHostIdx++ {
					if v, ok := fullReadHostsDone.Load(currentFullReadHostIdx); ok && v.(bool) {
						continue
					}
					// wait until we can either submit or are done...
					select {
					case <-fullReadDone:
						return
					default:
						<-fullReadJobs
						resp, err := op(ctx, hosts[currentFullReadHostIdx], true)
						hostDone := err == nil || currentFullReadBackoff > fullReadBackoffConfig.MaxInterval || currentFullReadBackoff == backoff.Stop
						if hostDone {
							fullReadHostsDone.Store(currentFullReadHostIdx, true)
						}
						tempReplyCh <- adapterReply[T]{
							result:   _Result[T]{resp, err},
							hostIdx:  currentFullReadHostIdx,
							fullRead: true,
							hostDone: hostDone,
						}

					}
					// fullReadPool.Submit(func() {
					// 	resp, err := op(ctx, hosts[currentFullReadHostIdx], true)
					// 	hostDone := err == nil || currentFullReadBackoff > fullReadBackoffConfig.MaxElapsedTime
					// 	if hostDone {
					// 		fullReadHostsDone.Store(currentFullReadHostIdx, true)
					// 	}
					// 	tempReplyCh <- adapterReply[T]{
					// 		result:   _Result[T]{resp, err},
					// 		hostIdx:  currentFullReadHostIdx,
					// 		fullRead: true,
					// 		hostDone: hostDone,
					// 	}
					// })
					// TODO timeout/context?
					// select {
					// case <-fullReadDone:
					// 	// fullReadPool.StopAndWait() // defer this and replace break with return?
					// 	return
					// default:
					// }
				}
				select {
				case <-fullReadDone:
					// fullReadPool.StopAndWait()
					return
				case <-time.After(currentFullReadBackoff):
					currentFullReadBackoff = fullReadBackoffConfig.NextBackOff()
				}
			}
		}()

		if level > 1 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// workerPool := pond.New(level-1, 0, pond.MinWorkers(level-1))
				// defer workerPool.StopAndWait()
				// currentWorkerHostIdx := 1
				workerJobs := make(chan interface{}, level-1)
				go func() {
					for {
						select {
						case <-workersDone:
							close(workerJobs)
							return
						default:
							workerJobs <- struct{}{} //94
						}
					}
				}()
				workerHostsDone := sync.Map{}
				workerBackoffConfig := utils.CloneExponentialBackoff(backoffConfig)
				currentWorkerBackoff := workerBackoffConfig.NextBackOff()
				for {
					// TODO doing workers in reverse could make sense as well, though its nice to query hosts in order
					for currentWorkerHostIdxPre := 0; currentWorkerHostIdxPre < numHosts; currentWorkerHostIdxPre++ {
						currentWorkerHostIdx := currentWorkerHostIdxPre + 1
						// TODO or mod numHosts
						if currentWorkerHostIdx >= numHosts {
							currentWorkerHostIdx = 0
						}
						if v, ok := workerHostsDone.Load(currentWorkerHostIdx); ok && v.(bool) {
							continue
						}
						select {
						case <-workersDone:
							return
						case <-workerJobs:
							resp, err := op(ctx, hosts[currentWorkerHostIdx], false)
							// TODO check currentWorkerBackoff == backoff.Stop?
							hostDone := err == nil || currentWorkerBackoff > workerBackoffConfig.MaxInterval || currentWorkerBackoff == backoff.Stop
							if hostDone {
								workerHostsDone.Store(currentWorkerHostIdx, true)
							}
							tempReplyCh <- adapterReply[T]{
								result:   _Result[T]{resp, err},
								hostIdx:  currentWorkerHostIdx,
								fullRead: false,
								hostDone: hostDone,
							}
						}
						// workerPool.Submit(func() {
						// })
						// select {
						// case <-workersDone:
						// 	return
						// default:
						// }
					}
					select { //118
					case <-workersDone:
						return
					case <-time.After(currentWorkerBackoff):
						currentWorkerBackoff = workerBackoffConfig.NextBackOff()
					}
				}
			}()
		}

		go func() {
			fullReadLatestRepliesByHostIdx := make(map[int]adapterReply[T])
			workerLatestRepliesByHostIdx := make(map[int]adapterReply[T])
			for {
				tempReply := (<-tempReplyCh) //119
				if tempReply.fullRead {
					fullReadLatestRepliesByHostIdx[tempReply.hostIdx] = tempReply
					if tempReply.result.Err == nil {
						fullReadDone <- true
						fullReadDone <- true
						// close(fullReadDone)
					}
				} else {
					workerLatestRepliesByHostIdx[tempReply.hostIdx] = tempReply
				}
				// fmt.Printf("NATEE done checker fullread: %v\n", fullReadLatestRepliesByHostIdx)
				// fmt.Printf("NATEE done checker workers: %v\n\n", workerLatestRepliesByHostIdx)

				// do we have a fullRead reply + (>=level-1) worker replies
				fullReadSucceeded := false
				fullReadSuccessfulHostIdx := -1
				for _, reply := range fullReadLatestRepliesByHostIdx {
					if reply.result.Err == nil {
						fullReadSucceeded = true
						fullReadSuccessfulHostIdx = reply.hostIdx
						break
					}
				}
				enoughWorkerSuccesses := false
				numWorkerSuccesses := 0
				for _, reply := range workerLatestRepliesByHostIdx {
					// TODO checking hostIdx vs fullReadHostIdx can make us get extra responses?
					if reply.result.Err == nil && reply.hostIdx != fullReadSuccessfulHostIdx {
						numWorkerSuccesses++
					}
				}
				if numWorkerSuccesses >= (level - 1) {
					enoughWorkerSuccesses = true
				}
				if fullReadSucceeded && enoughWorkerSuccesses {
					replyCh <- fullReadLatestRepliesByHostIdx[fullReadSuccessfulHostIdx].result
					workerSuccessesSent := 0
					for workerSuccessesSent < (level - 1) {
						for _, reply := range workerLatestRepliesByHostIdx {
							if reply.result.Err == nil && reply.hostIdx != fullReadSuccessfulHostIdx {
								replyCh <- reply.result
								workerSuccessesSent++
							}
						}
					}
					break
				}

				// should we give up and return an error?

				// did full reads for every host fail?
				numFullReadsFailed := 0
				for i := 0; i < numHosts; i++ {
					if reply, ok := fullReadLatestRepliesByHostIdx[i]; ok {
						if reply.hostDone && reply.result.Err != nil {
							numFullReadsFailed++
						}
					}
				}
				if numFullReadsFailed >= numHosts {
					replyCh <- fullReadLatestRepliesByHostIdx[0].result
					fullReadDone <- true
					fullReadDone <- true
					// close(fullReadDone)
					break
				}

				// have too many worker hosts failed?
				numWorkersFailed := 0
				workerFailedHostIdx := -1
				for i := 0; i < numHosts; i++ {
					if reply, ok := workerLatestRepliesByHostIdx[i]; ok {
						if reply.hostDone && reply.result.Err != nil {
							numWorkersFailed++
							workerFailedHostIdx = i
						}
					}
				}
				// TODO fail early if too many workers fail
				allowedFailures := numHosts - level
				if numWorkersFailed > allowedFailures {
					replyCh <- workerLatestRepliesByHostIdx[workerFailedHostIdx].result
					break
				}

				// TODO count numbers of hosts done vs how many successes we still need
				// fmt.Println("a")
			}
			close(replyCh)
			workersDone <- true
			workersDone <- true
			wg.Wait()
			close(fullReadDone)
			close(workersDone)
		}()
	}
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
						resp, err := op(ctx, candidates[idx], idx == 0)
						if err == nil {
							// TODO https://stackoverflow.com/questions/54488284/attempting-to-acquire-a-lock-with-a-deadline-in-golang or similar to avoid race condition sending more than level
							sr := successfulReplies.Load()
							if idx == 0 {
								replyCh <- _Result[T]{resp, err}
								fullReadWasSuccessful.Store(true)
								successfulReplies.Add(1)
							} else {
								if fullReadWasSuccessful.Load() {
									if sr < int32(level) {
										replyCh <- _Result[T]{resp, err}
										successfulReplies.Add(1)
									}
								} else {
									if sr < int32(level-1) {
										replyCh <- _Result[T]{resp, err}
										successfulReplies.Add(1)
									}
								}
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
				if err != nil {
					errors <- _Result[T]{resp, err}
				}
			}
			enterrors.GoWrapper(f, c.log)
		}
		wg.Wait()
		// TODO if the same node returns above and in this loop, then we will have received level-1 acks thus not meeting the consistency level
		if !fullReadWasSuccessful.Load() {
			for j := 1; j < len(candidates); j++ {
				err = backoff.Retry(
					func() error {
						resp, err := op(ctx, candidates[j], true)
						if err == nil {
							replyCh <- _Result[T]{resp, err}
							successfulReplies.Add(1)
							// fullReadWasSuccessful.Store(true) // unneeded?
						}
						return err
					},
					utils.CloneExponentialBackoff(backoffConfig),
				)
				if err == nil {
					break
				}
			}
		}
		close(errors)
		if successfulReplies.Load() < int32(level) {
			replyCh <- (<-errors)
		}
		close(replyCh)
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

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)
func (c *coordinator[T]) PullLoic(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string,
	backoffConfig backoff.ExponentialBackOff,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	replyCh := make(chan _Result[T], level)

	candidatePool := make(chan int, len(state.Hosts)) // remaining ones
	for i := range state.Hosts {
		candidatePool <- i
	}

	// TODO can i ensure fullRead tries the direct candidate node first? this seems important for perf reasons
	// failBackOff := time.Second * 20
	// initialBackOff := time.Millisecond * 250
	failBackOff := backoffConfig.MaxInterval
	initialBackOff := backoffConfig.InitialInterval
	f := func() {
		wg := sync.WaitGroup{}
		wg.Add(level)
		for i := 0; i < level; i++ { // Ask direct candidate first
			doFullRead := i == 0
			f := func() {
				retryCounter := make(map[int]time.Duration)
				failedNodes := make(map[int]_Result[T])
				defer wg.Done()
				tryDelegate := func(d int) bool {
					if f, ok := failedNodes[d]; ok {
						replyCh <- f
						return false
					}
					if r, ok := retryCounter[d]; ok {
						timer := time.NewTimer(r)
						select {
						case <-ctx.Done():
							timer.Stop()
							break
						case <-timer.C:
						}
						timer.Stop()
					}
					// fmt.Println("NATEE plop before", i, delegate, doFullRead)
					resp, err := op(ctx, state.Hosts[d], doFullRead)
					// fmt.Println("NATEE plop after", i, delegate, doFullRead, err)
					if err == nil {
						replyCh <- _Result[T]{resp, err}
						return false
					}

					candidatePool <- d
					var newDelay time.Duration
					if d, ok := retryCounter[d]; ok {
						newDelay = backOff(d)
					} else {
						newDelay = initialBackOff
					}
					if newDelay > failBackOff {
						failedNodes[d] = _Result[T]{resp, err}
					}
					retryCounter[d] = newDelay
					return true

				}
				for delegate := range candidatePool {
					tryDelegate(delegate)
				}
			}
			enterrors.GoWrapper(f, c.log)
		}

		wg.Wait()
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}

// TODO dry
// backOff return a new random duration in the interval [d, 3d].
// It implements truncated exponential back-off with introduced jitter.
func backOff(d time.Duration) time.Duration {
	return time.Duration(float64(d.Nanoseconds()*2) * (0.5 + rand.Float64()))
}

type opT[T any] struct {
	fullRead        bool
	backoffDuration time.Duration
	result          _Result[T]
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)
// func (c *coordinator[T]) PullPool(ctx context.Context,
// 	cl ConsistencyLevel,
// 	op readOp[T], directCandidate string,
// 	backoffConfig backoff.ExponentialBackOff,
// ) (<-chan _Result[T], rState, error) {
// 	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
// 	if err != nil {
// 		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
// 	}
// 	level := state.Level
// 	replyCh := make(chan _Result[T], level)

// 	candidatePool := make(chan string, len(state.Hosts)) // remaining ones
// 	for _, replica := range state.Hosts {
// 		candidatePool <- replica
// 	}

// 	// TODO can i ensure fullRead tries the direct candidate node first? this seems important for perf reasons
// 	// failBackOff := time.Second * 20
// 	// initialBackOff := time.Millisecond * 250
// 	failBackOff := backoffConfig.MaxInterval
// 	initialBackOff := backoffConfig.InitialInterval
// 	f := func() {
// 		wg := sync.WaitGroup{}
// 		wg.Add(level)
// 		for i := 0; i < level; i++ { // Ask direct candidate first
// 			doFullRead := i == 0
// 			f := func() {
// 				retryCounter := make(map[string]time.Duration)
// 				failedNodes := make(map[string]_Result[T])
// 				defer wg.Done()
// 				for delegate := range candidatePool {
// 					if f, ok := failedNodes[delegate]; ok {
// 						replyCh <- f
// 						break
// 					}
// 					if r, ok := retryCounter[delegate]; ok {
// 						timer := time.NewTimer(r)
// 						select {
// 						case <-ctx.Done():
// 							timer.Stop()
// 							break
// 						case <-timer.C:
// 						}
// 						timer.Stop()
// 					}
// 					// fmt.Println("NATEE plop before", i, delegate, doFullRead)
// 					resp, err := op(ctx, delegate, doFullRead)
// 					// fmt.Println("NATEE plop after", i, delegate, doFullRead, err)
// 					if err == nil {
// 						replyCh <- _Result[T]{resp, err}
// 						break
// 					}

// 					candidatePool <- delegate
// 					var newDelay time.Duration
// 					if d, ok := retryCounter[delegate]; ok {
// 						newDelay = backOff(d)
// 					} else {
// 						newDelay = initialBackOff
// 					}
// 					if newDelay > failBackOff {
// 						failedNodes[delegate] = _Result[T]{resp, err}
// 					}
// 					retryCounter[delegate] = newDelay
// 					continue
// 				}
// 			}
// 			enterrors.GoWrapper(f, c.log)
// 		}

// 		wg.Wait()
// 		close(replyCh)
// 	}
// 	enterrors.GoWrapper(f, c.log)

// 	return replyCh, state, nil
// }

func (c *coordinator[T]) PullMinimal(ctx context.Context,
	cl ConsistencyLevel,
	op readOp[T], directCandidate string, backoffConfig backoff.ExponentialBackOff,
) (<-chan _Result[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl, directCandidate)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	replyCh := make(chan _Result[T], level)

	failBackOff := backoffConfig.MaxInterval
	initialBackOff := backoffConfig.InitialInterval

	candidates := state.Hosts[:level]                    // direct ones
	candidatePool := make(chan string, len(state.Hosts)) //-level) // remaining ones
	for _, replica := range state.Hosts[level:] {
		candidatePool <- replica
	}
	// close(candidatePool) // pool is ready
	f := func() {
		wg := sync.WaitGroup{}
		wg.Add(len(candidates))
		for i := range candidates { // Ask direct candidate first
			idx := i
			f := func() {
				defer wg.Done()
				resp, err := op(ctx, candidates[idx], idx == 0)
				if err == nil {
					replyCh <- _Result[T]{resp, err}
					return
				} else {
					candidatePool <- candidates[idx]
				}

				retryCounter := make(map[string]time.Duration)
				failedNodes := make(map[string]_Result[T])
				// TODO can goroutines get stuck on this range?
				for delegate := range candidatePool {
					if f, ok := failedNodes[delegate]; ok {
						replyCh <- f
						break
					}
					if r, ok := retryCounter[delegate]; ok {
						timer := time.NewTimer(r)
						select {
						case <-ctx.Done():
							timer.Stop()
							break
						case <-timer.C:
						}
						timer.Stop()
					}

					resp, err := op(ctx, delegate, idx == 0)
					if err == nil {
						replyCh <- _Result[T]{resp, err}
						break
					}

					candidatePool <- delegate
					var newDelay time.Duration
					if d, ok := retryCounter[delegate]; ok {
						newDelay = backOff(d)
					} else {
						newDelay = initialBackOff
					}
					if newDelay > failBackOff {
						failedNodes[delegate] = _Result[T]{resp, err}
					}
					retryCounter[delegate] = newDelay
				}
			}
			enterrors.GoWrapper(f, c.log)
		}
		wg.Wait()
		close(replyCh)
	}
	enterrors.GoWrapper(f, c.log)

	return replyCh, state, nil
}
