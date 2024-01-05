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
		go func() { // broadcast
			defer close(resChan)
			var wg sync.WaitGroup
			wg.Add(len(replicas))
			for _, replica := range replicas {
				go func(replica string, candidateCh chan<- _Result[string]) error {
					defer wg.Done()
					err := op(ctx, replica, c.TxID)
					candidateCh <- _Result[string]{replica, err}
					return err
				}(replica, resChan)
			}
			wg.Wait()
		}()
		return resChan
	}

	// handle responses to prepare requests
	replicaCh := make(chan string, len(replicas))
	go func(level int) {
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
	}(level)
	return replicaCh
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context,
	replicaCh <-chan string,
	op commitOp[T],
) <-chan _Result[T] {
	replyCh := make(chan _Result[T], cap(replicaCh))
	go func() { // tells active replicas to commit
		wg := sync.WaitGroup{}
		for replica := range replicaCh {
			wg.Add(1)
			go func(replica string) {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
				replyCh <- _Result[T]{resp, err}
			}(replica)
		}
		wg.Wait()
		close(replyCh)
	}()

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
	nodeCh := c.broadcast(ctx, state.Hosts, ask, level)
	return c.commitAll(context.Background(), nodeCh, com), level, nil
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level.
//
// directCandidate when specified a direct request is set to this node (default to this node)
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

	candidates := state.Hosts[:level]                          // direct ones
	candidatePool := make(chan string, len(state.Hosts)-level) // remaining ones
	for _, replica := range state.Hosts[level:] {
		candidatePool <- replica
	}
	close(candidatePool) // pool is ready
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(candidates))
		for i := range candidates { // Ask direct candidate first
			go func(idx int) {
				defer wg.Done()
				resp, err := op(ctx, candidates[idx], idx == 0)

				// If node is not responding delegate request to another node
				for err != nil {
					if delegate, ok := <-candidatePool; ok {
						resp, err = op(ctx, delegate, idx == 0)
					} else {
						break
					}
				}
				replyCh <- _Result[T]{resp, err}
			}(i)
		}
		wg.Wait()
		close(replyCh)
	}()

	return replyCh, state, nil
}
