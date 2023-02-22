//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
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
		Class    string
		Shard    string
		TxID     string // transaction ID
	}
)

func newCoordinator[T any](r *Replicator, shard, requestID string) *coordinator[T] {
	return &coordinator[T]{
		Client: r.client,
		Resolver: &resolver{
			schema:       r.stateGetter,
			nodeResolver: r.resolver,
			class:        r.class,
		},
		Class: r.class,
		Shard: shard,
		TxID:  requestID,
	}
}

func newReadCoordinator[T any](f *Finder, shard string) *coordinator[T] {
	return &coordinator[T]{
		Resolver: &resolver{
			schema:       f.resolver.schema,
			nodeResolver: f.resolver,
			class:        f.class,
		},
		Class: f.class,
		Shard: shard,
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context, replicas []string, op readyOp, level int) ([]string, error) {
	errs := make([]error, len(replicas))
	activeReplicas := make([]string, 0, len(replicas))
	var g errgroup.Group
	for i, replica := range replicas {
		i, replica := i, replica
		g.Go(func() error {
			errs[i] = op(ctx, replica, c.TxID)
			return errs[i]
		})
	}
	firstErr := g.Wait()
	for i, err := range errs {
		if err == nil {
			activeReplicas = append(activeReplicas, replicas[i])
		}
	}
	if len(activeReplicas) < level {
		firstErr = fmt.Errorf("not enough active replicas found: %w", firstErr)
	} else {
		firstErr = nil
	}

	if firstErr != nil {
		for _, node := range replicas {
			c.Abort(ctx, node, c.Class, c.Shard, c.TxID)
		}
	}

	return activeReplicas, firstErr
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context, replicas []string, op commitOp[T]) <-chan simpleResult[T] {
	replyCh := make(chan simpleResult[T], len(replicas))
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(replicas))
		for _, replica := range replicas {
			go func(replica string) {
				defer wg.Done()
				resp, err := op(ctx, replica, c.TxID)
				replyCh <- simpleResult[T]{resp, err}
			}(replica)
		}
		wg.Wait()
		close(replyCh)
	}()

	return replyCh
}

// Push pushes updates to all replicas of a specific shard
func (c *coordinator[T]) Push(ctx context.Context, cl ConsistencyLevel, ask readyOp, com commitOp[T]) (<-chan simpleResult[T], int, error) {
	state, err := c.Resolver.State(c.Shard, cl)
	if err != nil {
		return nil, 0, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	nodes, err := c.broadcast(ctx, state.Hosts, ask, level)
	if err != nil {
		return nil, level, fmt.Errorf("broadcast: %w", err)
	}
	return c.commitAll(context.Background(), nodes, com), level, nil
}

// Pull data from replica depending on consistency level
// Pull involves just as many replicas to satisfy the consistency level
func (c *coordinator[T]) Pull(ctx context.Context, cl ConsistencyLevel, op readOp[T]) (<-chan simpleResult[T], rState, error) {
	state, err := c.Resolver.State(c.Shard, cl)
	if err != nil {
		return nil, state, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	level := state.Level
	replyCh := make(chan simpleResult[T], level)

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
				replyCh <- simpleResult[T]{resp, err}
			}(i)
		}
		wg.Wait()
		close(replyCh)
	}()

	return replyCh, state, nil
}
