//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

// readyOp asks a replica if it is ready to commit
type readyOp func(ctx context.Context, host, requestID string) error

// readyOp asks a replica to execute the actual operation
type commitOp[T any] func(ctx context.Context, host, requestID string) (T, error)

// coordinator coordinates replication of write requests
type coordinator[T any] struct {
	Client
	Resolver *resolver // node-name -> host-address
	Class    string
	Shard    string
	TID      string // transaction ID
}

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
		TID:   requestID,
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
			errs[i] = op(ctx, replica, c.TID)
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
			c.Abort(ctx, node, c.Class, c.Shard, c.TID)
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
				resp, err := op(ctx, replica, c.TID)
				replyCh <- simpleResult[T]{resp, err}
			}(replica)
		}
		wg.Wait()
		close(replyCh)
	}()

	return replyCh
}

// Replicate writes on all replicas of specific shard
func (c *coordinator[T]) Replicate(ctx context.Context, cl ConsistencyLevel, ask readyOp, com commitOp[T]) (<-chan simpleResult[T], int, error) {
	state, err := c.Resolver.State(c.Shard)
	level := 0
	if err == nil {
		level, err = state.ConsistencyLevel(cl)
	}
	if err != nil {
		return nil, level, fmt.Errorf("%w : class %q shard %q", err, c.Class, c.Shard)
	}
	nodes, err := c.broadcast(ctx, state.Hosts, ask, level)
	if err != nil {
		return nil, level, fmt.Errorf("broadcast: %w", err)
	}
	return c.commitAll(context.Background(), nodes, com), level, nil
}
