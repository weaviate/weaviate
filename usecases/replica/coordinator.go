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
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var _ErrReplicaNotFound = errors.New("no replica found")

// replicaFinder find nodes associated with a specific shard
type replicaFinder interface {
	FindReplicas(shardName string) []string
}

// readyOp asks a replica to be read to second phase commit
type readyOp func(ctx context.Context, host, requestID string) error

// readyOp asks a replica to execute the actual operation
type commitOp[T any] func(ctx context.Context, host, requestID string) (T, error)

// coordinator coordinates replication of write request
type coordinator[T any] struct {
	client        // needed to commit and abort operation
	replicaFinder // host names of replicas
	class         string
	shard         string
	requestID     string
	// responses collect all responses of batch job
	responses []T
	nodes     []string
}

func newCoordinator[T any](r *Replicator, shard, localhost string) *coordinator[T] {
	return &coordinator[T]{
		client: r.client,
		replicaFinder: &finder{
			schema:    r.stateGetter,
			resolver:  r.resolver,
			localhost: localhost,
			class:     r.class,
		},
		class:     r.class,
		shard:     shard,
		requestID: time.Now().String(), // TODO: use a counter to build request id
	}
}

// broadcast sends write request to all replicas (first phase of a two-phase commit)
func (c *coordinator[T]) broadcast(ctx context.Context, replicas []string, op readyOp) error {
	errs := make([]error, len(replicas))
	var g errgroup.Group
	for i, replica := range replicas {
		i, replica := i, replica
		g.Go(func() error {
			errs[i] = op(ctx, replica, c.requestID)
			return nil
		})
	}
	g.Wait()
	var err error
	for _, err = range errs {
		if err != nil {
			break
		}
	}

	if err != nil {
		for _, node := range replicas {
			c.Abort(ctx, node, c.class, c.shard, c.requestID)
		}
	}

	return err
}

// commitAll tells replicas to commit pending updates related to a specific request
// (second phase of a two-phase commit)
func (c *coordinator[T]) commitAll(ctx context.Context, replicas []string, op commitOp[T]) error {
	var g errgroup.Group
	c.responses = make([]T, len(replicas))
	errs := make([]error, len(replicas))
	for i, replica := range replicas {
		i, replica := i, replica
		g.Go(func() error {
			resp, err := op(ctx, replica, c.requestID)
			c.responses[i], errs[i] = resp, err
			return nil
		})
	}
	g.Wait()
	var err error
	for _, err = range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// Replicate writes on all replicas of specific shard
func (c *coordinator[T]) Replicate(ctx context.Context, ask readyOp, com commitOp[T]) error {
	c.nodes = c.FindReplicas(c.shard)
	if len(c.nodes) == 0 {
		return fmt.Errorf("%w : class %q shard %q", _ErrReplicaNotFound, c.class, c.shard)
	}
	if err := c.broadcast(ctx, c.nodes, ask); err != nil {
		return fmt.Errorf("broadcast: %w", err)
	}
	if err := c.commitAll(ctx, c.nodes, com); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
