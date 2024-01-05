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

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"golang.org/x/sync/errgroup"
)

var (
	// msgCLevel consistency level cannot be achieved
	msgCLevel = "cannot achieve consistency level"

	errReplicas = errors.New("cannot reach enough replicas")
	errRepair   = errors.New("read repair error")
	errRead     = errors.New("read error")
)

type (
	// senderReply is a container for the data received from a replica
	senderReply[T any] struct {
		sender     string // hostname of the sender
		Version    int64  // sender's current version of the object
		Data       T      // the data sent by the sender
		UpdateTime int64  // sender's current update time
		DigestRead bool
	}
	findOneReply senderReply[objects.Replica]
	existReply   struct {
		Sender string
		RepairResponse
	}
)

// Finder finds replicated objects
type Finder struct {
	resolver     *resolver // host names of replicas
	finderStream           // stream of objects
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	resolver *resolver,
	client rClient,
	l logrus.FieldLogger,
) *Finder {
	cl := finderClient{client}
	return &Finder{
		resolver: resolver,
		finderStream: finderStream{
			repairer: repairer{
				class:  className,
				client: cl,
			},
			log: l,
		},
	}
}

// GetOne gets object which satisfies the giving consistency
func (f *Finder) GetOne(ctx context.Context,
	l ConsistencyLevel, shard string,
	id strfmt.UUID,
	props search.SelectProperties,
	adds additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard)
	op := func(ctx context.Context, host string, fullRead bool) (findOneReply, error) {
		if fullRead {
			r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds)
			return findOneReply{host, 0, r, r.UpdateTime(), false}, err
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id})
			var x RepairResponse
			if len(xs) == 1 {
				x = xs[0]
			}
			r := objects.Replica{ID: id, Deleted: x.Deleted}
			return findOneReply{host, x.Version, r, x.UpdateTime, true}, err
		}
	}
	replyCh, state, err := c.Pull(ctx, l, op, "")
	if err != nil {
		f.log.WithField("op", "pull.one").Error(err)
		return nil, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readOne(ctx, shard, id, replyCh, state)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.Value, err
}

type ShardDesc struct {
	Name string
	Node string
}

// CheckConsistency for objects belonging to different physical shards.
//
// For each x in xs the fields BelongsToNode and BelongsToShard must be set non empty
func (f *Finder) CheckConsistency(ctx context.Context,
	l ConsistencyLevel, xs []*storobj.Object,
) (retErr error) {
	if len(xs) == 0 {
		return nil
	}
	for i, x := range xs { // check shard and node name are set
		if x == nil {
			return fmt.Errorf("contains nil at object at index %d", i)
		}
		if x.BelongsToNode == "" || x.BelongsToShard == "" {
			return fmt.Errorf("missing node or shard at index %d", i)
		}
	}

	if l == One { // already consistent
		for i := range xs {
			xs[i].IsConsistent = true
		}
		return nil
	}
	// check shard consistency concurrently
	gr, ctx := errgroup.WithContext(ctx)
	for _, part := range cluster(createBatch(xs)) {
		part := part
		gr.Go(func() error {
			_, err := f.checkShardConsistency(ctx, l, part)
			if err != nil {
				f.log.WithField("op", "check_shard_consistency").
					WithField("shard", part.Shard).Error(err)
			}
			return err
		})
	}
	return gr.Wait()
}

// Exists checks if an object exists which satisfies the giving consistency
func (f *Finder) Exists(ctx context.Context,
	l ConsistencyLevel,
	shard string,
	id strfmt.UUID,
) (bool, error) {
	c := newReadCoordinator[existReply](f, shard)
	op := func(ctx context.Context, host string, _ bool) (existReply, error) {
		xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id})
		var x RepairResponse
		if len(xs) == 1 {
			x = xs[0]
		}
		return existReply{host, x}, err
	}
	replyCh, state, err := c.Pull(ctx, l, op, "")
	if err != nil {
		f.log.WithField("op", "pull.exist").Error(err)
		return false, fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	result := <-f.readExistence(ctx, shard, id, replyCh, state)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, err)
	}
	return result.Value, err
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context,
	nodeName,
	shard string,
	id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds)
	return r.Object, err
}

// checkShardConsistency checks consistency for a set of objects belonging to a shard
// It returns the most recent objects or and error
func (f *Finder) checkShardConsistency(ctx context.Context,
	l ConsistencyLevel,
	batch shardPart,
) ([]*storobj.Object, error) {
	var (
		c         = newReadCoordinator[batchReply](f, batch.Shard)
		shard     = batch.Shard
		data, ids = batch.Extract() // extract from current content
	)
	op := func(ctx context.Context, host string, fullRead bool) (batchReply, error) {
		if fullRead { // we already have the content
			return batchReply{Sender: host, IsDigest: false, FullData: data}, nil
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, ids)
			return batchReply{Sender: host, IsDigest: true, DigestData: xs}, err
		}
	}

	replyCh, state, err := c.Pull(ctx, l, op, batch.Node)
	if err != nil {
		return nil, fmt.Errorf("pull shard: %w", errReplicas)
	}
	result := <-f.readBatchPart(ctx, batch, ids, replyCh, state)
	return result.Value, result.Err
}
