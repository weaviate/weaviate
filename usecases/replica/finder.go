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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
)

type (
	// senderReply represent the data received from a sender
	senderReply[T any] struct {
		sender string // hostname of the sender
		data   T      // the data sent by the sender
	}
	findOneReply senderReply[*storobj.Object]
	existReply   senderReply[bool]
)

// Finder finds replicated objects
type Finder struct {
	RClient            // needed to commit and abort operation
	resolver *resolver // host names of replicas
	class    string
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client RClient,
) *Finder {
	return &Finder{
		class: className,
		resolver: &resolver{
			schema:       stateGetter,
			nodeResolver: nodeResolver,
			class:        className,
		},
		RClient: client,
	}
}

// FindOne finds one object which satisfies the giving consistency
func (f *Finder) FindOne(ctx context.Context, l ConsistencyLevel, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard)
	op := func(ctx context.Context, host string) (findOneReply, error) {
		obj, err := f.FindObject(ctx, host, f.class, shard, id, props, additional)
		return findOneReply{host, obj}, err
	}
	replyCh, level, err := c.Fetch(ctx, l, op)
	if err != nil {
		return nil, err
	}
	return readOne(replyCh, level)
}

// Exists checks if an object exists which satisfies the giving consistency
func (f *Finder) Exists(ctx context.Context, l ConsistencyLevel, shard string, id strfmt.UUID) (bool, error) {
	c := newReadCoordinator[existReply](f, shard)
	op := func(ctx context.Context, host string) (existReply, error) {
		obj, err := f.RClient.Exists(ctx, host, f.class, shard, id)
		return existReply{host, obj}, err
	}
	replyCh, level, err := c.Fetch(ctx, l, op)
	if err != nil {
		return false, err
	}
	return readOneExists(replyCh, level)
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context, nodeName, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.resolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	return f.RClient.FindObject(ctx, host, f.class, shard, id, props, additional)
}
