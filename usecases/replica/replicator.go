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
	"errors"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type shardingState interface {
	ShardingState(class string) *sharding.State
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type Replicator struct {
	class       string
	stateGetter shardingState
	client      ReplicationClient
	resolver    nodeResolver
}

func NewReplicator(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client ReplicationClient,
) *Replicator {
	return &Replicator{
		class:       className,
		stateGetter: stateGetter,
		client:      client,
		resolver:    nodeResolver,
	}
}

func (r *Replicator) PutObject(ctx context.Context, shard string,
	obj *storobj.Object,
) error {
	coord := newCoordinator[SimpleResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObject(ctx, host, r.class, shard, requestID, obj)
		if err != nil {
			return err
		}
		return resp.FirstError()
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) PutObjects(ctx context.Context, shard string,
	objs []*storobj.Object,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObjects(ctx, host, r.class, shard, requestID, objs)
		if err != nil {
			return err
		}
		return resp.FirstError()
	}
	err := coord.Replicate(ctx, op, r.simpleCommit(shard))
	return errorsFromSimpleResponses(len(objs), coord.responses, err)
}

func (r *Replicator) MergeObject(ctx context.Context, shard string,
	mergeDoc *objects.MergeDocument,
) error {
	coord := newCoordinator[SimpleResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.MergeObject(ctx, host, r.class, shard, requestID, mergeDoc)
		if err != nil {
			return err
		}
		return resp.FirstError()
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) simpleCommit(shard string) commitOp[SimpleResponse] {
	resp := SimpleResponse{}
	return func(ctx context.Context, host, requestID string) (SimpleResponse, error) {
		err := r.client.Commit(ctx, host, r.class, shard, requestID, &resp)
		if err == nil {
			err = resp.FirstError()
		}
		return resp, err
	}
}

func (r *Replicator) DeleteObject(ctx context.Context, shard string,
	id strfmt.UUID,
) error {
	coord := newCoordinator[SimpleResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObject(ctx, host, r.class, shard, requestID, id)
		if err == nil {
			err = resp.FirstError()
		}
		return err
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) DeleteObjects(ctx context.Context, shard string,
	docIDs []uint64, dryRun bool,
) []objects.BatchSimpleObject {
	coord := newCoordinator[DeleteBatchResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObjects(
			ctx, host, r.class, shard, requestID, docIDs, dryRun)
		if err != nil {
			return err
		}
		return resp.FirstError()
	}
	commit := func(ctx context.Context, host, requestID string) (DeleteBatchResponse, error) {
		resp := DeleteBatchResponse{}
		err := r.client.Commit(ctx, host, r.class, shard, requestID, &resp)
		if err == nil {
			err = resp.FirstError()
		}
		return resp, err
	}

	err := coord.Replicate(ctx, op, commit)
	return resultsFromDeletionResponses(len(docIDs), coord.responses, err)
}

func (r *Replicator) AddReferences(ctx context.Context, shard string,
	refs []objects.BatchReference,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.AddReferences(ctx, host, r.class, shard, requestID, refs)
		if err != nil {
			return err
		}
		return resp.FirstError()
	}
	err := coord.Replicate(ctx, op, r.simpleCommit(shard))
	return errorsFromSimpleResponses(len(refs), coord.responses, err)
}

// finder is just a place holder to find replicas of specific hard
// TODO: the mapping between a shard and its replicas need to be implemented
type finder struct {
	schema   shardingState
	resolver nodeResolver
	class    string
}

func (r *finder) FindReplicas(shardName string) []string {
	shard, ok := r.schema.ShardingState(r.class).Physical[shardName]
	if !ok {
		return nil
	}
	replicas := make([]string, 0, len(shard.BelongsToNodes))
	for _, node := range shard.BelongsToNodes {
		host, ok := r.resolver.NodeHostname(node)
		if !ok || host == "" {
			return nil
		}
		replicas = append(replicas, host)

	}
	return replicas
}

func errorsFromSimpleResponses(batchSize int, rs []SimpleResponse, defaultErr error) []error {
	errs := make([]error, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Errors) != batchSize {
			continue
		}
		n++
		for i, msg := range resp.Errors {
			if msg != "" && errs[i] == nil {
				errs[i] = errors.New(msg)
			}
		}
	}
	if n != batchSize {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = defaultErr
			}
		}
	}
	return errs
}

func resultsFromDeletionResponses(size int, rs []DeleteBatchResponse, defaultErr error) []objects.BatchSimpleObject {
	ret := make([]objects.BatchSimpleObject, size)
	n := 0
	for _, resp := range rs {
		if len(resp.Batch) != size {
			continue
		}
		n++
		for i, x := range resp.Batch {
			if x.Error != "" && ret[i].Err == nil {
				ret[i].Err = errors.New(x.Error)
			}
			if ret[i].UUID == "" && x.UUID != "" {
				ret[i].UUID = strfmt.UUID(x.UUID)
			}
		}
	}
	if n != size {
		for i := range ret {
			if ret[i].Err == nil {
				ret[i].Err = defaultErr
			}
		}
	}
	return ret
}
