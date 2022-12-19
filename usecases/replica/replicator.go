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
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type opID int

const (
	opPutObject opID = iota + 1
	opMergeObject
	opDeleteObject

	opPutObjects = iota + 97
	opAddReferences
	opDeleteObjects
)

type shardingState interface {
	NodeName() string
	ShardingState(class string) *sharding.State
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type Replicator struct {
	class          string
	stateGetter    shardingState
	client         Client
	resolver       nodeResolver
	requestCounter atomic.Uint64
	*Finder
}

func NewReplicator(className string,
	stateGetter shardingState, nodeResolver nodeResolver,
	client Client, rClient RClient,
) *Replicator {
	return &Replicator{
		class:       className,
		stateGetter: stateGetter,
		client:      client,
		resolver:    nodeResolver,
		Finder:      NewFinder(className, stateGetter, nodeResolver, rClient),
	}
}

func (r *Replicator) PutObject(ctx context.Context, shard string,
	obj *storobj.Object,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObject))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObject(ctx, host, r.class, shard, requestID, obj)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) PutObjects(ctx context.Context, shard string,
	objs []*storobj.Object,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObjects))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObjects(ctx, host, r.class, shard, requestID, objs)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	err := coord.Replicate(ctx, op, r.simpleCommit(shard))
	return errorsFromSimpleResponses(len(objs), coord.responses, err)
}

func (r *Replicator) MergeObject(ctx context.Context, shard string,
	mergeDoc *objects.MergeDocument,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opMergeObject))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.MergeObject(ctx, host, r.class, shard, requestID, mergeDoc)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) simpleCommit(shard string) commitOp[SimpleResponse] {
	return func(ctx context.Context, host, requestID string) (SimpleResponse, error) {
		resp := SimpleResponse{}
		err := r.client.Commit(ctx, host, r.class, shard, requestID, &resp)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			err = fmt.Errorf("%s: %w", host, err)
		}
		return resp, err
	}
}

func (r *Replicator) DeleteObject(ctx context.Context, shard string,
	id strfmt.UUID,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opDeleteObject))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObject(ctx, host, r.class, shard, requestID, id)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	return coord.Replicate(ctx, op, r.simpleCommit(shard))
}

func (r *Replicator) DeleteObjects(ctx context.Context, shard string,
	docIDs []uint64, dryRun bool,
) []objects.BatchSimpleObject {
	coord := newCoordinator[DeleteBatchResponse](r, shard, r.requestID(opDeleteObjects))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObjects(
			ctx, host, r.class, shard, requestID, docIDs, dryRun)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	commit := func(ctx context.Context, host, requestID string) (DeleteBatchResponse, error) {
		resp := DeleteBatchResponse{}
		err := r.client.Commit(ctx, host, r.class, shard, requestID, &resp)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			err = fmt.Errorf("%q: %w", host, err)
		}
		return resp, err
	}

	err := coord.Replicate(ctx, op, commit)
	return resultsFromDeletionResponses(len(docIDs), coord.responses, err)
}

func (r *Replicator) AddReferences(ctx context.Context, shard string,
	refs []objects.BatchReference,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opAddReferences))
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.AddReferences(ctx, host, r.class, shard, requestID, refs)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	err := coord.Replicate(ctx, op, r.simpleCommit(shard))
	return errorsFromSimpleResponses(len(refs), coord.responses, err)
}

// rFinder is just a place holder to find replicas of specific hard
// TODO: the mapping between a shard and its replicas need to be implemented
type rFinder struct {
	schema   shardingState
	resolver nodeResolver
	class    string
}

func (r *rFinder) FindReplicas(shardName string) []string {
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
		for i, err := range resp.Errors {
			if !err.Empty() && errs[i] == nil {
				errs[i] = err.Clone()
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
			if !x.Error.Empty() && ret[i].Err == nil {
				ret[i].Err = x.Error.Clone()
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

func (r *Replicator) requestID(op opID) string {
	return fmt.Sprintf("%s-%.2x-%x-%x",
		r.stateGetter.NodeName(),
		op,
		time.Now().UnixMilli(),
		r.requestCounter.Add(1))
}
