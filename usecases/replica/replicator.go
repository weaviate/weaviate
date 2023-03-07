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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
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

// errBroadcast broadcast error
var errBroadcast = errors.New("broadcast: cannot reach enough replicas")

type shardingState interface {
	NodeName() string
	ResolveParentNodes(class, shardName string) (hosts, nodes []string, err error)
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

type Replicator struct {
	class          string
	stateGetter    shardingState
	client         Client
	resolver       nodeResolver
	log            logrus.FieldLogger
	requestCounter atomic.Uint64
	*Finder
}

func NewReplicator(className string,
	stateGetter shardingState,
	nodeResolver nodeResolver,
	client Client,
	l logrus.FieldLogger,
) *Replicator {
	return &Replicator{
		class:       className,
		stateGetter: stateGetter,
		client:      client,
		resolver:    nodeResolver,
		log:         l,
		Finder:      NewFinder(className, stateGetter, nodeResolver, client, l),
	}
}

func (r *Replicator) PutObject(ctx context.Context, shard string,
	obj *storobj.Object, l ConsistencyLevel,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObject), r.log)
	isReady := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObject(ctx, host, r.class, shard, requestID, obj)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	replyCh, level, err := coord.Push(ctx, l, isReady, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.one").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)

	}
	err = readSimpleResponses(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", obj.ID()).Error(err)
	}
	return err
}

func (r *Replicator) MergeObject(ctx context.Context, shard string,
	doc *objects.MergeDocument, l ConsistencyLevel,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opMergeObject), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.MergeObject(ctx, host, r.class, shard, requestID, doc)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.merge").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	err = readSimpleResponses(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", doc.ID).Error(err)
	}
	return err
}

func (r *Replicator) DeleteObject(ctx context.Context, shard string,
	id strfmt.UUID, l ConsistencyLevel,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opDeleteObject), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.delete").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	err = readSimpleResponses(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", id).Error(err)
	}
	return err
}

func (r *Replicator) PutObjects(ctx context.Context, shard string,
	objs []*storobj.Object, l ConsistencyLevel,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObjects), r.log)
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

	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.many").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
		errs := make([]error, len(objs))
		for i := 0; i < len(objs); i++ {
			errs[i] = err
		}
		return errs
	}
	errs := readSimpleResponses(len(objs), level, replyCh)
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put.many").WithField("class", r.class).
			WithField("shard", shard).Error(errs)
	}
	return errs
}

func (r *Replicator) DeleteObjects(ctx context.Context, shard string,
	docIDs []uint64, dryRun bool, l ConsistencyLevel,
) []objects.BatchSimpleObject {
	coord := newCoordinator[DeleteBatchResponse](r, shard, r.requestID(opDeleteObjects), r.log)
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

	replyCh, level, err := coord.Push(ctx, l, op, commit)
	if err != nil {
		r.log.WithField("op", "push.deletes").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
		errs := make([]objects.BatchSimpleObject, len(docIDs))
		for i := 0; i < len(docIDs); i++ {
			errs[i].Err = err
		}
		return errs
	}
	rs := readDeletionResponses(len(docIDs), level, replyCh)
	if err := firstBatchError(rs); err != nil {
		r.log.WithField("op", "put.many").WithField("class", r.class).
			WithField("shard", shard).Error(rs)
	}
	return rs
}

func (r *Replicator) AddReferences(ctx context.Context, shard string,
	refs []objects.BatchReference, l ConsistencyLevel,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opAddReferences), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.refs").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
		errs := make([]error, len(refs))
		for i := 0; i < len(refs); i++ {
			errs[i] = err
		}
		return errs
	}
	errs := readSimpleResponses(len(refs), level, replyCh)
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put.refs").WithField("class", r.class).
			WithField("shard", shard).Error(errs)
	}
	return errs
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
	if n == 0 || n != len(rs) {
		for i := range errs {
			if errs[i] == nil {
				errs[i] = defaultErr
			}
		}
	}
	return errs
}

func resultsFromDeletionResponses(batchSize int, rs []DeleteBatchResponse, defaultErr error) []objects.BatchSimpleObject {
	ret := make([]objects.BatchSimpleObject, batchSize)
	n := 0
	for _, resp := range rs {
		if len(resp.Batch) != batchSize {
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
	if n == 0 || n != len(rs) {
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

type simpleResult[T any] struct {
	Response T
	Err      error
}

func readSimpleResponses(batchSize int, level int, ch <-chan simpleResult[SimpleResponse]) []error {
	urs := make([]SimpleResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Response)
			if len(x.Response.Errors) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			if level == 0 {
				return make([]error, batchSize)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = errBroadcast
	}
	return errorsFromSimpleResponses(batchSize, urs, firstError)
}

func readDeletionResponses(batchSize int, level int, ch <-chan simpleResult[DeleteBatchResponse]) []objects.BatchSimpleObject {
	rs := make([]DeleteBatchResponse, 0, level)
	urs := make([]DeleteBatchResponse, 0, level)
	var firstError error
	for x := range ch {
		if x.Err != nil {
			urs = append(urs, x.Response)
			if len(x.Response.Batch) == 0 && firstError == nil {
				firstError = x.Err
			}
		} else {
			level--
			rs = append(rs, x.Response)
			if level == 0 {
				return resultsFromDeletionResponses(batchSize, rs, nil)
			}
		}
	}
	if level > 0 && firstError == nil {
		firstError = errBroadcast
	}
	urs = append(urs, rs...)
	return resultsFromDeletionResponses(batchSize, urs, firstError)
}

func firstError(es []error) error {
	for _, e := range es {
		if e != nil {
			return e
		}
	}
	return nil
}

func firstBatchError(xs []objects.BatchSimpleObject) error {
	for _, x := range xs {
		if x.Err != nil {
			return x.Err
		}
	}
	return nil
}
