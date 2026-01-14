//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// opID operation encode as and int
type opID int

const (
	opPutObject opID = iota + 1
	opMergeObject
	opDeleteObject

	opPutObjects = iota + 97
	opAddReferences
	opDeleteObjects
)

type (
	// Result represents a valid value or an error.
	Result[T any] struct {
		Value T
		Err   error
	}
)

type Replicator struct {
	class          string
	nodeName       string
	router         types.Router
	client         Client
	log            logrus.FieldLogger
	requestCounter atomic.Uint64
	*Finder
}

func NewReplicator(className string,
	router types.Router,
	nodeResolver cluster.NodeResolver,
	nodeName string,
	getDeletionStrategy func() string,
	client Client,
	promMetrics *monitoring.PrometheusMetrics,
	l logrus.FieldLogger,
) (*Replicator, error) {
	metrics, err := NewMetrics(promMetrics)
	if err != nil {
		return nil, fmt.Errorf("create metrics: %w", err)
	}

	return &Replicator{
		class:    className,
		nodeName: nodeName,
		router:   router,
		client:   client,
		log:      l,
		Finder: NewFinder(
			className,
			router,
			nodeResolver,
			nodeName,
			client,
			metrics,
			l,
			getDeletionStrategy,
		),
	}, nil
}

func (r *Replicator) PutObject(ctx context.Context,
	shard string,
	obj *storobj.Object,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := NewWriteCoordinator[SimpleResponse, error](r.client, r.router, r.metrics, r.class, shard, r.requestID(opPutObject), r.log)
	isReady := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObject(ctx, host, r.class, shard, requestID, obj, schemaVersion)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	errs, err := coord.Push(ctx, l, isReady, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.one").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)

	}
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", obj.ID()).Error(err)
		return err
	}
	return nil
}

func (r *Replicator) MergeObject(ctx context.Context,
	shard string,
	doc *objects.MergeDocument,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := NewWriteCoordinator[SimpleResponse, error](r.client, r.router, r.metrics, r.class, shard, r.requestID(opMergeObject), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.MergeObject(ctx, host, r.class, shard, requestID, doc, schemaVersion)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	errs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.merge").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "merge").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", doc.ID).Error(err)
		var replicaErr *Error
		if errors.As(err, &replicaErr) && replicaErr != nil && replicaErr.Code == StatusObjectNotFound {
			return objects.NewErrDirtyWriteOfDeletedObject(replicaErr)
		}
		return err
	}
	return nil
}

func (r *Replicator) DeleteObject(ctx context.Context,
	shard string,
	id strfmt.UUID,
	deletionTime time.Time,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := NewWriteCoordinator[SimpleResponse, error](r.client, r.router, r.metrics, r.class, shard, r.requestID(opDeleteObject), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObject(ctx, host, r.class, shard, requestID, id, deletionTime, schemaVersion)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	errs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.delete").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", id).Error(err)
		return err
	}
	return nil
}

func (r *Replicator) PutObjects(ctx context.Context,
	shard string,
	objs []*storobj.Object,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) []error {
	coord := NewWriteCoordinator[SimpleResponse, error](r.client, r.router, r.metrics, r.class, shard, r.requestID(opPutObjects), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.PutObjects(ctx, host, r.class, shard, requestID, objs, schemaVersion)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	errs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, len(objs))
	if err != nil {
		r.log.WithField("op", "push.many").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
		errs := make([]error, len(objs))
		for i := 0; i < len(objs); i++ {
			errs[i] = err
		}
		return errs
	}
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put.many").WithField("class", r.class).
			WithField("shard", shard).Error(errs)
	}
	return errs
}

func (r *Replicator) DeleteObjects(ctx context.Context,
	shard string,
	uuids []strfmt.UUID,
	deletionTime time.Time,
	dryRun bool,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) []objects.BatchSimpleObject {
	coord := NewWriteCoordinator[DeleteBatchResponse, objects.BatchSimpleObject](r.client, r.router, r.metrics, r.class, shard, r.requestID(opDeleteObjects), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.DeleteObjects(ctx, host, r.class, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
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
	rs, err := coord.Push(ctx, l, op, commit, r.readDeleteBatchResponse, r.flattenDeletions, len(uuids))
	if err != nil {
		r.log.WithField("op", "push.deletes").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
		errs := make([]objects.BatchSimpleObject, len(uuids))
		for i := 0; i < len(uuids); i++ {
			errs[i].Err = err
		}
		return errs
	}
	if err := firstBatchError(rs); err != nil {
		r.log.WithField("op", "put.deletes").WithField("class", r.class).
			WithField("shard", shard).Error(rs)
	}
	return rs
}

func (r *Replicator) AddReferences(ctx context.Context,
	shard string,
	refs []objects.BatchReference,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) []error {
	coord := NewWriteCoordinator[SimpleResponse, error](r.client, r.router, r.metrics, r.class, shard, r.requestID(opAddReferences), r.log)
	op := func(ctx context.Context, host, requestID string) error {
		resp, err := r.client.AddReferences(ctx, host, r.class, shard, requestID, refs, schemaVersion)
		if err == nil {
			err = resp.FirstError()
		}
		if err != nil {
			return fmt.Errorf("%q: %w", host, err)
		}
		return nil
	}
	errs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, len(refs))
	if err != nil {
		r.log.WithField("op", "push.refs").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
		errs := make([]error, len(refs))
		for i := 0; i < len(refs); i++ {
			errs[i] = err
		}
		return errs
	}
	if err := firstError(errs); err != nil {
		r.log.WithField("op", "put.refs").WithField("class", r.class).
			WithField("shard", shard).Error(errs)
	}
	return errs
}

// simpleCommit generate commit function for the coordinator
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

func (r *Replicator) readSimpleResponse(x Result[SimpleResponse], successes []SimpleResponse, failures []SimpleResponse) ([]SimpleResponse, []SimpleResponse, bool, error) {
	var err error
	decreaseLevel := true
	if x.Err != nil {
		failures = append(failures, x.Value)
		if len(x.Value.Errors) == 0 {
			err = x.Err
		}
		decreaseLevel = false
	}
	return successes, failures, decreaseLevel, err
}

func (*Replicator) flattenErrors(batchSize int,
	rs []SimpleResponse,
	defaultErr error,
) []error {
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

func (*Replicator) flattenDeletions(batchSize int,
	rs []DeleteBatchResponse,
	defaultErr error,
) []objects.BatchSimpleObject {
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

func (r *Replicator) readDeleteBatchResponse(x Result[DeleteBatchResponse], successes []DeleteBatchResponse, failures []DeleteBatchResponse) ([]DeleteBatchResponse, []DeleteBatchResponse, bool, error) {
	var err error
	decreaseLevel := true
	if x.Err != nil {
		failures = append(failures, x.Value)
		if len(x.Value.Batch) == 0 {
			err = x.Err
		}
		decreaseLevel = false
	} else {
		successes = append(successes, x.Value)
	}
	return successes, failures, decreaseLevel, err
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

// requestID returns ID as [CoordinatorName-OpCode-TimeStamp-Counter].
// The coordinator uses it to uniquely identify a transaction.
// ID makes the request observable in the cluster by specifying its origin
// and the kind of replication request.
func (r *Replicator) requestID(op opID) string {
	return fmt.Sprintf("%s-%.2x-%x-%x",
		r.nodeName,
		op,
		time.Now().UnixMilli(),
		r.requestCounter.Add(1))
}
