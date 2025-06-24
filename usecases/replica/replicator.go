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
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
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
	router interface {
		BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error)
		BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error)
		NodeHostname(nodeName string) (string, bool)
		AllHostnames() []string
	}

	// _Result represents a valid value or an error ( _ prevent make it public).
	_Result[T any] struct {
		Value T
		Err   error
	}
)

type Replicator struct {
	class          string
	nodeName       string
	router         router
	client         Client
	log            logrus.FieldLogger
	requestCounter atomic.Uint64
	stream         replicatorStream
	*Finder
}

func NewReplicator(className string,
	router router,
	nodeName string,
	getDeletionStrategy func() string,
	client Client,
	l logrus.FieldLogger,
) *Replicator {
	return &Replicator{
		class:    className,
		nodeName: nodeName,
		router:   router,
		client:   client,
		log:      l,
		Finder: NewFinder(
			className,
			router,
			nodeName,
			client,
			l,
			defaultPullBackOffInitialInterval,
			defaultPullBackOffMaxElapsedTime,
			getDeletionStrategy,
		),
	}
}

func (r *Replicator) AllHostnames() []string {
	return r.router.AllHostnames()
}

func (r *Replicator) PutObject(ctx context.Context,
	shard string,
	obj *storobj.Object,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObject), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, isReady, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.one").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)

	}
	err = r.stream.readErrors(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", obj.ID()).Error(err)
	}
	return err
}

func (r *Replicator) MergeObject(ctx context.Context,
	shard string,
	doc *objects.MergeDocument,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opMergeObject), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.merge").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	err = r.stream.readErrors(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "merge").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", doc.ID).Error(err)
		var replicaErr *Error
		if errors.As(err, &replicaErr) && replicaErr != nil && replicaErr.Code == StatusObjectNotFound {
			return objects.NewErrDirtyWriteOfDeletedObject(replicaErr)
		}
	}
	return err
}

func (r *Replicator) DeleteObject(ctx context.Context,
	shard string,
	id strfmt.UUID,
	deletionTime time.Time,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opDeleteObject), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.delete").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	err = r.stream.readErrors(1, level, replyCh)[0]
	if err != nil {
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", id).Error(err)
	}
	return err
}

func (r *Replicator) PutObjects(ctx context.Context,
	shard string,
	objs []*storobj.Object,
	l types.ConsistencyLevel,
	schemaVersion uint64,
) []error {
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opPutObjects), r.log)
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

	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
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
	errs := r.stream.readErrors(len(objs), level, replyCh)
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
	coord := newCoordinator[DeleteBatchResponse](r, shard, r.requestID(opDeleteObjects), r.log)
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

	replyCh, level, err := coord.Push(ctx, l, op, commit)
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
	rs := r.stream.readDeletions(len(uuids), level, replyCh)
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
	coord := newCoordinator[SimpleResponse](r, shard, r.requestID(opAddReferences), r.log)
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
	replyCh, level, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
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
	errs := r.stream.readErrors(len(refs), level, replyCh)
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
