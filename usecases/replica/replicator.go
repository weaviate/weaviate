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
	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, isReady, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.one").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	handleResp := func(resp pushResponse[SimpleResponse]) error {
		return r.stream.readErrors(1, resp.level, resp.commitCh)[0]
	}
	logEntry := r.log.WithField("op", "put").WithField("class", r.class).
		WithField("shard", shard).WithField("uuid", obj.ID())
	return handlePushResponses(pushResp, additionalHostsPushResp, handleResp, logEntry)
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
	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.merge").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	handleResp := func(resp pushResponse[SimpleResponse]) error {
		pushErr := r.stream.readErrors(1, pushResp.level, pushResp.commitCh)[0]
		if pushErr != nil {
			var replicaErr *Error
			if errors.As(pushErr, &replicaErr) && replicaErr != nil && replicaErr.Code == StatusObjectNotFound {
				return objects.NewErrDirtyWriteOfDeletedObject(replicaErr)
			}
		}
		return pushErr
	}
	logEntry := r.log.WithField("op", "merge").WithField("class", r.class).
		WithField("shard", shard).WithField("uuid", doc.ID)
	return handlePushResponses(pushResp, additionalHostsPushResp, handleResp, logEntry)
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
	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
	if err != nil {
		r.log.WithField("op", "push.delete").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
	}
	handleResp := func(resp pushResponse[SimpleResponse]) error {
		return r.stream.readErrors(1, resp.level, resp.commitCh)[0]
	}
	logEntry := r.log.WithField("op", "delete").WithField("class", r.class).
		WithField("shard", shard).WithField("uuid", id)
	return handlePushResponses(pushResp, additionalHostsPushResp, handleResp, logEntry)
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

	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
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
	handleResp := func(resp pushResponse[SimpleResponse]) []error {
		errs := r.stream.readErrors(len(objs), resp.level, resp.commitCh)
		return errs
	}
	return handleBatchPushResponses(
		pushResp,
		additionalHostsPushResp,
		handleResp,
		r.log.WithField("op", "put.many").WithField("class", r.class).WithField("shard", shard),
	)
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

	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, op, commit)
	if err != nil {
		r.log.WithField("op", "push.deletes").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", msgCLevel, l, errReplicas)
		errs := make([]objects.BatchSimpleObject, len(uuids))
		for i := 0; i < len(uuids); i++ {
			errs[i].Err = err
		}
		return errs
	}
	rs := r.stream.readDeletions(len(uuids), pushResp.level, pushResp.commitCh)
	if err := firstBatchError(rs); err != nil {
		r.log.WithField("op", "put.deletes").WithField("class", r.class).
			WithField("shard", shard).Error(rs)
		return rs
	}
	if additionalHostsPushResp != nil {
		additionalHostRs := r.stream.readDeletions(len(uuids), additionalHostsPushResp.level, additionalHostsPushResp.commitCh)
		if additionalHostErr := firstBatchError(additionalHostRs); additionalHostErr != nil {
			r.log.WithField("op", "put.deletes").WithField("class", r.class).
				WithField("shard", shard).Error(additionalHostRs)
		}
		if additionalHostsPushResp.requireSuccess {
			return additionalHostRs
		}
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
	pushResp, additionalHostsPushResp, err := coord.Push(ctx, l, op, r.simpleCommit(shard))
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
	handleResp := func(resp pushResponse[SimpleResponse]) []error {
		return r.stream.readErrors(len(refs), resp.level, resp.commitCh)
	}
	return handleBatchPushResponses(
		pushResp,
		additionalHostsPushResp,
		handleResp,
		r.log.WithField("op", "put.refs").WithField("class", r.class).WithField("shard", shard),
	)
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

func handlePushResponses[T any](
	pushResp pushResponse[T],
	additionalHostsPushResp *pushResponse[T],
	respHandler func(resp pushResponse[T]) error,
	logEntry *logrus.Entry,
) error {
	checkResp := func(resp pushResponse[T]) error {
		checkErr := respHandler(resp)
		if checkErr != nil {
			logEntry.WithFields(logrus.Fields{
				"additionalHosts": false,
				"requireSuccess":  resp.requireSuccess,
				"level":           resp.level,
			}).Error(checkErr)
			if resp.requireSuccess {
				return checkErr
			}
		}
		return nil
	}
	err := checkResp(pushResp)
	if err != nil {
		return err
	}
	if additionalHostsPushResp != nil {
		return checkResp(*additionalHostsPushResp)
	}
	return nil
}

// handleBatchPushResponses handles a batch of push responses.
// respHandler should always return a slice of errors whose length is the same as the number of objects in the batch
func handleBatchPushResponses[T any](
	pushResp pushResponse[T],
	additionalHostsPushResp *pushResponse[T],
	respHandler func(resp pushResponse[T]) []error,
	logEntry *logrus.Entry,
) []error {
	checkResp := func(resp pushResponse[T]) []error {
		checkErrs := respHandler(resp)
		if err := firstError(checkErrs); err != nil {
			logEntry.WithFields(logrus.Fields{
				"additionalHosts": false,
				"requireSuccess":  resp.requireSuccess,
				"level":           resp.level,
			}).Error(checkErrs)
			if resp.requireSuccess {
				return checkErrs
			}
		}
		// return nil for all objects in batch
		return make([]error, len(checkErrs))
	}
	errs := checkResp(pushResp)
	if err := firstError(errs); err != nil {
		return errs
	}
	if additionalHostsPushResp != nil {
		return checkResp(*additionalHostsPushResp)
	}
	return errs
}
