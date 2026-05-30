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
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
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

// coordVersionLockStripes is the number of stripes in the coordinator-scoped
// per-(shard, UUID) lock pool. A fixed-width pool bounds memory and avoids
// map-churn on hot keys. 64 stripes give 1/64 collision probability for a
// uniform UUID distribution; contention on the same stripe is the documented
// cross-coordinator KNOWN-WEAK boundary, not a same-coordinator issue.
const coordVersionLockStripes = 64

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
	// coordVersionLock is a fixed-stripe per-(shard, UUID) mutex pool that
	// serialises same-coordinator concurrent writes for the same object through
	// the read → CAS-compare → mint → coord.Push critical section.
	//
	// The lock must be held across coord.Push (not just across the mint) because
	// the second writer's digest read must observe the first writer's committed
	// version. Holding the lock until coord.Push returns (post-commit at the
	// request CL) guarantees the quorum intersection property: writer-2 reads
	// from a quorum that necessarily overlaps writer-1's commit quorum.
	//
	// Cross-coordinator contention (two nodes, two lock instances) is the
	// documented Plan-A KNOWN-WEAK boundary and resolves via LWW. It is NOT
	// serialised by this lock.
	coordVersionLock [coordVersionLockStripes]sync.Mutex
	*Finder
}

// coordVersionLockFor returns the mutex stripe for the given shard+UUID
// combination. The stripe index is stable for the lifetime of the Replicator.
func (r *Replicator) coordVersionLockFor(shard string, id strfmt.UUID) *sync.Mutex {
	h := fnv.New32a()
	_, _ = fmt.Fprint(h, shard, "|", id)
	return &r.coordVersionLock[h.Sum32()%coordVersionLockStripes]
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
	// Coordinator-authoritative version mint + IfVersion CAS (Plan-A, §1-3).
	//
	// Acquire the per-(shard, UUID) coordinator lock BEFORE the digest read so
	// that two same-coordinator goroutines cannot interleave their
	// read → CAS-compare → mint. The lock is held through coord.Push so that
	// writer-2's subsequent digest read observes writer-1's committed version
	// (quorum intersection: writer-1 commits at CL l before releasing the lock,
	// writer-2 reads at CL l and overlaps writer-1's commit quorum).
	//
	// INV-HA-1: resolveCurrentVersion uses l (the request CL), never ALL.
	if obj != nil && obj.ID() != "" {
		mu := r.coordVersionLockFor(shard, obj.ID())
		mu.Lock()
		defer mu.Unlock()

		current, err := r.Finder.resolveCurrentVersion(ctx, l, shard, obj.ID())
		if err != nil {
			return fmt.Errorf("resolve current version for mint: %w", err)
		}

		if obj.Conditional.IfVersion != nil && *obj.Conditional.IfVersion != current {
			r.log.WithField("op", "put").WithField("class", r.class).
				WithField("shard", shard).WithField("uuid", obj.ID()).
				Debugf("coordinator CAS mismatch: expected version %d, actual %d",
					*obj.Conditional.IfVersion, current)
			return &objects.ErrPreconditionFailed{
				ObjectID:        obj.ID().String(),
				Reason:          fmt.Sprintf("object version mismatch: expected %d, actual %d", *obj.Conditional.IfVersion, current),
				ExpectedVersion: *obj.Conditional.IfVersion,
				ActualVersion:   current,
			}
		}

		obj.Version = current + 1
		obj.MarshallerVersion = storobj.CurrentMarshallerVersion
	}

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
	rs, err := coord.Push(ctx, l, isReady, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.one").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
	}
	if err := firstError(rs); err != nil {
		if precondErr := asPreconditionFailed(err); precondErr != nil {
			// Precondition failures are expected, client-driven outcomes (e.g.
			// insert_if_not_exists on an already-existing UUID). Log at debug so
			// hot-key conditional workloads do not flood error dashboards.
			r.log.WithField("op", "put").WithField("class", r.class).
				WithField("shard", shard).WithField("uuid", obj.ID()).Debugf("precondition not met: %v", precondErr)
			return precondErr
		}
		r.log.WithField("op", "put").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", obj.ID()).Errorf("put object: %v", err)
		return err
	}
	return nil
}

// asPreconditionFailed inspects err for a replicaerrors.Error carrying
// StatusPreconditionFailed and, if found, synthesises an
// *objects.ErrPreconditionFailed so the caller receives the domain-typed error
// rather than the wire-level replica error.
func asPreconditionFailed(err error) *objects.ErrPreconditionFailed {
	var re *replicaerrors.Error
	if errors.As(err, &re) && re.IsStatusCode(replicaerrors.StatusPreconditionFailed) {
		return &objects.ErrPreconditionFailed{Reason: re.Msg}
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
	rs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.merge").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
	}
	if err := firstError(rs); err != nil {
		r.log.WithField("op", "merge").WithField("class", r.class).
			WithField("shard", shard).WithField("uuid", doc.ID).Error(err)
		var replicaErr *replicaerrors.Error
		if errors.As(err, &replicaErr) && replicaErr.Code == replicaerrors.StatusObjectNotFound {
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
	rs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, 1)
	if err != nil {
		r.log.WithField("op", "push.delete").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		return fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
	}
	if err := firstError(rs); err != nil {
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
	rs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, len(objs))
	if err != nil {
		r.log.WithField("op", "push.many").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
		errs := make([]error, len(objs))
		for i := 0; i < len(objs); i++ {
			errs[i] = err
		}
		return errs
	}
	if err := firstError(rs); err != nil {
		r.log.WithField("op", "put.many").WithField("class", r.class).
			WithField("shard", shard).Error(rs)
	}
	return rs
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
		err = fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
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
	rs, err := coord.Push(ctx, l, op, r.simpleCommit(shard), r.readSimpleResponse, r.flattenErrors, len(refs))
	if err != nil {
		r.log.WithField("op", "push.refs").WithField("class", r.class).
			WithField("shard", shard).Error(err)
		err = fmt.Errorf("%s %q: %w", replicaerrors.MsgCLevel, l, replicaerrors.NewNotEnoughReplicasError(err))
		errs := make([]error, len(refs))
		for i := 0; i < len(refs); i++ {
			errs[i] = err
		}
		return errs
	}
	if err := firstError(rs); err != nil {
		r.log.WithField("op", "put.refs").WithField("class", r.class).
			WithField("shard", shard).Error(rs)
	}
	return rs
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
