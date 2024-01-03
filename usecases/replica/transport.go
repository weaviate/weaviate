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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	// RequestKey is used to marshalling request IDs
	RequestKey       = "request_id"
	SchemaVersionKey = "schema_version"
)

// Client is used to read and write objects on replicas
type Client interface {
	rClient
	wClient
}

// StatusCode is communicate the cause of failure during replication
type StatusCode int

const (
	StatusOK            = 0
	StatusClassNotFound = iota + 200
	StatusShardNotFound
	StatusNotFound
	StatusAlreadyExisted
	StatusNotReady
	StatusConflict = iota + 300
	StatusPreconditionFailed
	StatusReadOnly
)

// Error reports error happening during replication
type Error struct {
	Code StatusCode `json:"code"`
	Msg  string     `json:"msg,omitempty"`
	Err  error      `json:"-"`
}

// Empty checks whether e is an empty error which equivalent to e == nil
func (e *Error) Empty() bool {
	return e.Code == StatusOK && e.Msg == "" && e.Err == nil
}

// NewError create new replication error
func NewError(code StatusCode, msg string) *Error {
	return &Error{code, msg, nil}
}

func (e *Error) Clone() *Error {
	return &Error{Code: e.Code, Msg: e.Msg, Err: e.Err}
}

// Unwrap underlying error
func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Error() string {
	return fmt.Sprintf("%s %q: %v", statusText(e.Code), e.Msg, e.Err)
}

func (e *Error) IsStatusCode(sc StatusCode) bool {
	return e.Code == sc
}

// statusText returns a text for the status code. It returns the empty
// string if the code is unknown.
func statusText(code StatusCode) string {
	switch code {
	case StatusOK:
		return "ok"
	case StatusNotFound:
		return "not found"
	case StatusClassNotFound:
		return "class not found"
	case StatusShardNotFound:
		return "shard not found"
	case StatusConflict:
		return "conflict"
	case StatusPreconditionFailed:
		return "precondition failed"
	case StatusAlreadyExisted:
		return "already existed"
	case StatusNotReady:
		return "local index not ready"
	case StatusReadOnly:
		return "read only"
	default:
		return ""
	}
}

func (e *Error) Timeout() bool {
	t, ok := e.Err.(interface {
		Timeout() bool
	})
	return ok && t.Timeout()
}

type SimpleResponse struct {
	Errors []Error `json:"errors,omitempty"`
}

func (r *SimpleResponse) FirstError() error {
	for i, err := range r.Errors {
		if !err.Empty() {
			return &r.Errors[i]
		}
	}
	return nil
}

// DeleteBatchResponse represents the response returned by DeleteObjects
type DeleteBatchResponse struct {
	Batch []UUID2Error `json:"batch,omitempty"`
}

type UUID2Error struct {
	UUID  string `json:"uuid,omitempty"`
	Error Error  `json:"error,omitempty"`
}

// FirstError returns the first found error
func (r *DeleteBatchResponse) FirstError() error {
	for i, ue := range r.Batch {
		if !ue.Error.Empty() {
			return &r.Batch[i].Error
		}
	}
	return nil
}

type RepairResponse struct {
	ID         string // object id
	Version    int64  // sender's current version of the object
	UpdateTime int64  // sender's current update time
	Err        string
	Deleted    bool
}

func fromReplicas(xs []objects.Replica) []*storobj.Object {
	rs := make([]*storobj.Object, len(xs))
	for i := range xs {
		rs[i] = xs[i].Object
	}
	return rs
}

type ObjectRange struct {
	InitialUUID strfmt.UUID `json:"initialUUID,omitempty"`
	FinalUUID   strfmt.UUID `json:"finalUUID,omitempty"`
	Limit       int         `json:"limit,omitempty"`
}

// wClient is the client used to write to replicas
type wClient interface {
	PutObject(ctx context.Context, host, index, shard, requestID string,
		obj *storobj.Object, schemaVersion uint64) (SimpleResponse, error)
	DeleteObject(ctx context.Context, host, index, shard, requestID string,
		id strfmt.UUID, schemaVersion uint64) (SimpleResponse, error)
	PutObjects(ctx context.Context, host, index, shard, requestID string,
		objs []*storobj.Object, schemaVersion uint64) (SimpleResponse, error)
	MergeObject(ctx context.Context, host, index, shard, requestID string,
		mergeDoc *objects.MergeDocument, schemaVersion uint64) (SimpleResponse, error)
	DeleteObjects(ctx context.Context, host, index, shard, requestID string,
		uuids []strfmt.UUID, dryRun bool, schemaVersion uint64) (SimpleResponse, error)
	AddReferences(ctx context.Context, host, index, shard, requestID string,
		refs []objects.BatchReference, schemaVersion uint64) (SimpleResponse, error)
	Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error
	Abort(ctx context.Context, host, index, shard, requestID string) (SimpleResponse, error)
}

// rClient is the client used to read from remote replicas
type rClient interface {
	// FetchObject fetches one object
	FetchObject(_ context.Context, host, index, shard string,
		id strfmt.UUID, props search.SelectProperties,
		additional additional.Properties) (objects.Replica, error)

	// FetchObjects fetches objects specified in ids list.
	FetchObjects(_ context.Context, host, index, shard string,
		ids []strfmt.UUID) ([]objects.Replica, error)

	// OverwriteObjects conditionally updates existing objects.
	OverwriteObjects(_ context.Context, host, index, shard string,
		_ []*objects.VObject) ([]RepairResponse, error)

	// DigestObjects finds a list of objects and returns a compact representation
	// of a list of the objects. This is used by the replicator to optimize the
	// number of bytes transferred over the network when fetching a replicated
	// object
	DigestObjects(ctx context.Context, host, index, shard string,
		ids []strfmt.UUID) ([]RepairResponse, error)

	DigestObjectsInRange(ctx context.Context, host, index, shard string,
		initialUUID, finalUUID uuid.UUID, limit int) ([]RepairResponse, error)

	HashTreeLevel(ctx context.Context, host, index, shard string, level int,
		discriminant *hashtree.Bitset) (digests []hashtree.Digest, err error)
}

// finderClient extends RClient with consistency checks
type finderClient struct {
	cl rClient
}

// FullRead reads full object
func (fc finderClient) FullRead(ctx context.Context,
	host, index, shard string,
	id strfmt.UUID,
	props search.SelectProperties,
	additional additional.Properties,
) (objects.Replica, error) {
	return fc.cl.FetchObject(ctx, host, index, shard, id, props, additional)
}

// DigestReads reads digests of all specified objects
func (fc finderClient) DigestReads(ctx context.Context,
	host, index, shard string,
	ids []strfmt.UUID,
) ([]RepairResponse, error) {
	n := len(ids)
	rs, err := fc.cl.DigestObjects(ctx, host, index, shard, ids)
	if err == nil && len(rs) != n {
		err = fmt.Errorf("malformed digest read response: length expected %d got %d", n, len(rs))
	}
	return rs, err
}

func (fc finderClient) DigestObjectsInRange(ctx context.Context,
	host, index, shard string,
	initialUUID, finalUUID uuid.UUID, limit int,
) ([]RepairResponse, error) {
	return fc.cl.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

// FullReads read full objects
func (fc finderClient) FullReads(ctx context.Context,
	host, index, shard string,
	ids []strfmt.UUID,
) ([]objects.Replica, error) {
	n := len(ids)
	rs, err := fc.cl.FetchObjects(ctx, host, index, shard, ids)
	if m := len(rs); err == nil && n != m {
		err = fmt.Errorf("malformed full read response: length expected %d got %d", n, m)
	}
	return rs, err
}

// Overwrite specified object with most recent contents
func (fc finderClient) Overwrite(ctx context.Context,
	host, index, shard string,
	xs []*objects.VObject,
) ([]RepairResponse, error) {
	return fc.cl.OverwriteObjects(ctx, host, index, shard, xs)
}
