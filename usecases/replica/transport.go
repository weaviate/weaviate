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

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

const (
	// RequestKey is used to marshalling request IDs
	RequestKey = "request_id"
)

type StatusCode int

const (
	StatusOK            = 0
	StatusClassNotFound = iota + 200
	StatusShardNotFound
	StatusNotFound
	StatusAlreadyExisted
	StatusConflict = iota + 300
	StatusPreconditionFailed
	StatusReadOnly
)

// Error reports error happing during replication
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

func (e *Error) Error() string { return fmt.Sprintf("%s %q: %v", statusText(e.Code), e.Msg, e.Err) }

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

type Client interface {
	PutObject(ctx context.Context, host, index, shard, requestID string,
		obj *storobj.Object) (SimpleResponse, error)
	DeleteObject(ctx context.Context, host, index, shard, requestID string,
		id strfmt.UUID) (SimpleResponse, error)
	PutObjects(ctx context.Context, host, index, shard, requestID string,
		objs []*storobj.Object) (SimpleResponse, error)
	MergeObject(ctx context.Context, host, index, shard, requestID string,
		mergeDoc *objects.MergeDocument) (SimpleResponse, error)
	DeleteObjects(ctx context.Context, host, index, shard, requestID string,
		docIDs []uint64, dryRun bool) (SimpleResponse, error)
	AddReferences(ctx context.Context, host, index, shard, requestID string,
		refs []objects.BatchReference) (SimpleResponse, error)
	Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error
	Abort(ctx context.Context, host, index, shard, requestID string) (SimpleResponse, error)
}
