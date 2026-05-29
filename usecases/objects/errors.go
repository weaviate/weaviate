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

package objects

import (
	"errors"
	"fmt"
)

// objects status code
const (
	StatusForbidden           = 403
	StatusBadRequest          = 400
	StatusNotFound            = 404
	StatusUnprocessableEntity = 422
	StatusInternalServerError = 500
)

type Error struct {
	Msg  string
	Code int
	Err  error
}

// Error implements error interface
func (e *Error) Error() string {
	return fmt.Sprintf("msg:%s code:%v err:%v", e.Msg, e.Code, e.Err)
}

// Unwrap underlying error
func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) NotFound() bool {
	return e.Code == StatusNotFound
}

func (e *Error) Forbidden() bool {
	return e.Code == StatusForbidden
}

func (e *Error) BadRequest() bool {
	return e.Code == StatusBadRequest
}

func (e *Error) UnprocessableEntity() bool {
	return e.Code == StatusUnprocessableEntity
}

// ErrInvalidUserInput indicates a client-side error
type ErrInvalidUserInput struct {
	msg string
}

func (e ErrInvalidUserInput) Error() string {
	return e.msg
}

// NewErrInvalidUserInput with Errorf signature
func NewErrInvalidUserInput(format string, args ...interface{}) ErrInvalidUserInput {
	return ErrInvalidUserInput{msg: fmt.Sprintf(format, args...)}
}

// ErrInternal indicates something went wrong during processing
type ErrInternal struct {
	msg string
}

func (e ErrInternal) Error() string {
	return e.msg
}

// NewErrInternal with Errorf signature
func NewErrInternal(format string, args ...interface{}) ErrInternal {
	return ErrInternal{msg: fmt.Sprintf(format, args...)}
}

// ErrNotFound indicates the desired resource doesn't exist
type ErrNotFound struct {
	msg string
}

func (e ErrNotFound) Error() string {
	return e.msg
}

// NewErrNotFound with Errorf signature
func NewErrNotFound(format string, args ...interface{}) ErrNotFound {
	return ErrNotFound{msg: fmt.Sprintf(format, args...)}
}

// ErrEndpointGone marks an operation that is no longer available in the
// current cluster configuration. The REST layer maps this to HTTP 410.
type ErrEndpointGone struct {
	msg string
}

func (e ErrEndpointGone) Error() string {
	return e.msg
}

func NewErrEndpointGone(format string, args ...interface{}) ErrEndpointGone {
	return ErrEndpointGone{msg: fmt.Sprintf(format, args...)}
}

type ErrMultiTenancy struct {
	err error
}

func (e ErrMultiTenancy) Error() string {
	return e.err.Error()
}

func (e ErrMultiTenancy) Unwrap() error {
	return e.err
}

// NewErrMultiTenancy with error signature
func NewErrMultiTenancy(err error) ErrMultiTenancy {
	return ErrMultiTenancy{err}
}

// This error is thrown by the replication logic when an object has either:
//
// 1. been deleted locally but exists remotely
//
// 2. been deleted remotely but exists locally
//
// signifying that the current operation is happening simultaneously to another operation
// on the same replicated resource.
//
// This error is used to bubble up the error from the replication logic so that it can be handled
// depending on the context of the higher level operation.
//
// This was introduced originally to handle
// cases where concurrent delete_many and single_patch operations were happening on the same object
// across multiple replicas. The read repair of the patch method would fail with a 500 conflict error
// if the delete operation was not propagated to all replicas before the patch operation was attempted.
// By using this error and handling it in func (m *Manager) MergeObject, any patch updates will assume that
// the object has been deleted everywhere, despite it only being deleted in one place, and will therefore
// return a 404 not found error.
type ErrDirtyReadOfDeletedObject struct {
	err error
}

func (e ErrDirtyReadOfDeletedObject) Error() string {
	return e.err.Error()
}

func (e ErrDirtyReadOfDeletedObject) Unwrap() error {
	return e.err
}

// It depends on the order of operations
//
// Created -> Deleted    => It is safe in this case to propagate deletion to all replicas
// Created -> Deleted -> Created => propagating deletion will result in data lost
//
// Updated -> Deleted => It is safe in this case to propagate deletion to all replicas
// Updated -> Deleted -> Updated => It is also safe in this case since updating a deleted object makes no logical sense
func NewErrDirtyReadOfDeletedObject(err error) ErrDirtyReadOfDeletedObject {
	return ErrDirtyReadOfDeletedObject{err}
}

type ErrDirtyWriteOfDeletedObject struct {
	err error
}

func (e ErrDirtyWriteOfDeletedObject) Error() string {
	return e.err.Error()
}

func (e ErrDirtyWriteOfDeletedObject) Unwrap() error {
	return e.err
}

func NewErrDirtyWriteOfDeletedObject(err error) ErrDirtyWriteOfDeletedObject {
	return ErrDirtyWriteOfDeletedObject{err}
}

// ErrConditionalCheckFailed is the sentinel value for errors.Is matching on any
// conditional-write precondition failure. Callers that only need to test whether
// an error is a precondition failure use errors.Is(err, ErrConditionalCheckFailed).
// Callers that need the structured detail (ObjectID, Reason, …) use errors.As.
var ErrConditionalCheckFailed = errors.New("conditional check failed")

// ErrPreconditionFailed is returned when a conditional write operation fails
// because its precondition was not satisfied (object already exists, version
// mismatch, or field-predicate mismatch). It wraps ErrConditionalCheckFailed so
// that errors.Is(err, ErrConditionalCheckFailed) returns true through any
// fmt.Errorf("%w", …) wrap chain.
type ErrPreconditionFailed struct {
	// ObjectID is the UUID of the object that failed the precondition check.
	ObjectID string

	// Reason is a human-readable description of why the precondition failed.
	Reason string

	// ExpectedVersion is the version the caller expected (Phase 2, version-CAS).
	// Zero means "not applicable for this condition kind."
	ExpectedVersion uint64

	// ActualVersion is the version the server observed (Phase 2).
	// Zero means "not applicable for this condition kind."
	ActualVersion uint64

	// PredicatePath is the dotted field path that was evaluated (Phase 3,
	// field-predicate update_if). Empty means "not applicable for this condition kind."
	PredicatePath string
}

// Error implements the error interface.
func (e *ErrPreconditionFailed) Error() string {
	return fmt.Sprintf("precondition failed for object %q: %s", e.ObjectID, e.Reason)
}

// Unwrap returns ErrConditionalCheckFailed so that errors.Is checks on the
// sentinel propagate through any fmt.Errorf("%w", err) wrap chain.
func (e *ErrPreconditionFailed) Unwrap() error {
	return ErrConditionalCheckFailed
}
