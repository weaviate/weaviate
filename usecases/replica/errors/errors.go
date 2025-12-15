//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/cluster/router/types"
)

// sentinel errors (unexported) to support errors.Is checks while exposing
// only constructor and predicate helpers from this package.
var (
	errReplicas = errors.New("cannot achieve consistency level")
	errRepair   = errors.New("read repair error")
	errRead     = errors.New("read error")

	errNoDiffFound = errors.New("no diff found")
)

// NewReplicasError creates a new error for "cannot reach enough replicas".
func NewReplicasError(err error, level ...types.ConsistencyLevel) error {
	if err == nil {
		return errReplicas
	}
	if len(level) > 0 {
		return fmt.Errorf("%w: level %q: %w", errReplicas, level[0], err)
	}
	return fmt.Errorf("%w: %w", errReplicas, err)
}

func IsReplicasError(err error) bool {
	if errors.Is(err, errReplicas) {
		return true
	}
	// This check for string also because it has been misused in the past
	// as a defensive measure.
	return strings.Contains(err.Error(), errReplicas.Error())
}

// NewRepairError creates a new error for "read repair error".
func NewRepairError(err error) error {
	if err == nil {
		return errRepair
	}
	return fmt.Errorf("%w: %w", errRepair, err)
}

func IsRepairError(err error) bool {
	return errors.Is(err, errRepair)
}

// NewReadError creates a new error for "read error".
func NewReadError(err error) error {
	if err == nil {
		return errRead
	}
	return fmt.Errorf("%w: %w", errRead, err)
}

func IsReadError(err error) bool {
	return errors.Is(err, errRead)
}

// NewNoDiffFoundError creates a new error for "no diff found".
func NewNoDiffFoundError(err error) error {
	if err == nil {
		return errNoDiffFound
	}
	return fmt.Errorf("%w: %w", errNoDiffFound, err)
}

// IsNoDiffFoundError reports whether err represents the "no diff found" condition.
func IsNoDiffFoundError(err error) bool {
	return errors.Is(err, errNoDiffFound)
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
	StatusObjectNotFound
)

// StatusText returns a text for the status code. It returns the empty
// string if the code is unknown.
func StatusText(code StatusCode) string {
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
	case StatusObjectNotFound:
		return "object not found"
	default:
		return ""
	}
}

// // NewError create new replication error
// func NewError(code StatusCode, msg string) *Error {
// 	return &Error{code, msg, nil}
// }

// NewClassNotFoundError creates a new error for class not found
func NewClassNotFoundError(err error) *Error {
	return &Error{Code: StatusClassNotFound, Msg: StatusText(StatusClassNotFound), Err: err}
}

// NewShardNotFoundError creates a new error for shard not found
func NewShardNotFoundError(err error) *Error {
	return &Error{Code: StatusShardNotFound, Msg: StatusText(StatusShardNotFound), Err: err}
}

// NewNotFoundError creates a new error for not found
func NewNotFoundError(err error) *Error {
	return &Error{Code: StatusNotFound, Msg: StatusText(StatusNotFound), Err: err}
}

// NewAlreadyExistedError creates a new error for already existed
func NewAlreadyExistedError(err error) *Error {
	return &Error{Code: StatusAlreadyExisted, Msg: StatusText(StatusAlreadyExisted), Err: err}
}

// NewNotReadyError creates a new error for not ready
func NewNotReadyError(err error) *Error {
	return &Error{Code: StatusNotReady, Msg: StatusText(StatusNotReady), Err: err}
}

// NewConflictError creates a new error for conflict
func NewConflictError(err error) *Error {
	return &Error{Code: StatusConflict, Msg: StatusText(StatusConflict), Err: err}
}

// NewPreconditionFailedError creates a new error for precondition failed
func NewPreconditionFailedError(err error) *Error {
	return &Error{Code: StatusPreconditionFailed, Msg: StatusText(StatusPreconditionFailed), Err: err}
}

// NewReadOnlyError creates a new error for read only
func NewReadOnlyError(err error) *Error {
	return &Error{Code: StatusReadOnly, Msg: StatusText(StatusReadOnly), Err: err}
}

// NewObjectNotFoundError creates a new error for object not found
func NewObjectNotFoundError(err error) *Error {
	return &Error{Code: StatusObjectNotFound, Msg: StatusText(StatusObjectNotFound), Err: err}
}

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

func (e *Error) Clone() *Error {
	return &Error{Code: e.Code, Msg: e.Msg, Err: e.Err}
}

// Unwrap underlying error
func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Error() string {
	return fmt.Sprintf("%s %q: %v", StatusText(e.Code), e.Msg, e.Err)
}

func (e *Error) IsStatusCode(sc StatusCode) bool {
	return e.Code == sc
}

func (e *Error) Timeout() bool {
	t, ok := e.Err.(interface {
		Timeout() bool
	})
	return ok && t.Timeout()
}
