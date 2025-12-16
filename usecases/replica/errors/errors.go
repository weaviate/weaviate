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
	if err != nil {
		return strings.Contains(err.Error(), errReplicas.Error())
	}
	return false
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
	_StatusCodeOK        = 0
	_StatusClassNotFound = iota + 200
	_StatusShardNotFound
	_StatusNotFound
	_StatusAlreadyExisted
	_StatusNotReady
	_StatusConflict = iota + 300
	_StatusPreconditionFailed
	_StatusReadOnly
	_StatusObjectNotFound
)

// StatusText returns a text for the status code. It returns the empty
// string if the code is unknown.
func StatusText(code StatusCode) string {
	switch code {
	case _StatusCodeOK:
		return "ok"
	case _StatusNotFound:
		return "not found"
	case _StatusClassNotFound:
		return "class not found"
	case _StatusShardNotFound:
		return "shard not found"
	case _StatusConflict:
		return "conflict"
	case _StatusPreconditionFailed:
		return "precondition failed"
	case _StatusAlreadyExisted:
		return "already existed"
	case _StatusNotReady:
		return "local index not ready"
	case _StatusReadOnly:
		return "read only"
	case _StatusObjectNotFound:
		return "object not found"
	default:
		return ""
	}
}

// NewClassNotFoundError creates a new error for class not found.
// The msg parameter should contain the class name.
func NewClassNotFoundError(className string, err ...error) *Error {
	if len(err) > 0 {
		return &Error{
			Code: _StatusClassNotFound,
			Msg:  className,
			Err:  err[0],
		}
	}

	return &Error{
		Code: _StatusClassNotFound,
		Msg:  className,
		Err:  fmt.Errorf("class %q not found", className),
	}
}

// NewShardNotFoundError creates a new error for shard not found.
// The msg parameter should contain the shard name.
func NewShardNotFoundError(shardName string, err ...error) *Error {
	if len(err) > 0 {
		return &Error{
			Code: _StatusShardNotFound,
			Msg:  shardName,
			Err:  err[0],
		}
	}

	return &Error{Code: _StatusShardNotFound, Msg: shardName, Err: fmt.Errorf("shard %q not found", shardName)}
}

// NewNotFoundError creates a new error for a generic "not found".
// The msg parameter should describe what was not found.
func NewNotFoundError(notFound string, err ...error) *Error {
	if len(err) > 0 {
		return &Error{
			Code: _StatusNotFound,
			Msg:  notFound,
			Err:  err[0],
		}
	}

	return &Error{
		Code: _StatusNotFound,
		Msg:  notFound,
		Err:  fmt.Errorf("%s not found", notFound),
	}
}

// NewAlreadyExistedError creates a new error for already existed
// The msg parameter should contain the already existed name.
func NewAlreadyExistedError(className string) *Error {
	return &Error{
		Code: _StatusAlreadyExisted,
		Msg:  className,
		Err:  fmt.Errorf("%s already existed", className),
	}
}

// NewNotReadyError creates a new error for not ready
// The msg parameter should contain the collection name.
func NewNotReadyError(className string) *Error {
	return &Error{
		Code: _StatusNotReady,
		Msg:  className,
		Err:  fmt.Errorf("class %q not ready", className),
	}
}

func IsNotReadyError(err error) bool {
	replicaErr := &Error{}
	errors.As(err, &replicaErr)
	if replicaErr != nil && replicaErr.Code == _StatusNotReady {
		return true
	}
	return false
}

// NewConflictError creates a new error for conflict
func NewConflictError(err error) *Error {
	return &Error{Code: _StatusConflict, Msg: StatusText(_StatusConflict), Err: err}
}

// NewPreconditionFailedError creates a new error for precondition failed
func NewPreconditionFailedError(err error) *Error {
	return &Error{Code: _StatusPreconditionFailed, Msg: StatusText(_StatusPreconditionFailed), Err: err}
}

// NewReadOnlyError creates a new error for read only
// The msg parameter should contain the read only name.
func NewReadOnlyError(className string, err ...error) *Error {
	if len(err) > 0 {
		return &Error{
			Code: _StatusReadOnly,
			Msg:  className,
			Err:  err[0],
		}
	}

	return &Error{Code: _StatusReadOnly, Msg: className, Err: fmt.Errorf("class %q is read only", className)}
}

// NewObjectNotFoundError creates a new error for object not found
func NewObjectNotFoundError(err error) *Error {
	return &Error{Code: _StatusObjectNotFound, Msg: StatusText(_StatusObjectNotFound), Err: err}
}
func IsObjectNotFoundError(err error) bool {
	replicaErr := &Error{}
	errors.As(err, &replicaErr)
	if replicaErr != nil && replicaErr.Code == _StatusObjectNotFound {
		return true
	}
	return false
}

// Error reports error happening during replication
type Error struct {
	Code StatusCode `json:"code"`
	Msg  string     `json:"msg,omitempty"`
	Err  error      `json:"-"`
}

func NewReplicationError(err error) *Error {
	replicaErr := &Error{}
	errors.As(err, &replicaErr)
	return replicaErr
}

// Empty checks whether e is an empty error which equivalent to e == nil
func (e *Error) Empty() bool {
	return e.Code == _StatusCodeOK && e.Msg == "" && e.Err == nil
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
