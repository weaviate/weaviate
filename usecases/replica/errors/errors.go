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

// Package errors centralises error types and sentinel values for the
// replication subsystem.  All packages that need to produce or inspect
// replica errors should import this package rather than defining their own.
package errors

import (
	"errors"
	"fmt"
)

// MsgCLevel is the human-readable prefix used in consistency-level errors.
const MsgCLevel = "cannot achieve consistency level"

// Sentinel errors – use errors.Is / errors.As to inspect wrapped values.
var (
	// ErrReplicas is the canonical sentinel for "not enough replicas were
	// reachable".  It is intentionally kept simple so that callers can rely
	// on errors.Is for detection while the richer NotEnoughReplicasError
	// type carries the underlying cause.
	ErrReplicas = errors.New("cannot reach enough replicas")

	ErrRepair      = errors.New("read repair error")
	ErrRead        = errors.New("read error")
	ErrNoDiffFound = errors.New("no diff found")

	// ErrConflictExistOrDeleted is returned during read-repair when an object
	// exists on one replica but has been deleted on another.
	ErrConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// ErrConflictObjectChanged is returned when the source object changed
	// between the digest read and the repair write.
	ErrConflictObjectChanged = errors.New("source object changed during repair")
)

// NotEnoughReplicasError is returned by the coordinator when it cannot reach
// a sufficient number of replicas to satisfy the requested consistency level.
// It wraps the underlying per-replica causes so that callers can inspect
// what went wrong, while still being detectable via errors.Is(err, ErrReplicas).
type NotEnoughReplicasError struct {
	cause error
}

// NewNotEnoughReplicasError constructs a NotEnoughReplicasError that wraps
// cause.  cause may be nil when no underlying diagnostic is available.
func NewNotEnoughReplicasError(cause error) error {
	return &NotEnoughReplicasError{cause: cause}
}

func (e *NotEnoughReplicasError) Error() string {
	if e.cause != nil {
		return ErrReplicas.Error() + ": " + e.cause.Error()
	}
	return ErrReplicas.Error()
}

// Unwrap returns the underlying cause so that errors.Is / errors.As can
// traverse the chain.
func (e *NotEnoughReplicasError) Unwrap() error { return e.cause }

// Is makes errors.Is(err, ErrReplicas) match any NotEnoughReplicasError,
// preserving backward-compatible error detection for callers that check for
// the sentinel directly.
func (e *NotEnoughReplicasError) Is(target error) bool {
	return target == ErrReplicas
}

// ---------------------------------------------------------------------------
// StatusCode and Error – transport-level error representation
// ---------------------------------------------------------------------------

// StatusCode communicates the cause of failure during replication.
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

// StatusText returns a text for the status code. It returns the empty string
// if the code is unknown.
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

// Error reports an error that occurred during replication.  It is used as a
// wire-level type and is intentionally JSON-serialisable.
type Error struct {
	Code StatusCode `json:"code"`
	Msg  string     `json:"msg,omitempty"`
	Err  error      `json:"-"`
}

// NewError creates a new replication error with the given code and message.
func NewError(code StatusCode, msg string) *Error {
	return &Error{code, msg, nil}
}

// Empty reports whether e is semantically nil (no code, no message, no
// underlying error), which is equivalent to the absence of an error.
func (e *Error) Empty() bool {
	return e.Code == StatusOK && e.Msg == "" && e.Err == nil
}

// Clone returns a shallow copy of e.
func (e *Error) Clone() *Error {
	return &Error{Code: e.Code, Msg: e.Msg, Err: e.Err}
}

// Unwrap returns the underlying error so that errors.Is / errors.As can
// traverse the chain.
func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Error() string {
	return fmt.Sprintf("%s %q: %v", StatusText(e.Code), e.Msg, e.Err)
}

// IsStatusCode reports whether e carries the given status code.
func (e *Error) IsStatusCode(sc StatusCode) bool {
	return e.Code == sc
}

// Timeout reports whether the underlying error is a timeout.
func (e *Error) Timeout() bool {
	t, ok := e.Err.(interface {
		Timeout() bool
	})
	return ok && t.Timeout()
}
