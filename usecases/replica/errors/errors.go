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
	"strings"
)

// MsgCLevel is the human-readable prefix used in consistency-level errors.
const MsgCLevel = "cannot achieve consistency level"

// Sentinel errors – use errors.Is / errors.As to inspect wrapped values.
var (
	// ErrReplicas is the canonical sentinel for "not enough replicas were
	// reachable".  It is intentionally kept simple so that callers can rely
	// on errors.Is for detection while richer wrapper types carry the
	// underlying cause.
	ErrReplicas = errors.New("cannot reach enough replicas")

	// ErrRepair is the canonical sentinel for read-repair failures.
	ErrRepair = errors.New("read repair error")

	// ErrRead is the canonical sentinel for replica read failures.
	ErrRead = errors.New("read error")

	ErrNoDiffFound = errors.New("no diff found")

	// ErrConflictExistOrDeleted is returned during read-repair when an object
	// exists on one replica but has been deleted on another.
	ErrConflictExistOrDeleted = errors.New("conflict: object has been deleted on another replica")

	// ErrConflictObjectChanged is returned when the source object changed
	// between the digest read and the repair write.
	ErrConflictObjectChanged = errors.New("source object changed during repair")
)

// notEnoughReplicasError is returned by the coordinator when it cannot reach
// a sufficient number of replicas to satisfy the requested consistency level.
// It preserves the first underlying cause and tracks how many additional
// errors were observed (without retaining them) so memory remains O(1).
//
// Use errors.Is(err, ErrReplicas) for detection and errors.Unwrap / errors.As
// to inspect the underlying cause.
type notEnoughReplicasError struct {
	required   int   // total replicas required to satisfy the consistency level
	got        int   // number of replicas that succeeded
	cause      error // first error observed from a failed replica (may be nil)
	additional int   // count of further errors observed beyond cause
}

// NewNotEnoughReplicasError wraps cause in an error that satisfies
// errors.Is(err, ErrReplicas).  Use this form when only the underlying
// error is known (e.g. a routing-plan failure); for richer diagnostics
// from the coordinator use NewNotEnoughReplicasErrorWithCounts.
func NewNotEnoughReplicasError(cause error) error {
	return &notEnoughReplicasError{cause: cause}
}

// NewNotEnoughReplicasErrorWithCounts is the rich form of
// NewNotEnoughReplicasError used by the coordinator when it knows how
// many replicas were required vs. succeeded.  additional is the count of
// further errors observed beyond the first one (cause); the count is kept
// rather than the errors themselves so memory remains O(1).
func NewNotEnoughReplicasErrorWithCounts(required, got int, cause error, additional int) error {
	return &notEnoughReplicasError{
		required:   required,
		got:        got,
		cause:      cause,
		additional: additional,
	}
}

func (e *notEnoughReplicasError) Error() string {
	var sb strings.Builder
	sb.WriteString(ErrReplicas.Error())
	if e.required > 0 {
		fmt.Fprintf(&sb, ": required %d replicas, got %d", e.required, e.got)
	}
	if e.cause != nil {
		fmt.Fprintf(&sb, ": %v", e.cause)
	}
	if e.additional > 0 {
		fmt.Fprintf(&sb, " (and %d more errors)", e.additional)
	}
	return sb.String()
}

// Unwrap returns the underlying cause so that errors.Is / errors.As can
// traverse the chain.
func (e *notEnoughReplicasError) Unwrap() error { return e.cause }

// Is makes errors.Is(err, ErrReplicas) match any notEnoughReplicasError,
// preserving backward-compatible error detection for callers that check for
// the sentinel directly.
func (e *notEnoughReplicasError) Is(target error) bool {
	return target == ErrReplicas
}

// readError wraps the underlying cause of a replica read failure while
// remaining detectable via errors.Is(err, ErrRead).
type readError struct {
	cause error
}

// NewReadError constructs an error that satisfies errors.Is(err, ErrRead)
// and wraps cause.
func NewReadError(cause error) error {
	return &readError{cause: cause}
}

func (e *readError) Error() string {
	if e.cause != nil {
		return ErrRead.Error() + ": " + e.cause.Error()
	}
	return ErrRead.Error()
}

func (e *readError) Unwrap() error { return e.cause }

func (e *readError) Is(target error) bool {
	return target == ErrRead
}

// repairError wraps the underlying cause of a read-repair failure while
// remaining detectable via errors.Is(err, ErrRepair).
type repairError struct {
	cause error
}

// NewRepairError constructs an error that satisfies errors.Is(err, ErrRepair)
// and wraps cause.
func NewRepairError(cause error) error {
	return &repairError{cause: cause}
}

func (e *repairError) Error() string {
	if e.cause != nil {
		return ErrRepair.Error() + ": " + e.cause.Error()
	}
	return ErrRepair.Error()
}

func (e *repairError) Unwrap() error { return e.cause }

func (e *repairError) Is(target error) bool {
	return target == ErrRepair
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
