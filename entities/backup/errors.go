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

package backup

import (
	"context"
	"errors"
	"io"
	"strings"
)

// Match with errors.Is, never by substring. The texts name no shard and no node.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight")

var ErrReindexInFlight = errors.New("runtime-reindex in flight in the cluster")

// The gate could not answer. A separate sentinel from ErrReindexInFlight
// because "I could not check" and "a migration is running" send the
// operator to different places.
var ErrReindexActivityUndetermined = errors.New("restore blocked: whether a runtime-reindex is in flight could not be determined")

// The observed half of the commit-time pair; ErrReindexOverlapUndetermined is
// the half where the check could not answer.
var ErrReindexOverlappedBackup = errors.New("backup blocked: a runtime-reindex overlapped this backup")

// The other half: nothing observed says a migration overlapped, only that
// this capture cannot be cleared of one.
var ErrReindexOverlapUndetermined = errors.New("backup blocked: the runtime-reindex overlap could not be determined")

// The overlap check is installed but configured so that it could never clear
// a capture, so the backup is refused at admission instead of after its whole
// upload. Nothing is in flight, which is why this is not
// ErrBackupBlockedByInFlightReindex.
var ErrReindexOverlapCheckUnanswerable = errors.New("backup blocked: the runtime-reindex overlap check cannot answer")

type ReindexBlockedError struct {
	Msg string
}

func (e ReindexBlockedError) Error() string { return e.Msg }

func (e ReindexBlockedError) Unwrap() error { return ErrBackupBlockedByInFlightReindex }

// ReindexOverlapCheckError carries the refusing node's own text to the
// coordinator, which forwards it instead of rebuilding one: rebuilding would
// drop the two environment variables an operator has to change. The text
// names no node, shard or collection, so nothing needs redacting first.
type ReindexOverlapCheckError struct {
	Msg string
}

func (e ReindexOverlapCheckError) Error() string { return e.Msg }

func (e ReindexOverlapCheckError) Unwrap() error { return ErrReindexOverlapCheckUnanswerable }

// CancelSafeText rewords the phrase a coordinator reads as an operator abort:
// it relabels a FAILED participant CANCELLED on a text match with
// context.Canceled, and a CANCELLED id can be re-posted, so a quoted cancel
// would let a torn capture be silently overwritten by a clean one.
func CancelSafeText(text string) string {
	return strings.ReplaceAll(text, context.Canceled.Error(), "a canceled context")
}

// ReadCloserWithError extends io.ReadCloser with CloseWithError method.
// CloseWithError closes the reader and signals the given error to the writer,
// so the writer sees the actual error instead of a generic "closed pipe" error.
type ReadCloserWithError interface {
	io.ReadCloser
	CloseWithError(error) error
}

type ErrUnprocessable struct {
	err error
}

func (e ErrUnprocessable) Error() string {
	return e.err.Error()
}

func NewErrUnprocessable(err error) ErrUnprocessable {
	return ErrUnprocessable{err}
}

type ErrNotFound struct {
	err error
}

func (e ErrNotFound) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func NewErrNotFound(err error) ErrNotFound {
	return ErrNotFound{err}
}

type ErrContextExpired struct {
	err error
}

func (e ErrContextExpired) Error() string {
	return e.err.Error()
}

func NewErrContextExpired(err error) ErrContextExpired {
	return ErrContextExpired{err}
}

type ErrInternal struct {
	err error
}

func (e ErrInternal) Error() string {
	return e.err.Error()
}

func NewErrInternal(err error) ErrInternal {
	return ErrInternal{err}
}

func IsCancelled(err error, meta *DistributedBackupDescriptor) bool {
	if err == nil && meta.Status == Cancelled {
		return true
	}
	return false
}
