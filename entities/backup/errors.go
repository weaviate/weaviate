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
	"errors"
	"io"
)

// Match with errors.Is, never by substring. The texts name no shard and no node.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight")

var ErrReindexInFlight = errors.New("runtime-reindex in flight in the cluster")

// The gate could not answer. Separate from ErrReindexInFlight because "I could not check" and "a migration is running" send the operator elsewhere.
var ErrReindexActivityUndetermined = errors.New("restore blocked: whether a runtime-reindex is in flight could not be determined")

// The backup side of the same answer; the restore sentinel's text opens with "restore blocked:" and cannot be reused.
var ErrBackupReindexActivityUndetermined = errors.New("backup blocked: whether a runtime-reindex is in flight could not be determined")

var ErrBackupSpannedReindex = errors.New("backup blocked: a runtime-reindex overlapped this backup")

// Shares no wording with ErrBackupSpannedReindex: there may be none to find.
var ErrReindexOverlapUndetermined = errors.New("backup blocked: the runtime-reindex overlap could not be determined")

type ReindexBlockedError struct {
	Msg string
}

func (e ReindexBlockedError) Error() string { return e.Msg }

func (e ReindexBlockedError) Unwrap() error { return ErrBackupBlockedByInFlightReindex }

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
