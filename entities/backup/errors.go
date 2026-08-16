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

// The gate could not answer. Separate from ErrReindexInFlight because "I could not check" and "a migration is running" send the operator elsewhere.
var ErrReindexActivityUndetermined = errors.New("restore blocked: whether a runtime-reindex is in flight could not be determined")

// The backup side of the same answer; the restore sentinel's text opens with "restore blocked:" and cannot be reused.
var ErrBackupReindexActivityUndetermined = errors.New("backup blocked: whether a runtime-reindex is in flight could not be determined")

// The observed half of the commit-time pair; ErrReindexOverlapUndetermined is
// the half where the check could not answer.
var ErrReindexOverlappedBackup = errors.New("backup blocked: a runtime-reindex overlapped this backup")

// The other half: this capture cannot be cleared. An observed overlap can be
// standing behind it - an unjudgeable record outranks one and drops its finding.
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
// coordinator, which forwards it whole rather than rebuilding it (see
// overlapCheckUnanswerableByParticipant). It names no node, shard or
// collection, so nothing needs redacting first.
type ReindexOverlapCheckError struct {
	Msg string
}

func (e ReindexOverlapCheckError) Error() string { return e.Msg }

func (e ReindexOverlapCheckError) Unwrap() error { return ErrReindexOverlapCheckUnanswerable }

// CancelSafeText rewords the phrase a coordinator reads as an operator abort:
// it relabels a FAILED participant CANCELLED on a text match with
// context.Canceled, and a CANCELLED id can be re-posted, so a quoted cancel
// would let a torn capture be silently overwritten by a clean one.
//
// One occurrence per pass, because the replacement ends in the phrase's own
// first word: a single sweep turns "context canceled canceled" into "a
// canceled context canceled", which quotes it again. It cannot spin - each
// pass moves the leftmost match 11 bytes on while the string grows by 2.
func CancelSafeText(text string) string {
	phrase := context.Canceled.Error()
	for strings.Contains(text, phrase) {
		text = strings.Replace(text, phrase, "a canceled context", 1)
	}
	return text
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
