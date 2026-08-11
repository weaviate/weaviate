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

// ErrBackupBlockedByInFlightReindex is the canonical sentinel returned when
// a backup attempt races a runtime-reindex on the same shard; the DTM unit
// driving the migration is not part of the backup payload, so the tracker
// dir cannot be safely restored.
//
// Lives here so the storage layer (adapters/repos/db) and the coordinator
// layer (usecases/backup) share one value without an import cycle. Match
// with errors.Is, not substring comparison. Names no shard: the text
// reaches API response bodies.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight")

// ReindexBlockedError is the API-safe form of a backup refused by the reindex
// gate. Wrappers on the way out add the shard and node an operator wants and a
// backup caller is not granted, so the publishable message travels alongside
// them and the boundary recovers it with errors.As.
type ReindexBlockedError struct{ Msg string }

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
