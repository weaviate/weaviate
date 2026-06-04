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
// a backup attempt races a runtime-reindex on the same shard. The DTM unit
// driving the migration is not part of the backup payload, so a captured
// tracker dir cannot be safely restored.
//
// This sentinel lives in entities/backup so both the storage layer
// (adapters/repos/db) and the coordinator layer (usecases/backup) can
// share a single value without an import cycle. Match it across RPC
// boundaries with errors.Is, not substring comparison. The operator-visible
// error text wrapping this sentinel is owned by the storage layer in
// adapters/repos/db/reindex_inflight.go.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight on this shard")

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
