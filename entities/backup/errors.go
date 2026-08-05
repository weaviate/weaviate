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
//
// Names no shard: the text reaches API response bodies, and backing up a
// collection grants nothing on shard ids.
var ErrBackupBlockedByInFlightReindex = errors.New("backup blocked: runtime-reindex in flight")

// ErrBackupSpannedReindex marks a backup whose capture overlapped a
// runtime-reindex. Separate from [ErrBackupBlockedByInFlightReindex] because
// the migration has usually finished by the time this is raised, and calling it
// in-flight sends the operator after a task that is gone.
var ErrBackupSpannedReindex = errors.New("backup spanned a runtime-reindex")

// ReindexBlockedError is the API-safe form of a backup refused by the reindex
// gate. Wrappers on the way out add the shard and node an operator wants and a
// backup caller is not granted, so the publishable message travels alongside
// them and the boundary recovers it with errors.As.
type ReindexBlockedError struct{ Msg string }

func (e ReindexBlockedError) Error() string { return e.Msg }

func (e ReindexBlockedError) Unwrap() error { return ErrBackupBlockedByInFlightReindex }

// ErrReindexInFlight is the cluster-wide counterpart of
// [ErrBackupBlockedByInFlightReindex]. It names neither shard nor operation,
// so callers can frame it themselves (e.g. "restore blocked: ...").
var ErrReindexInFlight = errors.New("runtime-reindex in flight in the cluster")

// ErrReindexStateUnknown marks the refusal a node returns when it could
// not read cluster-wide reindex state at all: not "a reindex is running",
// but "no shard's state is known". It is never printed — the refusal's own
// message says what happened — and exists so the canCommit boundary can
// tell that message apart from a genuine per-shard refusal, and from an
// older node's message that carries no sentinel at all.
var ErrReindexStateUnknown = errors.New("runtime-reindex state unknown")

// CauseFirstRefusal is the shape of a refusal that must not open with
// [ErrBackupBlockedByInFlightReindex]'s own text: the message states what
// actually happened, and the sentinel stays reachable through Unwrap so
// errors.Is keeps matching across the canCommit RPC.
//
// The storage layer's unreachable-leader refusal implements it, and the
// stand-in used to drive the RPC boundary in usecases/backup asserts the
// same interface. Narrowing Unwrap on either one stops the other
// compiling, which is the point: the two must describe one contract.
type CauseFirstRefusal interface {
	error
	Unwrap() []error
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
