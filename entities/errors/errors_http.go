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

package errors

import (
	"errors"
	"fmt"
)

type ErrUnprocessable struct {
	err error
}

func (e ErrUnprocessable) Error() string {
	return e.err.Error()
}

// Unwrap exposes the cause, so callers classify with errors.Is/As instead of matching the message
func (e ErrUnprocessable) Unwrap() error {
	return e.err
}

// ErrLocalIndexNotFound is a class this node does not hold yet, so its schema has not caught up.
// The message is unchanged, so callers still matching on text keep working.
type ErrLocalIndexNotFound struct {
	Index string
}

func (e ErrLocalIndexNotFound) Error() string {
	return fmt.Sprintf("local index %q not found", e.Index)
}

// ErrLocalShardNotFound is a shard this node does not hold yet, so its schema has not caught up.
// For a multi-tenant class the shard is the tenant. The message is unchanged, so callers still
// matching on text keep working.
type ErrLocalShardNotFound struct {
	Shard string
}

func (e ErrLocalShardNotFound) Error() string {
	return fmt.Sprintf("local %s shard not found", e.Shard)
}

// ErrNotServedHere is an index or shard missing from a node whose schema is already at or past
// the version the request was resolved against. Waiting cannot make it appear. It is
// deliberately not an ErrLocalIndexNotFound or ErrLocalShardNotFound, which read as "not caught
// up yet": answering that here is what keeps a coordinator retrying a replica that never will.
type ErrNotServedHere struct {
	Index   string
	Shard   string
	Version uint64
}

func (e ErrNotServedHere) Error() string {
	if e.Shard == "" {
		return fmt.Sprintf("local index %q not found at schema version %d", e.Index, e.Version)
	}
	return fmt.Sprintf("local index %q has no shard %q at schema version %d", e.Index, e.Shard, e.Version)
}

// IsSchemaLag reports whether err is an index or shard that is not here yet, rather than a
// fault. [ErrNotServedHere] deliberately does not match: it is a miss that waiting cannot fix.
func IsSchemaLag(err error) bool {
	var missingIndex ErrLocalIndexNotFound
	var missingShard ErrLocalShardNotFound
	return errors.As(err, &missingIndex) || errors.As(err, &missingShard)
}

// NotServedHere reports whether a missing local index or shard is final: this node's schema is
// already at or past the version the read was resolved against, so waiting cannot make it
// appear and the caller must re-resolve rather than retry. The caller answers such a miss with
// [ErrNotServedHere], which the cluster API turns into a 422 that nothing retries.
//
// appliedIndex is the comparator rather than the local class version because it only advances
// once an entry's store side has run: comparing class versions would call a read that races a
// tenant's creation final. wantVersion 0 is a caller too old to send one, so lag cannot be
// ruled out.
//
// Everything else, a miss while still behind included, must be left exactly as it was: lag
// resolves on its own, and the retry that outlasts schema propagation is what keeps a read
// correct while an entry is still reaching every replica.
func NotServedHere(err error, wantVersion, appliedIndex uint64) bool {
	return wantVersion > 0 && appliedIndex >= wantVersion && IsSchemaLag(err)
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
