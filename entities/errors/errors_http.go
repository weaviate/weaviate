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

// HeaderSchemaLag marks a 503 this node sent because its schema is behind rather than because it
// cannot serve at all. A header rather than the body, so a client classifies without matching
// prose, and only this response family carries it.
const HeaderSchemaLag = "X-Weaviate-Schema-Lag"

// ErrLocalShardNotFound is a shard this node does not hold yet, so its schema has not caught up.
// For a multi-tenant class the shard is the tenant. The message is unchanged, so callers still
// matching on text keep working.
type ErrLocalShardNotFound struct {
	Shard string
}

func (e ErrLocalShardNotFound) Error() string {
	return fmt.Sprintf("local %s shard not found", e.Shard)
}

// ErrNotServedHere is an index or shard missing from a node already at or past the version the
// request was resolved against, so waiting cannot make it appear. Deliberately not an
// ErrLocalIndexNotFound or ErrLocalShardNotFound: those read as lag, which keeps a coordinator
// retrying a replica that never will.
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

// NotServedHere reports whether err is the [ErrNotServedHere] condition. appliedIndex must be
// the FSM's, the counter WaitForAppliedIndex compares. wantVersion 0 is a sender too old.
//
// Being caught up does not on its own make a miss final: restore, freeze, COLD-to-HOT activation
// and lazy loading all materialise shards outside the apply path, so the schema reads current
// while the shard is still arriving. assignedHere settles it from placement, and answers true
// whenever placement cannot be read, because a miss wrongly called final stops the retry.
func NotServedHere(err error, wantVersion, appliedIndex uint64, assignedHere func() bool) bool {
	return wantVersion > 0 && appliedIndex >= wantVersion && IsSchemaLag(err) && !assignedHere()
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
