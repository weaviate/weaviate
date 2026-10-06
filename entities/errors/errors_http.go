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

import "fmt"

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
// the version the request was resolved against. Waiting cannot make it appear, so the caller
// must re-resolve where the data lives rather than retry this node. It is deliberately not an
// ErrLocalIndexNotFound or ErrLocalShardNotFound: those read as "not caught up yet", and
// answering that here is what keeps a coordinator retrying a replica that will never serve.
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
