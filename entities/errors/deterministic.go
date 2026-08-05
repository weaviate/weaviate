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

import "errors"

// ErrDeterministic marks a write-path error as deterministic: produced purely
// by the operation's own content against the replicated schema, so every
// replica applying the same log entry fails identically. The shard-raft apply
// path skips a deterministically-failed item on every replica alike (counted
// and logged); every UNMARKED error is treated as environmental and parked
// (retried indefinitely). The polarity is deliberate: a missing mark parks
// loudly instead of skipping — annoying, never lossy — so only sites whose
// determinism is beyond doubt (validation of the object's own content,
// marshalling, key derivation) may carry the mark.
var ErrDeterministic = errors.New("deterministic apply error")

// Deterministic marks err as deterministic without altering its message.
// errors.Is(result, ErrDeterministic) reports true; the original error chain
// stays reachable through Unwrap for all other sentinel checks.
func Deterministic(err error) error {
	if err == nil {
		return nil
	}
	return &deterministicError{err: err}
}

// IsDeterministic reports whether err is marked deterministic anywhere in its
// chain.
func IsDeterministic(err error) bool {
	return errors.Is(err, ErrDeterministic)
}

type deterministicError struct{ err error }

func (e *deterministicError) Error() string { return e.err.Error() }

func (e *deterministicError) Unwrap() error { return e.err }

// Is makes the mark visible to errors.Is without the sentinel's text ever
// appearing in the message.
func (e *deterministicError) Is(target error) bool { return target == ErrDeterministic }
