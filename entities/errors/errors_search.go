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

// Typed search-path errors. They wrap the original error without altering its
// message, so API handlers can classify failures with errors.As instead of
// matching message substrings. All implement Unwrap so sentinels deeper in
// the chain (e.g. an ErrNoVectorizerModule inside an ErrQueryVectorization)
// stay reachable.

// ErrNoVectorizerModule marks a search that requires server-side
// vectorization on a collection (or target vector) with no vectorizer module
// configured: a valid request against an unrunnable configuration.
type ErrNoVectorizerModule struct {
	err error
}

func (e ErrNoVectorizerModule) Error() string {
	return e.err.Error()
}

func (e ErrNoVectorizerModule) Unwrap() error {
	return e.err
}

func NewErrNoVectorizerModule(err error) ErrNoVectorizerModule {
	return ErrNoVectorizerModule{err}
}

// ErrQueryVectorization marks a failure to turn the query into a vector.
// Check for ErrNoVectorizerModule first: a missing-vectorizer configuration
// error also surfaces through the vectorization call path, wrapped inside an
// ErrQueryVectorization.
type ErrQueryVectorization struct {
	err error
}

func (e ErrQueryVectorization) Error() string {
	return e.err.Error()
}

func (e ErrQueryVectorization) Unwrap() error {
	return e.err
}

func NewErrQueryVectorization(err error) ErrQueryVectorization {
	return ErrQueryVectorization{err}
}

// ErrCertaintyIncompatible marks a certainty request against a vector index
// whose distance metric is not cosine, where certainty cannot be computed.
type ErrCertaintyIncompatible struct {
	err error
}

func (e ErrCertaintyIncompatible) Error() string {
	return e.err.Error()
}

func (e ErrCertaintyIncompatible) Unwrap() error {
	return e.err
}

func NewErrCertaintyIncompatible(err error) ErrCertaintyIncompatible {
	return ErrCertaintyIncompatible{err}
}
