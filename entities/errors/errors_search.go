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

// Typed search-path errors: they wrap without altering the message and
// implement Unwrap, so handlers classify with errors.As and nested
// sentinels stay reachable.

// ErrNoVectorizerModule: the search needs server-side vectorization but the
// collection (or target vector) has no vectorizer module configured.
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
// Check ErrNoVectorizerModule first — it surfaces wrapped inside this one.
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

// ErrCertaintyIncompatible marks a certainty request against a non-cosine
// vector index.
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
