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

package reindex

import "errors"

// Sentinel errors the service returns so HTTP handlers can map them to
// the right status code without parsing message text. Wrap with
// [fmt.Errorf] when adding context; callers use [errors.Is] for the
// dispatch.
var (
	// ErrBadRequest is the default failure mode for validation errors
	// — invalid body shape, property mismatch, unsupported data type.
	// Maps to HTTP 400.
	ErrBadRequest = errors.New("reindex: bad request")

	// ErrNotFound indicates the collection, property, or in-flight
	// task does not exist. Maps to HTTP 404.
	ErrNotFound = errors.New("reindex: not found")

	// ErrConflict indicates a competing in-flight task already targets
	// the same (collection, property, index type) tuple. Maps to
	// HTTP 409.
	ErrConflict = errors.New("reindex: conflicting task in flight")

	// ErrServiceUnavailable indicates the cluster service is not yet
	// wired up (typically during startup) or has reported an
	// unrecoverable read failure. Maps to HTTP 503.
	ErrServiceUnavailable = errors.New("reindex: service unavailable")
)
