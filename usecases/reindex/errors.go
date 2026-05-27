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
	// Maps to HTTP 400.
	ErrBadRequest = errors.New("reindex: bad request")

	// Maps to HTTP 404. Reserved for "collection or property does not
	// exist" — not for "nothing in flight" (that is an idempotent NO_OP).
	ErrNotFound = errors.New("reindex: not found")

	// Maps to HTTP 409.
	ErrConflict = errors.New("reindex: conflicting task in flight")

	// Maps to HTTP 503. Returned when the cluster service is not yet
	// wired up (typically during startup).
	ErrServiceUnavailable = errors.New("reindex: service unavailable")
)
