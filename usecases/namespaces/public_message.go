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

package namespaces

import "errors"

// PublicMessage returns the user-facing text for a namespace lifecycle
// sentinel. Every caller outside the namespace-management API must render
// this instead of err: it names neither the namespace nor the concept. ok is
// false for errors that are not lifecycle sentinels, so callers keep the
// detail of a genuine internal failure.
func PublicMessage(err error) (msg string, ok bool) {
	switch {
	case errors.Is(err, ErrNamespaceSuspended), errors.Is(err, ErrCollectionSuspended):
		return "instance suspended", true
	case errors.Is(err, ErrNamespaceResuming):
		return "instance resuming, retry shortly", true
	case errors.Is(err, ErrNamespaceGone), errors.Is(err, ErrNamespaceDeleting),
		errors.Is(err, ErrInvalidState), errors.Is(err, ErrNamespaceNotEmpty),
		errors.Is(err, ErrInvalidStateTransition), errors.Is(err, ErrNotFound):
		return "instance unavailable", true
	default:
		return "", false
	}
}
