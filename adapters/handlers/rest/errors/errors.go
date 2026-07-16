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
	"net/http"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// HTTPStatusForNamespaceErr maps a namespace/collection lifecycle sentinel
// to its REST status: the terminal family (deleting, not-empty,
// invalid-state, suspended) renders 422, a resuming namespace renders 503.
// ok is false for anything else, so the caller falls through to its own
// default.
func HTTPStatusForNamespaceErr(err error) (status int, ok bool) {
	switch {
	case errors.Is(err, namespaces.ErrNamespaceResuming):
		return http.StatusServiceUnavailable, true
	case errors.Is(err, namespaces.ErrNamespaceDeleting),
		errors.Is(err, namespaces.ErrNamespaceNotEmpty),
		errors.Is(err, namespaces.ErrInvalidState),
		errors.Is(err, namespaces.ErrNamespaceSuspended),
		errors.Is(err, namespaces.ErrCollectionSuspended):
		return http.StatusUnprocessableEntity, true
	}
	return 0, false
}

// ErrPayloadFromSingleErr builds a single-message ErrorResponse with the
// principal's own namespace prefix stripped from err. Pass nil for global
// callers to leave the message unchanged. A nil err is tolerated and yields
// fmt's standard "<nil>" rendering rather than panicking, since this helper
// sits on dozens of REST error paths and a missed err-guard upstream should
// not crash the handler.
func ErrPayloadFromSingleErr(principal *models.Principal, err error) *models.ErrorResponse {
	return &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: namespacing.StripErrorMessage(principal, fmt.Sprintf("%v", err)),
	}}}
}

// restrictionViolationFromErr wraps a non-restriction error into the
// same RestrictionViolationResponse shape so handlers can return both
// 422 cases through the same swagger-generated payload type.
func ErrRestrictionViolation(principal *models.Principal, err error) *models.RestrictionViolationResponse {
	return &models.RestrictionViolationResponse{
		Error: []*models.RestrictionViolationResponseErrorItems0{{
			Message: namespacing.StripErrorMessage(principal, fmt.Sprintf("%v", err)),
		}},
	}
}
