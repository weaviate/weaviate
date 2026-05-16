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

package restrictions

import (
	"errors"
	"fmt"
	"strings"
)

// RestrictionName identifies which restriction was violated. Values are
// stable wire-level identifiers used in the JSON `restriction` field and
// in gRPC error details.
type RestrictionName string

const (
	RestrictionVectorIndexType RestrictionName = "vector_index_type"
	RestrictionCompression     RestrictionName = "compression"
)

// ErrorCode is the machine-stable identifier returned in the JSON
// `errorCode` field of the HTTP 422 response and as Reason on the
// gRPC FailedPrecondition error details. Operators / SDKs match on
// this value rather than on free-text messages.
const ErrorCode = "CONFIG_NOT_ALLOWED"

// ViolationError is the typed error returned when a class create or
// update request specifies a configuration value that is outside the
// operator-configured allow-list. The HTTP layer maps it to 422
// Unprocessable Entity with body
//
//	{"errorCode":"CONFIG_NOT_ALLOWED","restriction":"...","value":"...","allowed":[...],"message":"..."}
//
// and the gRPC layer maps it to codes.FailedPrecondition with the same
// fields attached as ErrorInfo metadata. The error itself is
// wire-protocol-agnostic.
type ViolationError struct {
	// Restriction identifies which allow-list rejected the value.
	Restriction RestrictionName
	// Value is the offending value the request submitted (e.g. "flat",
	// "pq"). Always a single string — never the whole allow-list.
	Value string
	// Allowed is the operator-configured allow-list at the time of
	// rejection, returned to the caller so SDKs/UIs can surface the
	// valid options without an extra round-trip.
	Allowed []string
	// RenderedMessage is the operator-overridable message rendered from
	// RESTRICTIONS_ERROR_MESSAGE with {restriction}, {value} and
	// {allowed} substituted.
	RenderedMessage string
}

func (e *ViolationError) Error() string {
	if e == nil {
		return ""
	}
	if e.RenderedMessage != "" {
		return e.RenderedMessage
	}
	return fmt.Sprintf("config not allowed: %s=%q not in allow-list [%s]",
		e.Restriction, e.Value, strings.Join(e.Allowed, ", "))
}

// AsViolation reports whether err (or any error in its chain) is a
// *ViolationError, returning the typed value when so. Convenience for the
// HTTP/gRPC error mappers.
func AsViolation(err error) (*ViolationError, bool) {
	if err == nil {
		return nil, false
	}
	var e *ViolationError
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// NewViolationError builds a *ViolationError with its message rendered
// from the given template via RenderTemplate. Pass an empty template to
// fall back to the default. This is a free function (not a Manager
// method) so callers (e.g. the schema Handler) don't need to depend on
// a Manager type — mirrors usagelimits.NewLimitExceededError.
func NewViolationError(template string, restriction RestrictionName, value string, allowed []string) *ViolationError {
	return &ViolationError{
		Restriction:     restriction,
		Value:           value,
		Allowed:         allowed,
		RenderedMessage: RenderTemplate(template, restriction, value, allowed),
	}
}
