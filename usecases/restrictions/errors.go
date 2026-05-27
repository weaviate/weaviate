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

// RestrictionName is the stable wire identifier for which restriction
// rejected a value. Surfaces in JSON / gRPC error metadata.
type RestrictionName string

const (
	RestrictionVectorIndexType RestrictionName = "vector_index_type"
	RestrictionCompression     RestrictionName = "compression"
)

// ErrorCode is the stable machine identifier for restriction violations
// in the HTTP body (`errorCode`) and the gRPC FailedPrecondition Reason.
const ErrorCode = "CONFIG_NOT_ALLOWED"

// ViolationError is the wire-protocol-agnostic error raised when a class
// config violates an operator allow-list. The HTTP layer maps to 422 +
// RestrictionViolationResponse; the gRPC layer maps to FailedPrecondition
// with the same fields as ErrorInfo metadata.
type ViolationError struct {
	Restriction     RestrictionName
	Value           string
	Allowed         []string
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

// AsViolation unwraps err to a *ViolationError if its chain contains one.
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
// from the template (empty = default). Free function (no Manager type)
// so callers don't pick up an extra dependency.
func NewViolationError(template string, restriction RestrictionName, value string, allowed []string) *ViolationError {
	return &ViolationError{
		Restriction:     restriction,
		Value:           value,
		Allowed:         allowed,
		RenderedMessage: RenderTemplate(template, restriction, value, allowed),
	}
}
