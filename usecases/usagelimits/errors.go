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

package usagelimits

import (
	"errors"
	"fmt"
)

// LimitName identifies which limit was hit. Values are stable wire-level
// identifiers used in the JSON `limit` field and in gRPC error details.
type LimitName string

const (
	LimitObjects     LimitName = "objects"
	LimitCollections LimitName = "collections"
	LimitTenants     LimitName = "tenants"
	LimitShards      LimitName = "shards"
)

// ErrorCode is the machine-stable identifier returned in the JSON
// `errorCode` field of the HTTP 429 response and as a prefix in gRPC
// RESOURCE_EXHAUSTED error details. Operators / SDKs match on this value
// rather than on free-text messages.
const ErrorCode = "USAGE_LIMIT_EXCEEDED"

// LimitExceededError is returned by Manager.Check* when an operation would
// exceed a configured limit. The HTTP layer maps this to 429 Too Many
// Requests with body
//
//	{"errorCode":"USAGE_LIMIT_EXCEEDED","limit":"...","value":N,"message":"..."}
//
// and the gRPC layer maps it to codes.ResourceExhausted with the same fields
// attached as structured details. The error itself is wire-protocol-agnostic.
type LimitExceededError struct {
	// Limit identifies which limit was hit. Stable wire identifier.
	Limit LimitName
	// Value is the configured threshold (the cap, not the current count).
	Value int64
	// RenderedMessage is the operator-overridable message rendered from
	// USAGE_LIMITS_ERROR_MESSAGE with {limit} and {value} substituted.
	RenderedMessage string
}

func (e *LimitExceededError) Error() string {
	if e == nil {
		return ""
	}
	if e.RenderedMessage != "" {
		return e.RenderedMessage
	}
	return fmt.Sprintf("usage limit exceeded: %s count limit of %d reached", e.Limit, e.Value)
}

// AsLimitExceeded reports whether err (or any error in its chain) is a
// *LimitExceededError, returning the typed value when so. Convenience for the
// HTTP/gRPC error mappers.
func AsLimitExceeded(err error) (*LimitExceededError, bool) {
	if err == nil {
		return nil, false
	}
	var e *LimitExceededError
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}
