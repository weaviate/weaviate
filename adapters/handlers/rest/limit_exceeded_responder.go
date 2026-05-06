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

package rest

import (
	"encoding/json"
	"net/http"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// usageLimitErrorBody is the wire shape of the HTTP 429 response body for
// usage-limit failures. Field names are stable contract — Cloud SDKs and
// load balancers rely on errorCode being machine-readable while message is
// operator-overridable via USAGE_LIMITS_ERROR_MESSAGE.
type usageLimitErrorBody struct {
	ErrorCode string `json:"errorCode"`
	Limit     string `json:"limit"`
	Value     int64  `json:"value"`
	Message   string `json:"message"`
}

// limitExceededResponder is a middleware.Responder that emits HTTP 429
// with the structured USAGE_LIMIT_EXCEEDED body. Used by every REST handler
// path where a usecase may return *usagelimits.LimitExceededError. We do
// not depend on go-swagger generated constructors here because adding 429
// to every affected endpoint in the OpenAPI spec would be invasive (and
// unstable across regen runs); the manual responder gives us the exact
// shape the RFC promises with one line per handler.
type limitExceededResponder struct {
	err *usagelimits.LimitExceededError
}

func newLimitExceededResponder(err *usagelimits.LimitExceededError) middleware.Responder {
	return &limitExceededResponder{err: err}
}

func (r *limitExceededResponder) WriteResponse(rw http.ResponseWriter, _ runtime.Producer) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusTooManyRequests)
	body := usageLimitErrorBody{
		ErrorCode: usagelimits.ErrorCode,
		Limit:     string(r.err.Limit),
		Value:     r.err.Value,
		Message:   r.err.Error(),
	}
	_ = json.NewEncoder(rw).Encode(body)
}
