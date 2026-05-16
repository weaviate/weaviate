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

	"github.com/weaviate/weaviate/usecases/restrictions"
)

// restrictionViolationBody is the JSON body returned with HTTP 422 when
// a class create/update request specifies a configuration value outside
// the operator-configured allow-list. The structured fields
// (`errorCode`, `restriction`, `value`, `allowed`) are stable wire
// contract for SDK / UI consumers; the `message` text is rendered from
// the operator-overridable RESTRICTIONS_ERROR_MESSAGE template.
//
// Hand-written (not generated from openapi-specs) to avoid a swagger
// codegen pass for what is fundamentally a new typed error payload —
// the structured fields can be added to the OpenAPI spec separately
// without changing the wire format.
type restrictionViolationBody struct {
	ErrorCode   string   `json:"errorCode"`
	Restriction string   `json:"restriction"`
	Value       string   `json:"value"`
	Allowed     []string `json:"allowed"`
	Message     string   `json:"message"`
}

// newRestrictionViolationResponder builds a middleware.Responder that
// writes the canonical HTTP 422 + structured JSON body for a typed
// *restrictions.ViolationError. Used from every schema endpoint
// (single class create, class update, named-vector add) that can
// surface this error.
func newRestrictionViolationResponder(v *restrictions.ViolationError) middleware.Responder {
	body := &restrictionViolationBody{
		ErrorCode:   restrictions.ErrorCode,
		Restriction: string(v.Restriction),
		Value:       v.Value,
		Allowed:     append([]string(nil), v.Allowed...),
		Message:     v.Error(),
	}
	return middleware.ResponderFunc(func(rw http.ResponseWriter, _ runtime.Producer) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(rw).Encode(body)
	})
}
