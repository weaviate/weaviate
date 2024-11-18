//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
)

// NewWeaviateWellknownLivenessParams creates a new WeaviateWellknownLivenessParams object
//
// There are no default values defined in the spec.
func NewWeaviateWellknownLivenessParams() WeaviateWellknownLivenessParams {

	return WeaviateWellknownLivenessParams{}
}

// WeaviateWellknownLivenessParams contains all the bound params for the weaviate wellknown liveness operation
// typically these are obtained from a http.Request
//
// swagger:parameters weaviate.wellknown.liveness
type WeaviateWellknownLivenessParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewWeaviateWellknownLivenessParams() beforehand.
func (o *WeaviateWellknownLivenessParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
