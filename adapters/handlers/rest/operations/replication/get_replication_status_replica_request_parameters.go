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

package replication

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

// NewGetReplicationStatusReplicaRequestParams creates a new GetReplicationStatusReplicaRequestParams object
//
// There are no default values defined in the spec.
func NewGetReplicationStatusReplicaRequestParams() GetReplicationStatusReplicaRequestParams {

	return GetReplicationStatusReplicaRequestParams{}
}

// GetReplicationStatusReplicaRequestParams contains all the bound params for the get replication status replica request operation
// typically these are obtained from a http.Request
//
// swagger:parameters getReplicationStatusReplicaRequest
type GetReplicationStatusReplicaRequestParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*The shard replica move operation id to get the status for
	  Required: true
	  In: path
	*/
	ID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewGetReplicationStatusReplicaRequestParams() beforehand.
func (o *GetReplicationStatusReplicaRequestParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	rID, rhkID, _ := route.Params.GetOK("id")
	if err := o.bindID(rID, rhkID, route.Formats); err != nil {
		res = append(res, err)
	}
	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindID binds and validates parameter ID from path.
func (o *GetReplicationStatusReplicaRequestParams) bindID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.ID = raw

	return nil
}
