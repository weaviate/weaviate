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
// Editing this file might prove futile when you re-run the generate command

import (
	"context"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// GetReplicationStatusReplicaRequestHandlerFunc turns a function with the right signature into a get replication status replica request handler
type GetReplicationStatusReplicaRequestHandlerFunc func(GetReplicationStatusReplicaRequestParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn GetReplicationStatusReplicaRequestHandlerFunc) Handle(params GetReplicationStatusReplicaRequestParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// GetReplicationStatusReplicaRequestHandler interface for that can handle valid get replication status replica request params
type GetReplicationStatusReplicaRequestHandler interface {
	Handle(GetReplicationStatusReplicaRequestParams, *models.Principal) middleware.Responder
}

// NewGetReplicationStatusReplicaRequest creates a new http.Handler for the get replication status replica request operation
func NewGetReplicationStatusReplicaRequest(ctx *middleware.Context, handler GetReplicationStatusReplicaRequestHandler) *GetReplicationStatusReplicaRequest {
	return &GetReplicationStatusReplicaRequest{Context: ctx, Handler: handler}
}

/*
	GetReplicationStatusReplicaRequest swagger:route GET /replication/{id}/status getReplicationStatusReplicaRequest

# Get the status of a shard replica move operation

Returns the status of a shard replica move operation for a given shard, identified by the provided id.
*/
type GetReplicationStatusReplicaRequest struct {
	Context *middleware.Context
	Handler GetReplicationStatusReplicaRequestHandler
}

func (o *GetReplicationStatusReplicaRequest) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewGetReplicationStatusReplicaRequestParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}

// GetReplicationStatusReplicaRequestOKBody get replication status replica request o k body
//
// swagger:model GetReplicationStatusReplicaRequestOKBody
type GetReplicationStatusReplicaRequestOKBody struct {

	// The current status of the shard replica move operation
	// Required: true
	Status *string `json:"status" yaml:"status"`
}

// Validate validates this get replication status replica request o k body
func (o *GetReplicationStatusReplicaRequestOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *GetReplicationStatusReplicaRequestOKBody) validateStatus(formats strfmt.Registry) error {

	if err := validate.Required("getReplicationStatusReplicaRequestOK"+"."+"status", "body", o.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this get replication status replica request o k body based on context it is used
func (o *GetReplicationStatusReplicaRequestOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *GetReplicationStatusReplicaRequestOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GetReplicationStatusReplicaRequestOKBody) UnmarshalBinary(b []byte) error {
	var res GetReplicationStatusReplicaRequestOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
