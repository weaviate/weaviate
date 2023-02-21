//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsPatchHandlerFunc turns a function with the right signature into a objects patch handler
type ObjectsPatchHandlerFunc func(ObjectsPatchParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsPatchHandlerFunc) Handle(params ObjectsPatchParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsPatchHandler interface for that can handle valid objects patch params
type ObjectsPatchHandler interface {
	Handle(ObjectsPatchParams, *models.Principal) middleware.Responder
}

// NewObjectsPatch creates a new http.Handler for the objects patch operation
func NewObjectsPatch(ctx *middleware.Context, handler ObjectsPatchHandler) *ObjectsPatch {
	return &ObjectsPatch{Context: ctx, Handler: handler}
}

/*
	ObjectsPatch swagger:route PATCH /objects/{id} objects objectsPatch

Update an Object based on its UUID (using patch semantics).

Updates an Object. This method supports json-merge style patch semantics (RFC 7396). Provided meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
type ObjectsPatch struct {
	Context *middleware.Context
	Handler ObjectsPatchHandler
}

func (o *ObjectsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	Params := NewObjectsPatchParams()
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
