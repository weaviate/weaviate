//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/semi-technologies/weaviate/models"
)

// ObjectsCreateHandlerFunc turns a function with the right signature into a objects create handler
type ObjectsCreateHandlerFunc func(ObjectsCreateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsCreateHandlerFunc) Handle(params ObjectsCreateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsCreateHandler interface for that can handle valid objects create params
type ObjectsCreateHandler interface {
	Handle(ObjectsCreateParams, *models.Principal) middleware.Responder
}

// NewObjectsCreate creates a new http.Handler for the objects create operation
func NewObjectsCreate(ctx *middleware.Context, handler ObjectsCreateHandler) *ObjectsCreate {
	return &ObjectsCreate{Context: ctx, Handler: handler}
}

/*
ObjectsCreate swagger:route POST /objects objects objectsCreate

Create Objects between two Objects (object and subject).

Registers a new Object. Provided meta-data and schema values are validated.
*/
type ObjectsCreate struct {
	Context *middleware.Context
	Handler ObjectsCreateHandler
}

func (o *ObjectsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewObjectsCreateParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
