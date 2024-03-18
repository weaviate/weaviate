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

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsClassPutHandlerFunc turns a function with the right signature into a objects class put handler
type ObjectsClassPutHandlerFunc func(ObjectsClassPutParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsClassPutHandlerFunc) Handle(params ObjectsClassPutParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsClassPutHandler interface for that can handle valid objects class put params
type ObjectsClassPutHandler interface {
	Handle(ObjectsClassPutParams, *models.Principal) middleware.Responder
}

// NewObjectsClassPut creates a new http.Handler for the objects class put operation
func NewObjectsClassPut(ctx *middleware.Context, handler ObjectsClassPutHandler) *ObjectsClassPut {
	return &ObjectsClassPut{Context: ctx, Handler: handler}
}

/*
	ObjectsClassPut swagger:route PUT /objects/{className}/{id} objects objectsClassPut

Update an object.

Update an object based on its uuid and collection.
*/
type ObjectsClassPut struct {
	Context *middleware.Context
	Handler ObjectsClassPutHandler
}

func (o *ObjectsClassPut) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewObjectsClassPutParams()
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
