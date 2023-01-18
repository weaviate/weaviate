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

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsClassReferencesCreateHandlerFunc turns a function with the right signature into a objects class references create handler
type ObjectsClassReferencesCreateHandlerFunc func(ObjectsClassReferencesCreateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsClassReferencesCreateHandlerFunc) Handle(params ObjectsClassReferencesCreateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsClassReferencesCreateHandler interface for that can handle valid objects class references create params
type ObjectsClassReferencesCreateHandler interface {
	Handle(ObjectsClassReferencesCreateParams, *models.Principal) middleware.Responder
}

// NewObjectsClassReferencesCreate creates a new http.Handler for the objects class references create operation
func NewObjectsClassReferencesCreate(ctx *middleware.Context, handler ObjectsClassReferencesCreateHandler) *ObjectsClassReferencesCreate {
	return &ObjectsClassReferencesCreate{Context: ctx, Handler: handler}
}

/*
ObjectsClassReferencesCreate swagger:route POST /objects/{className}/{id}/references/{propertyName} objects objectsClassReferencesCreate

Add a single reference to a class-property.

Add a single reference to a class-property.
*/
type ObjectsClassReferencesCreate struct {
	Context *middleware.Context
	Handler ObjectsClassReferencesCreateHandler
}

func (o *ObjectsClassReferencesCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewObjectsClassReferencesCreateParams()

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
