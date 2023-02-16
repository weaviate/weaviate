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

package meta

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// MetaGetHandlerFunc turns a function with the right signature into a meta get handler
type MetaGetHandlerFunc func(MetaGetParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn MetaGetHandlerFunc) Handle(params MetaGetParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// MetaGetHandler interface for that can handle valid meta get params
type MetaGetHandler interface {
	Handle(MetaGetParams, *models.Principal) middleware.Responder
}

// NewMetaGet creates a new http.Handler for the meta get operation
func NewMetaGet(ctx *middleware.Context, handler MetaGetHandler) *MetaGet {
	return &MetaGet{Context: ctx, Handler: handler}
}

/*MetaGet swagger:route GET /meta meta metaGet

Returns meta information of the current Weaviate instance.

Gives meta information about the server and can be used to provide information to another Weaviate instance that wants to interact with the current instance.

*/
type MetaGet struct {
	Context *middleware.Context
	Handler MetaGetHandler
}

func (o *MetaGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewMetaGetParams()

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
