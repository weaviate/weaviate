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

package nodes

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// NodesGetHandlerFunc turns a function with the right signature into a nodes get handler
type NodesGetHandlerFunc func(NodesGetParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn NodesGetHandlerFunc) Handle(params NodesGetParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// NodesGetHandler interface for that can handle valid nodes get params
type NodesGetHandler interface {
	Handle(NodesGetParams, *models.Principal) middleware.Responder
}

// NewNodesGet creates a new http.Handler for the nodes get operation
func NewNodesGet(ctx *middleware.Context, handler NodesGetHandler) *NodesGet {
	return &NodesGet{Context: ctx, Handler: handler}
}

/*NodesGet swagger:route GET /nodes nodes nodesGet

Returns status of Weaviate DB.

*/
type NodesGet struct {
	Context *middleware.Context
	Handler NodesGetHandler
}

func (o *NodesGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewNodesGetParams()

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
