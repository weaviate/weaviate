//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// WeaviateThingsGetHandlerFunc turns a function with the right signature into a weaviate things get handler
type WeaviateThingsGetHandlerFunc func(WeaviateThingsGetParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsGetHandlerFunc) Handle(params WeaviateThingsGetParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingsGetHandler interface for that can handle valid weaviate things get params
type WeaviateThingsGetHandler interface {
	Handle(WeaviateThingsGetParams, *models.Principal) middleware.Responder
}

// NewWeaviateThingsGet creates a new http.Handler for the weaviate things get operation
func NewWeaviateThingsGet(ctx *middleware.Context, handler WeaviateThingsGetHandler) *WeaviateThingsGet {
	return &WeaviateThingsGet{Context: ctx, Handler: handler}
}

/*WeaviateThingsGet swagger:route GET /things/{id} things weaviateThingsGet

Get a Thing based on its UUID.

Returns a particular Thing data.

*/
type WeaviateThingsGet struct {
	Context *middleware.Context
	Handler WeaviateThingsGetHandler
}

func (o *WeaviateThingsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsGetParams()

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
