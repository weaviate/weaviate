/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
// Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateActionHistoryGetHandlerFunc turns a function with the right signature into a weaviate action history get handler
type WeaviateActionHistoryGetHandlerFunc func(WeaviateActionHistoryGetParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionHistoryGetHandlerFunc) Handle(params WeaviateActionHistoryGetParams) middleware.Responder {
	return fn(params)
}

// WeaviateActionHistoryGetHandler interface for that can handle valid weaviate action history get params
type WeaviateActionHistoryGetHandler interface {
	Handle(WeaviateActionHistoryGetParams) middleware.Responder
}

// NewWeaviateActionHistoryGet creates a new http.Handler for the weaviate action history get operation
func NewWeaviateActionHistoryGet(ctx *middleware.Context, handler WeaviateActionHistoryGetHandler) *WeaviateActionHistoryGet {
	return &WeaviateActionHistoryGet{Context: ctx, Handler: handler}
}

/*WeaviateActionHistoryGet swagger:route GET /actions/{actionId}/history actions weaviateActionHistoryGet

Get an Action's history based on its UUID related to this key.

Returns a particular Action history.

*/
type WeaviateActionHistoryGet struct {
	Context *middleware.Context
	Handler WeaviateActionHistoryGetHandler
}

func (o *WeaviateActionHistoryGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionHistoryGetParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
