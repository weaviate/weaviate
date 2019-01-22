/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */ // Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateActionsPatchHandlerFunc turns a function with the right signature into a weaviate actions patch handler
type WeaviateActionsPatchHandlerFunc func(WeaviateActionsPatchParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionsPatchHandlerFunc) Handle(params WeaviateActionsPatchParams) middleware.Responder {
	return fn(params)
}

// WeaviateActionsPatchHandler interface for that can handle valid weaviate actions patch params
type WeaviateActionsPatchHandler interface {
	Handle(WeaviateActionsPatchParams) middleware.Responder
}

// NewWeaviateActionsPatch creates a new http.Handler for the weaviate actions patch operation
func NewWeaviateActionsPatch(ctx *middleware.Context, handler WeaviateActionsPatchHandler) *WeaviateActionsPatch {
	return &WeaviateActionsPatch{Context: ctx, Handler: handler}
}

/*WeaviateActionsPatch swagger:route PATCH /actions/{actionId} actions weaviateActionsPatch

Update an Action based on its UUID (using patch semantics) related to this key.

Updates an Action. This method supports patch semantics. Provided meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.

*/
type WeaviateActionsPatch struct {
	Context *middleware.Context
	Handler WeaviateActionsPatchHandler
}

func (o *WeaviateActionsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionsPatchParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
