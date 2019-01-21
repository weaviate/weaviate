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

// WeaviateActionUpdateHandlerFunc turns a function with the right signature into a weaviate action update handler
type WeaviateActionUpdateHandlerFunc func(WeaviateActionUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateActionUpdateHandlerFunc) Handle(params WeaviateActionUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaviateActionUpdateHandler interface for that can handle valid weaviate action update params
type WeaviateActionUpdateHandler interface {
	Handle(WeaviateActionUpdateParams) middleware.Responder
}

// NewWeaviateActionUpdate creates a new http.Handler for the weaviate action update operation
func NewWeaviateActionUpdate(ctx *middleware.Context, handler WeaviateActionUpdateHandler) *WeaviateActionUpdate {
	return &WeaviateActionUpdate{Context: ctx, Handler: handler}
}

/*WeaviateActionUpdate swagger:route PUT /actions/{actionId} actions weaviateActionUpdate

Update an Action based on its UUID related to this key.

Updates an Action's data. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.

*/
type WeaviateActionUpdate struct {
	Context *middleware.Context
	Handler WeaviateActionUpdateHandler
}

func (o *WeaviateActionUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateActionUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
