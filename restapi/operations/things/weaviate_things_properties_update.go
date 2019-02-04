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

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	context "golang.org/x/net/context"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingsPropertiesUpdateHandlerFunc turns a function with the right signature into a weaviate things properties update handler
type WeaviateThingsPropertiesUpdateHandlerFunc func(context.Context, WeaviateThingsPropertiesUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingsPropertiesUpdateHandlerFunc) Handle(ctx context.Context, params WeaviateThingsPropertiesUpdateParams) middleware.Responder {
	return fn(ctx, params)
}

// WeaviateThingsPropertiesUpdateHandler interface for that can handle valid weaviate things properties update params
type WeaviateThingsPropertiesUpdateHandler interface {
	Handle(context.Context, WeaviateThingsPropertiesUpdateParams) middleware.Responder
}

// NewWeaviateThingsPropertiesUpdate creates a new http.Handler for the weaviate things properties update operation
func NewWeaviateThingsPropertiesUpdate(ctx *middleware.Context, handler WeaviateThingsPropertiesUpdateHandler) *WeaviateThingsPropertiesUpdate {
	return &WeaviateThingsPropertiesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateThingsPropertiesUpdate swagger:route PUT /things/{thingId}/properties/{propertyName} things weaviateThingsPropertiesUpdate

Replace all references to a class-property.

Replace all references to a class-property.

*/
type WeaviateThingsPropertiesUpdate struct {
	Context *middleware.Context
	Handler WeaviateThingsPropertiesUpdateHandler
}

func (o *WeaviateThingsPropertiesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingsPropertiesUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(r.Context(), Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
