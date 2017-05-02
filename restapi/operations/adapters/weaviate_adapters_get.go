/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package adapters


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateAdaptersGetHandlerFunc turns a function with the right signature into a weaviate adapters get handler
type WeaviateAdaptersGetHandlerFunc func(WeaviateAdaptersGetParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateAdaptersGetHandlerFunc) Handle(params WeaviateAdaptersGetParams) middleware.Responder {
	return fn(params)
}

// WeaviateAdaptersGetHandler interface for that can handle valid weaviate adapters get params
type WeaviateAdaptersGetHandler interface {
	Handle(WeaviateAdaptersGetParams) middleware.Responder
}

// NewWeaviateAdaptersGet creates a new http.Handler for the weaviate adapters get operation
func NewWeaviateAdaptersGet(ctx *middleware.Context, handler WeaviateAdaptersGetHandler) *WeaviateAdaptersGet {
	return &WeaviateAdaptersGet{Context: ctx, Handler: handler}
}

/*WeaviateAdaptersGet swagger:route GET /adapters/{adapterId} adapters weaviateAdaptersGet

Get an adapter.

*/
type WeaviateAdaptersGet struct {
	Context *middleware.Context
	Handler WeaviateAdaptersGetHandler
}

func (o *WeaviateAdaptersGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateAdaptersGetParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
