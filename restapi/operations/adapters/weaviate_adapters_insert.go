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

// WeaviateAdaptersInsertHandlerFunc turns a function with the right signature into a weaviate adapters insert handler
type WeaviateAdaptersInsertHandlerFunc func(WeaviateAdaptersInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateAdaptersInsertHandlerFunc) Handle(params WeaviateAdaptersInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateAdaptersInsertHandler interface for that can handle valid weaviate adapters insert params
type WeaviateAdaptersInsertHandler interface {
	Handle(WeaviateAdaptersInsertParams, interface{}) middleware.Responder
}

// NewWeaviateAdaptersInsert creates a new http.Handler for the weaviate adapters insert operation
func NewWeaviateAdaptersInsert(ctx *middleware.Context, handler WeaviateAdaptersInsertHandler) *WeaviateAdaptersInsert {
	return &WeaviateAdaptersInsert{Context: ctx, Handler: handler}
}

/*WeaviateAdaptersInsert swagger:route POST /adapters adapters weaviateAdaptersInsert

Inserts adapter.

*/
type WeaviateAdaptersInsert struct {
	Context *middleware.Context
	Handler WeaviateAdaptersInsertHandler
}

func (o *WeaviateAdaptersInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateAdaptersInsertParams()

	uprinc, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	var principal interface{}
	if uprinc != nil {
		principal = uprinc
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
