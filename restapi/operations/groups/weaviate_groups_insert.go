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
 package groups


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateGroupsInsertHandlerFunc turns a function with the right signature into a weaviate groups insert handler
type WeaviateGroupsInsertHandlerFunc func(WeaviateGroupsInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateGroupsInsertHandlerFunc) Handle(params WeaviateGroupsInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateGroupsInsertHandler interface for that can handle valid weaviate groups insert params
type WeaviateGroupsInsertHandler interface {
	Handle(WeaviateGroupsInsertParams, interface{}) middleware.Responder
}

// NewWeaviateGroupsInsert creates a new http.Handler for the weaviate groups insert operation
func NewWeaviateGroupsInsert(ctx *middleware.Context, handler WeaviateGroupsInsertHandler) *WeaviateGroupsInsert {
	return &WeaviateGroupsInsert{Context: ctx, Handler: handler}
}

/*WeaviateGroupsInsert swagger:route POST /groups groups weaviateGroupsInsert

Inserts group.

*/
type WeaviateGroupsInsert struct {
	Context *middleware.Context
	Handler WeaviateGroupsInsertHandler
}

func (o *WeaviateGroupsInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateGroupsInsertParams()

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
