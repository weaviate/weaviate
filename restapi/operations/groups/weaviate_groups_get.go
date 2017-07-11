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

// WeaviateGroupsGetHandlerFunc turns a function with the right signature into a weaviate groups get handler
type WeaviateGroupsGetHandlerFunc func(WeaviateGroupsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateGroupsGetHandlerFunc) Handle(params WeaviateGroupsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateGroupsGetHandler interface for that can handle valid weaviate groups get params
type WeaviateGroupsGetHandler interface {
	Handle(WeaviateGroupsGetParams, interface{}) middleware.Responder
}

// NewWeaviateGroupsGet creates a new http.Handler for the weaviate groups get operation
func NewWeaviateGroupsGet(ctx *middleware.Context, handler WeaviateGroupsGetHandler) *WeaviateGroupsGet {
	return &WeaviateGroupsGet{Context: ctx, Handler: handler}
}

/*WeaviateGroupsGet swagger:route GET /groups/{groupId} groups weaviateGroupsGet

Get a group based on its uuid related to this key.

Get a group.

*/
type WeaviateGroupsGet struct {
	Context *middleware.Context
	Handler WeaviateGroupsGetHandler
}

func (o *WeaviateGroupsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateGroupsGetParams()

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
