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
   

package graphql

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavaiteGraphqlGetHandlerFunc turns a function with the right signature into a weavaite graphql get handler
type WeavaiteGraphqlGetHandlerFunc func(WeavaiteGraphqlGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavaiteGraphqlGetHandlerFunc) Handle(params WeavaiteGraphqlGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavaiteGraphqlGetHandler interface for that can handle valid weavaite graphql get params
type WeavaiteGraphqlGetHandler interface {
	Handle(WeavaiteGraphqlGetParams, interface{}) middleware.Responder
}

// NewWeavaiteGraphqlGet creates a new http.Handler for the weavaite graphql get operation
func NewWeavaiteGraphqlGet(ctx *middleware.Context, handler WeavaiteGraphqlGetHandler) *WeavaiteGraphqlGet {
	return &WeavaiteGraphqlGet{Context: ctx, Handler: handler}
}

/*WeavaiteGraphqlGet swagger:route GET /graphql graphql weavaiteGraphqlGet

Get a response based on GraphQL

Get results based on GraphQL

*/
type WeavaiteGraphqlGet struct {
	Context *middleware.Context
	Handler WeavaiteGraphqlGetHandler
}

func (o *WeavaiteGraphqlGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeavaiteGraphqlGetParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
