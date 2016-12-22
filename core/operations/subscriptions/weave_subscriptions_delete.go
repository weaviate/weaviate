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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package subscriptions


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveSubscriptionsDeleteHandlerFunc turns a function with the right signature into a weave subscriptions delete handler
type WeaveSubscriptionsDeleteHandlerFunc func(WeaveSubscriptionsDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveSubscriptionsDeleteHandlerFunc) Handle(params WeaveSubscriptionsDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveSubscriptionsDeleteHandler interface for that can handle valid weave subscriptions delete params
type WeaveSubscriptionsDeleteHandler interface {
	Handle(WeaveSubscriptionsDeleteParams, interface{}) middleware.Responder
}

// NewWeaveSubscriptionsDelete creates a new http.Handler for the weave subscriptions delete operation
func NewWeaveSubscriptionsDelete(ctx *middleware.Context, handler WeaveSubscriptionsDeleteHandler) *WeaveSubscriptionsDelete {
	return &WeaveSubscriptionsDelete{Context: ctx, Handler: handler}
}

/*WeaveSubscriptionsDelete swagger:route DELETE /subscriptions/{subscriptionId} subscriptions weaveSubscriptionsDelete

Deletes a subscription.

*/
type WeaveSubscriptionsDelete struct {
	Context *middleware.Context
	Handler WeaveSubscriptionsDeleteHandler
}

func (o *WeaveSubscriptionsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveSubscriptionsDeleteParams()

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
