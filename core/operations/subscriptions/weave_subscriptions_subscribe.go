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

// WeaveSubscriptionsSubscribeHandlerFunc turns a function with the right signature into a weave subscriptions subscribe handler
type WeaveSubscriptionsSubscribeHandlerFunc func(WeaveSubscriptionsSubscribeParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveSubscriptionsSubscribeHandlerFunc) Handle(params WeaveSubscriptionsSubscribeParams) middleware.Responder {
	return fn(params)
}

// WeaveSubscriptionsSubscribeHandler interface for that can handle valid weave subscriptions subscribe params
type WeaveSubscriptionsSubscribeHandler interface {
	Handle(WeaveSubscriptionsSubscribeParams) middleware.Responder
}

// NewWeaveSubscriptionsSubscribe creates a new http.Handler for the weave subscriptions subscribe operation
func NewWeaveSubscriptionsSubscribe(ctx *middleware.Context, handler WeaveSubscriptionsSubscribeHandler) *WeaveSubscriptionsSubscribe {
	return &WeaveSubscriptionsSubscribe{Context: ctx, Handler: handler}
}

/*WeaveSubscriptionsSubscribe swagger:route POST /subscriptions/subscribe subscriptions weaveSubscriptionsSubscribe

Subscribes the authenticated user and application to receiving notifications.

*/
type WeaveSubscriptionsSubscribe struct {
	Context *middleware.Context
	Handler WeaveSubscriptionsSubscribeHandler
}

func (o *WeaveSubscriptionsSubscribe) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveSubscriptionsSubscribeParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
