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

// WeaveSubscriptionsGetNotificationsHandlerFunc turns a function with the right signature into a weave subscriptions get notifications handler
type WeaveSubscriptionsGetNotificationsHandlerFunc func(WeaveSubscriptionsGetNotificationsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveSubscriptionsGetNotificationsHandlerFunc) Handle(params WeaveSubscriptionsGetNotificationsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveSubscriptionsGetNotificationsHandler interface for that can handle valid weave subscriptions get notifications params
type WeaveSubscriptionsGetNotificationsHandler interface {
	Handle(WeaveSubscriptionsGetNotificationsParams, interface{}) middleware.Responder
}

// NewWeaveSubscriptionsGetNotifications creates a new http.Handler for the weave subscriptions get notifications operation
func NewWeaveSubscriptionsGetNotifications(ctx *middleware.Context, handler WeaveSubscriptionsGetNotificationsHandler) *WeaveSubscriptionsGetNotifications {
	return &WeaveSubscriptionsGetNotifications{Context: ctx, Handler: handler}
}

/*WeaveSubscriptionsGetNotifications swagger:route GET /subscriptions/{subscriptionId}/notifications subscriptions weaveSubscriptionsGetNotifications

Returns past notifications for the subscription.

*/
type WeaveSubscriptionsGetNotifications struct {
	Context *middleware.Context
	Handler WeaveSubscriptionsGetNotificationsHandler
}

func (o *WeaveSubscriptionsGetNotifications) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveSubscriptionsGetNotificationsParams()

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
