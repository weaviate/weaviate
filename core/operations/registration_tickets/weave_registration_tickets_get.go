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
 package registration_tickets


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveRegistrationTicketsGetHandlerFunc turns a function with the right signature into a weave registration tickets get handler
type WeaveRegistrationTicketsGetHandlerFunc func(WeaveRegistrationTicketsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRegistrationTicketsGetHandlerFunc) Handle(params WeaveRegistrationTicketsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveRegistrationTicketsGetHandler interface for that can handle valid weave registration tickets get params
type WeaveRegistrationTicketsGetHandler interface {
	Handle(WeaveRegistrationTicketsGetParams, interface{}) middleware.Responder
}

// NewWeaveRegistrationTicketsGet creates a new http.Handler for the weave registration tickets get operation
func NewWeaveRegistrationTicketsGet(ctx *middleware.Context, handler WeaveRegistrationTicketsGetHandler) *WeaveRegistrationTicketsGet {
	return &WeaveRegistrationTicketsGet{Context: ctx, Handler: handler}
}

/*WeaveRegistrationTicketsGet swagger:route GET /registrationTickets/{registrationTicketId} registrationTickets weaveRegistrationTicketsGet

Returns an existing registration ticket.

*/
type WeaveRegistrationTicketsGet struct {
	Context *middleware.Context
	Handler WeaveRegistrationTicketsGetHandler
}

func (o *WeaveRegistrationTicketsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRegistrationTicketsGetParams()

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
