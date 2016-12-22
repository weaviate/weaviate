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
 package devices


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveDevicesGetHandlerFunc turns a function with the right signature into a weave devices get handler
type WeaveDevicesGetHandlerFunc func(WeaveDevicesGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesGetHandlerFunc) Handle(params WeaveDevicesGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesGetHandler interface for that can handle valid weave devices get params
type WeaveDevicesGetHandler interface {
	Handle(WeaveDevicesGetParams, interface{}) middleware.Responder
}

// NewWeaveDevicesGet creates a new http.Handler for the weave devices get operation
func NewWeaveDevicesGet(ctx *middleware.Context, handler WeaveDevicesGetHandler) *WeaveDevicesGet {
	return &WeaveDevicesGet{Context: ctx, Handler: handler}
}

/*WeaveDevicesGet swagger:route GET /devices/{deviceId} devices weaveDevicesGet

Returns a particular device data.

*/
type WeaveDevicesGet struct {
	Context *middleware.Context
	Handler WeaveDevicesGetHandler
}

func (o *WeaveDevicesGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesGetParams()

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
