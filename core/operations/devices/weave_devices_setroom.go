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

// WeaveDevicesSetroomHandlerFunc turns a function with the right signature into a weave devices setroom handler
type WeaveDevicesSetroomHandlerFunc func(WeaveDevicesSetroomParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesSetroomHandlerFunc) Handle(params WeaveDevicesSetroomParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesSetroomHandler interface for that can handle valid weave devices setroom params
type WeaveDevicesSetroomHandler interface {
	Handle(WeaveDevicesSetroomParams, interface{}) middleware.Responder
}

// NewWeaveDevicesSetroom creates a new http.Handler for the weave devices setroom operation
func NewWeaveDevicesSetroom(ctx *middleware.Context, handler WeaveDevicesSetroomHandler) *WeaveDevicesSetroom {
	return &WeaveDevicesSetroom{Context: ctx, Handler: handler}
}

/*WeaveDevicesSetroom swagger:route POST /devices/{deviceId}/setRoom devices weaveDevicesSetroom

Sets the room of a device.

*/
type WeaveDevicesSetroom struct {
	Context *middleware.Context
	Handler WeaveDevicesSetroomHandler
}

func (o *WeaveDevicesSetroom) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesSetroomParams()

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
