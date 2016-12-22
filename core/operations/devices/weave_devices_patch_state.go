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

// WeaveDevicesPatchStateHandlerFunc turns a function with the right signature into a weave devices patch state handler
type WeaveDevicesPatchStateHandlerFunc func(WeaveDevicesPatchStateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesPatchStateHandlerFunc) Handle(params WeaveDevicesPatchStateParams) middleware.Responder {
	return fn(params)
}

// WeaveDevicesPatchStateHandler interface for that can handle valid weave devices patch state params
type WeaveDevicesPatchStateHandler interface {
	Handle(WeaveDevicesPatchStateParams) middleware.Responder
}

// NewWeaveDevicesPatchState creates a new http.Handler for the weave devices patch state operation
func NewWeaveDevicesPatchState(ctx *middleware.Context, handler WeaveDevicesPatchStateHandler) *WeaveDevicesPatchState {
	return &WeaveDevicesPatchState{Context: ctx, Handler: handler}
}

/*WeaveDevicesPatchState swagger:route POST /devices/{deviceId}/patchState devices weaveDevicesPatchState

Applies provided patches to the device state. This method may be used only by devices.

*/
type WeaveDevicesPatchState struct {
	Context *middleware.Context
	Handler WeaveDevicesPatchStateHandler
}

func (o *WeaveDevicesPatchState) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesPatchStateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
