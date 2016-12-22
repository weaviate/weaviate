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

// WeaveDevicesUpdateHandlerFunc turns a function with the right signature into a weave devices update handler
type WeaveDevicesUpdateHandlerFunc func(WeaveDevicesUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesUpdateHandlerFunc) Handle(params WeaveDevicesUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaveDevicesUpdateHandler interface for that can handle valid weave devices update params
type WeaveDevicesUpdateHandler interface {
	Handle(WeaveDevicesUpdateParams) middleware.Responder
}

// NewWeaveDevicesUpdate creates a new http.Handler for the weave devices update operation
func NewWeaveDevicesUpdate(ctx *middleware.Context, handler WeaveDevicesUpdateHandler) *WeaveDevicesUpdate {
	return &WeaveDevicesUpdate{Context: ctx, Handler: handler}
}

/*WeaveDevicesUpdate swagger:route PUT /devices/{deviceId} devices weaveDevicesUpdate

Updates a device data.

*/
type WeaveDevicesUpdate struct {
	Context *middleware.Context
	Handler WeaveDevicesUpdateHandler
}

func (o *WeaveDevicesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
