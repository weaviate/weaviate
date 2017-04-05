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




import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveDevicesPatchHandlerFunc turns a function with the right signature into a weave devices patch handler
type WeaveDevicesPatchHandlerFunc func(WeaveDevicesPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesPatchHandlerFunc) Handle(params WeaveDevicesPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesPatchHandler interface for that can handle valid weave devices patch params
type WeaveDevicesPatchHandler interface {
	Handle(WeaveDevicesPatchParams, interface{}) middleware.Responder
}

// NewWeaveDevicesPatch creates a new http.Handler for the weave devices patch operation
func NewWeaveDevicesPatch(ctx *middleware.Context, handler WeaveDevicesPatchHandler) *WeaveDevicesPatch {
	return &WeaveDevicesPatch{Context: ctx, Handler: handler}
}

/*WeaveDevicesPatch swagger:route PATCH /devices/{deviceId} devices weaveDevicesPatch

Updates a device data. This method supports patch semantics.

*/
type WeaveDevicesPatch struct {
	Context *middleware.Context
	Handler WeaveDevicesPatchHandler
}

func (o *WeaveDevicesPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesPatchParams()

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
