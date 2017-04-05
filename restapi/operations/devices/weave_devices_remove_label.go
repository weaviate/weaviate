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

// WeaveDevicesRemoveLabelHandlerFunc turns a function with the right signature into a weave devices remove label handler
type WeaveDevicesRemoveLabelHandlerFunc func(WeaveDevicesRemoveLabelParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesRemoveLabelHandlerFunc) Handle(params WeaveDevicesRemoveLabelParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesRemoveLabelHandler interface for that can handle valid weave devices remove label params
type WeaveDevicesRemoveLabelHandler interface {
	Handle(WeaveDevicesRemoveLabelParams, interface{}) middleware.Responder
}

// NewWeaveDevicesRemoveLabel creates a new http.Handler for the weave devices remove label operation
func NewWeaveDevicesRemoveLabel(ctx *middleware.Context, handler WeaveDevicesRemoveLabelHandler) *WeaveDevicesRemoveLabel {
	return &WeaveDevicesRemoveLabel{Context: ctx, Handler: handler}
}

/*WeaveDevicesRemoveLabel swagger:route POST /devices/{deviceId}/removeLabel devices weaveDevicesRemoveLabel

Removes a label of the device.

*/
type WeaveDevicesRemoveLabel struct {
	Context *middleware.Context
	Handler WeaveDevicesRemoveLabelHandler
}

func (o *WeaveDevicesRemoveLabel) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesRemoveLabelParams()

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
