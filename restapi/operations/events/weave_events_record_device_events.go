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
 package events




import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveEventsRecordDeviceEventsHandlerFunc turns a function with the right signature into a weave events record device events handler
type WeaveEventsRecordDeviceEventsHandlerFunc func(WeaveEventsRecordDeviceEventsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveEventsRecordDeviceEventsHandlerFunc) Handle(params WeaveEventsRecordDeviceEventsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveEventsRecordDeviceEventsHandler interface for that can handle valid weave events record device events params
type WeaveEventsRecordDeviceEventsHandler interface {
	Handle(WeaveEventsRecordDeviceEventsParams, interface{}) middleware.Responder
}

// NewWeaveEventsRecordDeviceEvents creates a new http.Handler for the weave events record device events operation
func NewWeaveEventsRecordDeviceEvents(ctx *middleware.Context, handler WeaveEventsRecordDeviceEventsHandler) *WeaveEventsRecordDeviceEvents {
	return &WeaveEventsRecordDeviceEvents{Context: ctx, Handler: handler}
}

/*WeaveEventsRecordDeviceEvents swagger:route POST /events/recordDeviceEvents events weaveEventsRecordDeviceEvents

Enables or disables recording of a particular device's events based on a boolean parameter. Enabled by default.

*/
type WeaveEventsRecordDeviceEvents struct {
	Context *middleware.Context
	Handler WeaveEventsRecordDeviceEventsHandler
}

func (o *WeaveEventsRecordDeviceEvents) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveEventsRecordDeviceEventsParams()

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
