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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package devices


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateDevicesDeleteHandlerFunc turns a function with the right signature into a weaviate devices delete handler
type WeaviateDevicesDeleteHandlerFunc func(WeaviateDevicesDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateDevicesDeleteHandlerFunc) Handle(params WeaviateDevicesDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateDevicesDeleteHandler interface for that can handle valid weaviate devices delete params
type WeaviateDevicesDeleteHandler interface {
	Handle(WeaviateDevicesDeleteParams, interface{}) middleware.Responder
}

// NewWeaviateDevicesDelete creates a new http.Handler for the weaviate devices delete operation
func NewWeaviateDevicesDelete(ctx *middleware.Context, handler WeaviateDevicesDeleteHandler) *WeaviateDevicesDelete {
	return &WeaviateDevicesDelete{Context: ctx, Handler: handler}
}

/*WeaviateDevicesDelete swagger:route DELETE /devices/{deviceId} devices weaviateDevicesDelete

Deletes a device from the system.

*/
type WeaviateDevicesDelete struct {
	Context *middleware.Context
	Handler WeaviateDevicesDeleteHandler
}

func (o *WeaviateDevicesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateDevicesDeleteParams()

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
