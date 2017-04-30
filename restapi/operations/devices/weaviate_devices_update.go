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

// WeaviateDevicesUpdateHandlerFunc turns a function with the right signature into a weaviate devices update handler
type WeaviateDevicesUpdateHandlerFunc func(WeaviateDevicesUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateDevicesUpdateHandlerFunc) Handle(params WeaviateDevicesUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaviateDevicesUpdateHandler interface for that can handle valid weaviate devices update params
type WeaviateDevicesUpdateHandler interface {
	Handle(WeaviateDevicesUpdateParams) middleware.Responder
}

// NewWeaviateDevicesUpdate creates a new http.Handler for the weaviate devices update operation
func NewWeaviateDevicesUpdate(ctx *middleware.Context, handler WeaviateDevicesUpdateHandler) *WeaviateDevicesUpdate {
	return &WeaviateDevicesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateDevicesUpdate swagger:route PUT /devices/{deviceId} devices weaviateDevicesUpdate

Updates a device data.

*/
type WeaviateDevicesUpdate struct {
	Context *middleware.Context
	Handler WeaviateDevicesUpdateHandler
}

func (o *WeaviateDevicesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateDevicesUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
