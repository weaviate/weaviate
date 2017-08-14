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
   

package commands

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateCommandsGetHandlerFunc turns a function with the right signature into a weaviate commands get handler
type WeaviateCommandsGetHandlerFunc func(WeaviateCommandsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsGetHandlerFunc) Handle(params WeaviateCommandsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateCommandsGetHandler interface for that can handle valid weaviate commands get params
type WeaviateCommandsGetHandler interface {
	Handle(WeaviateCommandsGetParams, interface{}) middleware.Responder
}

// NewWeaviateCommandsGet creates a new http.Handler for the weaviate commands get operation
func NewWeaviateCommandsGet(ctx *middleware.Context, handler WeaviateCommandsGetHandler) *WeaviateCommandsGet {
	return &WeaviateCommandsGet{Context: ctx, Handler: handler}
}

/*WeaviateCommandsGet swagger:route GET /commands/{commandId} commands weaviateCommandsGet

Get a command based on its uuid related to this key.

Returns a particular command.

*/
type WeaviateCommandsGet struct {
	Context *middleware.Context
	Handler WeaviateCommandsGetHandler
}

func (o *WeaviateCommandsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateCommandsGetParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
