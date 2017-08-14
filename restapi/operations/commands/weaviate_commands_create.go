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

// WeaviateCommandsCreateHandlerFunc turns a function with the right signature into a weaviate commands create handler
type WeaviateCommandsCreateHandlerFunc func(WeaviateCommandsCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateCommandsCreateHandlerFunc) Handle(params WeaviateCommandsCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateCommandsCreateHandler interface for that can handle valid weaviate commands create params
type WeaviateCommandsCreateHandler interface {
	Handle(WeaviateCommandsCreateParams, interface{}) middleware.Responder
}

// NewWeaviateCommandsCreate creates a new http.Handler for the weaviate commands create operation
func NewWeaviateCommandsCreate(ctx *middleware.Context, handler WeaviateCommandsCreateHandler) *WeaviateCommandsCreate {
	return &WeaviateCommandsCreate{Context: ctx, Handler: handler}
}

/*WeaviateCommandsCreate swagger:route POST /commands commands weaviateCommandsCreate

Create a new command related to this key related to this key.

Creates and sends a new command.

*/
type WeaviateCommandsCreate struct {
	Context *middleware.Context
	Handler WeaviateCommandsCreateHandler
}

func (o *WeaviateCommandsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateCommandsCreateParams()

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
