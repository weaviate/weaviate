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
 package commands




import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsGetQueueHandlerFunc turns a function with the right signature into a weave commands get queue handler
type WeaveCommandsGetQueueHandlerFunc func(WeaveCommandsGetQueueParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsGetQueueHandlerFunc) Handle(params WeaveCommandsGetQueueParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsGetQueueHandler interface for that can handle valid weave commands get queue params
type WeaveCommandsGetQueueHandler interface {
	Handle(WeaveCommandsGetQueueParams, interface{}) middleware.Responder
}

// NewWeaveCommandsGetQueue creates a new http.Handler for the weave commands get queue operation
func NewWeaveCommandsGetQueue(ctx *middleware.Context, handler WeaveCommandsGetQueueHandler) *WeaveCommandsGetQueue {
	return &WeaveCommandsGetQueue{Context: ctx, Handler: handler}
}

/*WeaveCommandsGetQueue swagger:route GET /commands/queue commands weaveCommandsGetQueue

Returns queued commands that device is supposed to execute. This method may be used only by devices.

*/
type WeaveCommandsGetQueue struct {
	Context *middleware.Context
	Handler WeaveCommandsGetQueueHandler
}

func (o *WeaveCommandsGetQueue) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsGetQueueParams()

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
