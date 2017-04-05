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

// WeaveEventsDeleteAllHandlerFunc turns a function with the right signature into a weave events delete all handler
type WeaveEventsDeleteAllHandlerFunc func(WeaveEventsDeleteAllParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveEventsDeleteAllHandlerFunc) Handle(params WeaveEventsDeleteAllParams) middleware.Responder {
	return fn(params)
}

// WeaveEventsDeleteAllHandler interface for that can handle valid weave events delete all params
type WeaveEventsDeleteAllHandler interface {
	Handle(WeaveEventsDeleteAllParams) middleware.Responder
}

// NewWeaveEventsDeleteAll creates a new http.Handler for the weave events delete all operation
func NewWeaveEventsDeleteAll(ctx *middleware.Context, handler WeaveEventsDeleteAllHandler) *WeaveEventsDeleteAll {
	return &WeaveEventsDeleteAll{Context: ctx, Handler: handler}
}

/*WeaveEventsDeleteAll swagger:route POST /events/deleteAll events weaveEventsDeleteAll

Deletes all events associated with a particular device. Leaves an event to indicate deletion happened.

*/
type WeaveEventsDeleteAll struct {
	Context *middleware.Context
	Handler WeaveEventsDeleteAllHandler
}

func (o *WeaveEventsDeleteAll) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveEventsDeleteAllParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
