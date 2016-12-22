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
 package rooms


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveRoomsListHandlerFunc turns a function with the right signature into a weave rooms list handler
type WeaveRoomsListHandlerFunc func(WeaveRoomsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRoomsListHandlerFunc) Handle(params WeaveRoomsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveRoomsListHandler interface for that can handle valid weave rooms list params
type WeaveRoomsListHandler interface {
	Handle(WeaveRoomsListParams, interface{}) middleware.Responder
}

// NewWeaveRoomsList creates a new http.Handler for the weave rooms list operation
func NewWeaveRoomsList(ctx *middleware.Context, handler WeaveRoomsListHandler) *WeaveRoomsList {
	return &WeaveRoomsList{Context: ctx, Handler: handler}
}

/*WeaveRoomsList swagger:route GET /places/{placeId}/rooms rooms weaveRoomsList

Lists rooms for a place.

*/
type WeaveRoomsList struct {
	Context *middleware.Context
	Handler WeaveRoomsListHandler
}

func (o *WeaveRoomsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRoomsListParams()

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
