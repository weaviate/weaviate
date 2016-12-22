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

// WeaveRoomsModifyHandlerFunc turns a function with the right signature into a weave rooms modify handler
type WeaveRoomsModifyHandlerFunc func(WeaveRoomsModifyParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRoomsModifyHandlerFunc) Handle(params WeaveRoomsModifyParams) middleware.Responder {
	return fn(params)
}

// WeaveRoomsModifyHandler interface for that can handle valid weave rooms modify params
type WeaveRoomsModifyHandler interface {
	Handle(WeaveRoomsModifyParams) middleware.Responder
}

// NewWeaveRoomsModify creates a new http.Handler for the weave rooms modify operation
func NewWeaveRoomsModify(ctx *middleware.Context, handler WeaveRoomsModifyHandler) *WeaveRoomsModify {
	return &WeaveRoomsModify{Context: ctx, Handler: handler}
}

/*WeaveRoomsModify swagger:route POST /places/{placeId}/rooms/{roomId}/modify rooms weaveRoomsModify

Updates a room.

*/
type WeaveRoomsModify struct {
	Context *middleware.Context
	Handler WeaveRoomsModifyHandler
}

func (o *WeaveRoomsModify) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRoomsModifyParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
