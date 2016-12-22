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
 package places


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePlacesDeleteHandlerFunc turns a function with the right signature into a weave places delete handler
type WeavePlacesDeleteHandlerFunc func(WeavePlacesDeleteParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePlacesDeleteHandlerFunc) Handle(params WeavePlacesDeleteParams) middleware.Responder {
	return fn(params)
}

// WeavePlacesDeleteHandler interface for that can handle valid weave places delete params
type WeavePlacesDeleteHandler interface {
	Handle(WeavePlacesDeleteParams) middleware.Responder
}

// NewWeavePlacesDelete creates a new http.Handler for the weave places delete operation
func NewWeavePlacesDelete(ctx *middleware.Context, handler WeavePlacesDeleteHandler) *WeavePlacesDelete {
	return &WeavePlacesDelete{Context: ctx, Handler: handler}
}

/*WeavePlacesDelete swagger:route DELETE /places/{placeId} places weavePlacesDelete

Deletes a place.

*/
type WeavePlacesDelete struct {
	Context *middleware.Context
	Handler WeavePlacesDeleteHandler
}

func (o *WeavePlacesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePlacesDeleteParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
