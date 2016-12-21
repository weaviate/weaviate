package rooms


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveRoomsGetHandlerFunc turns a function with the right signature into a weave rooms get handler
type WeaveRoomsGetHandlerFunc func(WeaveRoomsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRoomsGetHandlerFunc) Handle(params WeaveRoomsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveRoomsGetHandler interface for that can handle valid weave rooms get params
type WeaveRoomsGetHandler interface {
	Handle(WeaveRoomsGetParams, interface{}) middleware.Responder
}

// NewWeaveRoomsGet creates a new http.Handler for the weave rooms get operation
func NewWeaveRoomsGet(ctx *middleware.Context, handler WeaveRoomsGetHandler) *WeaveRoomsGet {
	return &WeaveRoomsGet{Context: ctx, Handler: handler}
}

/*WeaveRoomsGet swagger:route GET /places/{placeId}/rooms/{roomId} rooms weaveRoomsGet

Get a room.

*/
type WeaveRoomsGet struct {
	Context *middleware.Context
	Handler WeaveRoomsGetHandler
}

func (o *WeaveRoomsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRoomsGetParams()

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
