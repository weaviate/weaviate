package devices


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveDevicesAddtoplaceHandlerFunc turns a function with the right signature into a weave devices addtoplace handler
type WeaveDevicesAddtoplaceHandlerFunc func(WeaveDevicesAddtoplaceParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesAddtoplaceHandlerFunc) Handle(params WeaveDevicesAddtoplaceParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesAddtoplaceHandler interface for that can handle valid weave devices addtoplace params
type WeaveDevicesAddtoplaceHandler interface {
	Handle(WeaveDevicesAddtoplaceParams, interface{}) middleware.Responder
}

// NewWeaveDevicesAddtoplace creates a new http.Handler for the weave devices addtoplace operation
func NewWeaveDevicesAddtoplace(ctx *middleware.Context, handler WeaveDevicesAddtoplaceHandler) *WeaveDevicesAddtoplace {
	return &WeaveDevicesAddtoplace{Context: ctx, Handler: handler}
}

/*WeaveDevicesAddtoplace swagger:route POST /devices/{deviceId}/addToPlace devices weaveDevicesAddtoplace

Adds a device to a place.

*/
type WeaveDevicesAddtoplace struct {
	Context *middleware.Context
	Handler WeaveDevicesAddtoplaceHandler
}

func (o *WeaveDevicesAddtoplace) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesAddtoplaceParams()

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
