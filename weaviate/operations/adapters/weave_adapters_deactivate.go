package adapters


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveAdaptersDeactivateHandlerFunc turns a function with the right signature into a weave adapters deactivate handler
type WeaveAdaptersDeactivateHandlerFunc func(WeaveAdaptersDeactivateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAdaptersDeactivateHandlerFunc) Handle(params WeaveAdaptersDeactivateParams) middleware.Responder {
	return fn(params)
}

// WeaveAdaptersDeactivateHandler interface for that can handle valid weave adapters deactivate params
type WeaveAdaptersDeactivateHandler interface {
	Handle(WeaveAdaptersDeactivateParams) middleware.Responder
}

// NewWeaveAdaptersDeactivate creates a new http.Handler for the weave adapters deactivate operation
func NewWeaveAdaptersDeactivate(ctx *middleware.Context, handler WeaveAdaptersDeactivateHandler) *WeaveAdaptersDeactivate {
	return &WeaveAdaptersDeactivate{Context: ctx, Handler: handler}
}

/*WeaveAdaptersDeactivate swagger:route POST /adapters/{adapterId}/deactivate adapters weaveAdaptersDeactivate

Deactivates an adapter. This will also delete all devices provided by that adapter.

*/
type WeaveAdaptersDeactivate struct {
	Context *middleware.Context
	Handler WeaveAdaptersDeactivateHandler
}

func (o *WeaveAdaptersDeactivate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAdaptersDeactivateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
