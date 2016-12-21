package places


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePlacesGetHandlerFunc turns a function with the right signature into a weave places get handler
type WeavePlacesGetHandlerFunc func(WeavePlacesGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePlacesGetHandlerFunc) Handle(params WeavePlacesGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavePlacesGetHandler interface for that can handle valid weave places get params
type WeavePlacesGetHandler interface {
	Handle(WeavePlacesGetParams, interface{}) middleware.Responder
}

// NewWeavePlacesGet creates a new http.Handler for the weave places get operation
func NewWeavePlacesGet(ctx *middleware.Context, handler WeavePlacesGetHandler) *WeavePlacesGet {
	return &WeavePlacesGet{Context: ctx, Handler: handler}
}

/*WeavePlacesGet swagger:route GET /places/{placeId} places weavePlacesGet

Returns a particular place data.

*/
type WeavePlacesGet struct {
	Context *middleware.Context
	Handler WeavePlacesGetHandler
}

func (o *WeavePlacesGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePlacesGetParams()

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
