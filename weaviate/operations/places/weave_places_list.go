package places


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePlacesListHandlerFunc turns a function with the right signature into a weave places list handler
type WeavePlacesListHandlerFunc func(WeavePlacesListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePlacesListHandlerFunc) Handle(params WeavePlacesListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavePlacesListHandler interface for that can handle valid weave places list params
type WeavePlacesListHandler interface {
	Handle(WeavePlacesListParams, interface{}) middleware.Responder
}

// NewWeavePlacesList creates a new http.Handler for the weave places list operation
func NewWeavePlacesList(ctx *middleware.Context, handler WeavePlacesListHandler) *WeavePlacesList {
	return &WeavePlacesList{Context: ctx, Handler: handler}
}

/*WeavePlacesList swagger:route GET /places places weavePlacesList

Lists user's places (homes).

*/
type WeavePlacesList struct {
	Context *middleware.Context
	Handler WeavePlacesListHandler
}

func (o *WeavePlacesList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePlacesListParams()

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
