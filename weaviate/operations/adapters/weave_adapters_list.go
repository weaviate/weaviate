package adapters


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveAdaptersListHandlerFunc turns a function with the right signature into a weave adapters list handler
type WeaveAdaptersListHandlerFunc func(WeaveAdaptersListParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveAdaptersListHandlerFunc) Handle(params WeaveAdaptersListParams) middleware.Responder {
	return fn(params)
}

// WeaveAdaptersListHandler interface for that can handle valid weave adapters list params
type WeaveAdaptersListHandler interface {
	Handle(WeaveAdaptersListParams) middleware.Responder
}

// NewWeaveAdaptersList creates a new http.Handler for the weave adapters list operation
func NewWeaveAdaptersList(ctx *middleware.Context, handler WeaveAdaptersListHandler) *WeaveAdaptersList {
	return &WeaveAdaptersList{Context: ctx, Handler: handler}
}

/*WeaveAdaptersList swagger:route GET /adapters adapters weaveAdaptersList

Lists adapters.

*/
type WeaveAdaptersList struct {
	Context *middleware.Context
	Handler WeaveAdaptersListHandler
}

func (o *WeaveAdaptersList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveAdaptersListParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
