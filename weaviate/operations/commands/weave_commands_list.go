package commands


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsListHandlerFunc turns a function with the right signature into a weave commands list handler
type WeaveCommandsListHandlerFunc func(WeaveCommandsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsListHandlerFunc) Handle(params WeaveCommandsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsListHandler interface for that can handle valid weave commands list params
type WeaveCommandsListHandler interface {
	Handle(WeaveCommandsListParams, interface{}) middleware.Responder
}

// NewWeaveCommandsList creates a new http.Handler for the weave commands list operation
func NewWeaveCommandsList(ctx *middleware.Context, handler WeaveCommandsListHandler) *WeaveCommandsList {
	return &WeaveCommandsList{Context: ctx, Handler: handler}
}

/*WeaveCommandsList swagger:route GET /commands commands weaveCommandsList

Lists all commands in reverse order of creation.

*/
type WeaveCommandsList struct {
	Context *middleware.Context
	Handler WeaveCommandsListHandler
}

func (o *WeaveCommandsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsListParams()

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
