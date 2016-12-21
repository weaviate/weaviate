package commands


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsInsertHandlerFunc turns a function with the right signature into a weave commands insert handler
type WeaveCommandsInsertHandlerFunc func(WeaveCommandsInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsInsertHandlerFunc) Handle(params WeaveCommandsInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveCommandsInsertHandler interface for that can handle valid weave commands insert params
type WeaveCommandsInsertHandler interface {
	Handle(WeaveCommandsInsertParams, interface{}) middleware.Responder
}

// NewWeaveCommandsInsert creates a new http.Handler for the weave commands insert operation
func NewWeaveCommandsInsert(ctx *middleware.Context, handler WeaveCommandsInsertHandler) *WeaveCommandsInsert {
	return &WeaveCommandsInsert{Context: ctx, Handler: handler}
}

/*WeaveCommandsInsert swagger:route POST /commands commands weaveCommandsInsert

Creates and sends a new command.

*/
type WeaveCommandsInsert struct {
	Context *middleware.Context
	Handler WeaveCommandsInsertHandler
}

func (o *WeaveCommandsInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsInsertParams()

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
