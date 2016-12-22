package registration_tickets


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveRegistrationTicketsInsertHandlerFunc turns a function with the right signature into a weave registration tickets insert handler
type WeaveRegistrationTicketsInsertHandlerFunc func(WeaveRegistrationTicketsInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRegistrationTicketsInsertHandlerFunc) Handle(params WeaveRegistrationTicketsInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveRegistrationTicketsInsertHandler interface for that can handle valid weave registration tickets insert params
type WeaveRegistrationTicketsInsertHandler interface {
	Handle(WeaveRegistrationTicketsInsertParams, interface{}) middleware.Responder
}

// NewWeaveRegistrationTicketsInsert creates a new http.Handler for the weave registration tickets insert operation
func NewWeaveRegistrationTicketsInsert(ctx *middleware.Context, handler WeaveRegistrationTicketsInsertHandler) *WeaveRegistrationTicketsInsert {
	return &WeaveRegistrationTicketsInsert{Context: ctx, Handler: handler}
}

/*WeaveRegistrationTicketsInsert swagger:route POST /registrationTickets registrationTickets weaveRegistrationTicketsInsert

Creates a new registration ticket.

*/
type WeaveRegistrationTicketsInsert struct {
	Context *middleware.Context
	Handler WeaveRegistrationTicketsInsertHandler
}

func (o *WeaveRegistrationTicketsInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRegistrationTicketsInsertParams()

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
