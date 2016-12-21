package registration_tickets


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveRegistrationTicketsFinalizeHandlerFunc turns a function with the right signature into a weave registration tickets finalize handler
type WeaveRegistrationTicketsFinalizeHandlerFunc func(WeaveRegistrationTicketsFinalizeParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveRegistrationTicketsFinalizeHandlerFunc) Handle(params WeaveRegistrationTicketsFinalizeParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveRegistrationTicketsFinalizeHandler interface for that can handle valid weave registration tickets finalize params
type WeaveRegistrationTicketsFinalizeHandler interface {
	Handle(WeaveRegistrationTicketsFinalizeParams, interface{}) middleware.Responder
}

// NewWeaveRegistrationTicketsFinalize creates a new http.Handler for the weave registration tickets finalize operation
func NewWeaveRegistrationTicketsFinalize(ctx *middleware.Context, handler WeaveRegistrationTicketsFinalizeHandler) *WeaveRegistrationTicketsFinalize {
	return &WeaveRegistrationTicketsFinalize{Context: ctx, Handler: handler}
}

/*WeaveRegistrationTicketsFinalize swagger:route POST /registrationTickets/{registrationTicketId}/finalize registrationTickets weaveRegistrationTicketsFinalize

Finalizes device registration and returns its credentials. This method may be used only by devices.

*/
type WeaveRegistrationTicketsFinalize struct {
	Context *middleware.Context
	Handler WeaveRegistrationTicketsFinalizeHandler
}

func (o *WeaveRegistrationTicketsFinalize) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveRegistrationTicketsFinalizeParams()

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
