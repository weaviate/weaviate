package commands


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveCommandsUpdateHandlerFunc turns a function with the right signature into a weave commands update handler
type WeaveCommandsUpdateHandlerFunc func(WeaveCommandsUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveCommandsUpdateHandlerFunc) Handle(params WeaveCommandsUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaveCommandsUpdateHandler interface for that can handle valid weave commands update params
type WeaveCommandsUpdateHandler interface {
	Handle(WeaveCommandsUpdateParams) middleware.Responder
}

// NewWeaveCommandsUpdate creates a new http.Handler for the weave commands update operation
func NewWeaveCommandsUpdate(ctx *middleware.Context, handler WeaveCommandsUpdateHandler) *WeaveCommandsUpdate {
	return &WeaveCommandsUpdate{Context: ctx, Handler: handler}
}

/*WeaveCommandsUpdate swagger:route PUT /commands/{commandId} commands weaveCommandsUpdate

Updates a command. This method may be used only by devices.

*/
type WeaveCommandsUpdate struct {
	Context *middleware.Context
	Handler WeaveCommandsUpdateHandler
}

func (o *WeaveCommandsUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveCommandsUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
