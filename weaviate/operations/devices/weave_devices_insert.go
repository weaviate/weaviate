package devices


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveDevicesInsertHandlerFunc turns a function with the right signature into a weave devices insert handler
type WeaveDevicesInsertHandlerFunc func(WeaveDevicesInsertParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesInsertHandlerFunc) Handle(params WeaveDevicesInsertParams) middleware.Responder {
	return fn(params)
}

// WeaveDevicesInsertHandler interface for that can handle valid weave devices insert params
type WeaveDevicesInsertHandler interface {
	Handle(WeaveDevicesInsertParams) middleware.Responder
}

// NewWeaveDevicesInsert creates a new http.Handler for the weave devices insert operation
func NewWeaveDevicesInsert(ctx *middleware.Context, handler WeaveDevicesInsertHandler) *WeaveDevicesInsert {
	return &WeaveDevicesInsert{Context: ctx, Handler: handler}
}

/*WeaveDevicesInsert swagger:route POST /devices devices weaveDevicesInsert

Registers a new device. This method may be used only by aggregator devices or adapters.

*/
type WeaveDevicesInsert struct {
	Context *middleware.Context
	Handler WeaveDevicesInsertHandler
}

func (o *WeaveDevicesInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesInsertParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
