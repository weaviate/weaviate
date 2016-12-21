package devices


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveDevicesUpsertLocalAuthInfoHandlerFunc turns a function with the right signature into a weave devices upsert local auth info handler
type WeaveDevicesUpsertLocalAuthInfoHandlerFunc func(WeaveDevicesUpsertLocalAuthInfoParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveDevicesUpsertLocalAuthInfoHandlerFunc) Handle(params WeaveDevicesUpsertLocalAuthInfoParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveDevicesUpsertLocalAuthInfoHandler interface for that can handle valid weave devices upsert local auth info params
type WeaveDevicesUpsertLocalAuthInfoHandler interface {
	Handle(WeaveDevicesUpsertLocalAuthInfoParams, interface{}) middleware.Responder
}

// NewWeaveDevicesUpsertLocalAuthInfo creates a new http.Handler for the weave devices upsert local auth info operation
func NewWeaveDevicesUpsertLocalAuthInfo(ctx *middleware.Context, handler WeaveDevicesUpsertLocalAuthInfoHandler) *WeaveDevicesUpsertLocalAuthInfo {
	return &WeaveDevicesUpsertLocalAuthInfo{Context: ctx, Handler: handler}
}

/*WeaveDevicesUpsertLocalAuthInfo swagger:route POST /devices/{deviceId}/upsertLocalAuthInfo devices weaveDevicesUpsertLocalAuthInfo

Upserts a device's local auth info.

*/
type WeaveDevicesUpsertLocalAuthInfo struct {
	Context *middleware.Context
	Handler WeaveDevicesUpsertLocalAuthInfoHandler
}

func (o *WeaveDevicesUpsertLocalAuthInfo) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveDevicesUpsertLocalAuthInfoParams()

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
