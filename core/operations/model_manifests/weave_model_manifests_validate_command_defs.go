package model_manifests


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveModelManifestsValidateCommandDefsHandlerFunc turns a function with the right signature into a weave model manifests validate command defs handler
type WeaveModelManifestsValidateCommandDefsHandlerFunc func(WeaveModelManifestsValidateCommandDefsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveModelManifestsValidateCommandDefsHandlerFunc) Handle(params WeaveModelManifestsValidateCommandDefsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveModelManifestsValidateCommandDefsHandler interface for that can handle valid weave model manifests validate command defs params
type WeaveModelManifestsValidateCommandDefsHandler interface {
	Handle(WeaveModelManifestsValidateCommandDefsParams, interface{}) middleware.Responder
}

// NewWeaveModelManifestsValidateCommandDefs creates a new http.Handler for the weave model manifests validate command defs operation
func NewWeaveModelManifestsValidateCommandDefs(ctx *middleware.Context, handler WeaveModelManifestsValidateCommandDefsHandler) *WeaveModelManifestsValidateCommandDefs {
	return &WeaveModelManifestsValidateCommandDefs{Context: ctx, Handler: handler}
}

/*WeaveModelManifestsValidateCommandDefs swagger:route POST /modelManifests/validateCommandDefs modelManifests weaveModelManifestsValidateCommandDefs

Validates given command definitions and returns errors.

*/
type WeaveModelManifestsValidateCommandDefs struct {
	Context *middleware.Context
	Handler WeaveModelManifestsValidateCommandDefsHandler
}

func (o *WeaveModelManifestsValidateCommandDefs) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveModelManifestsValidateCommandDefsParams()

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
