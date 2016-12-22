package model_manifests


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveModelManifestsListHandlerFunc turns a function with the right signature into a weave model manifests list handler
type WeaveModelManifestsListHandlerFunc func(WeaveModelManifestsListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveModelManifestsListHandlerFunc) Handle(params WeaveModelManifestsListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveModelManifestsListHandler interface for that can handle valid weave model manifests list params
type WeaveModelManifestsListHandler interface {
	Handle(WeaveModelManifestsListParams, interface{}) middleware.Responder
}

// NewWeaveModelManifestsList creates a new http.Handler for the weave model manifests list operation
func NewWeaveModelManifestsList(ctx *middleware.Context, handler WeaveModelManifestsListHandler) *WeaveModelManifestsList {
	return &WeaveModelManifestsList{Context: ctx, Handler: handler}
}

/*WeaveModelManifestsList swagger:route GET /modelManifests modelManifests weaveModelManifestsList

Lists all model manifests.

*/
type WeaveModelManifestsList struct {
	Context *middleware.Context
	Handler WeaveModelManifestsListHandler
}

func (o *WeaveModelManifestsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveModelManifestsListParams()

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
