// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/semi-technologies/weaviate/entities/models"
)

// WeaviateWellknownReadinessHandlerFunc turns a function with the right signature into a weaviate wellknown readiness handler
type WeaviateWellknownReadinessHandlerFunc func(WeaviateWellknownReadinessParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateWellknownReadinessHandlerFunc) Handle(params WeaviateWellknownReadinessParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// WeaviateWellknownReadinessHandler interface for that can handle valid weaviate wellknown readiness params
type WeaviateWellknownReadinessHandler interface {
	Handle(WeaviateWellknownReadinessParams, *models.Principal) middleware.Responder
}

// NewWeaviateWellknownReadiness creates a new http.Handler for the weaviate wellknown readiness operation
func NewWeaviateWellknownReadiness(ctx *middleware.Context, handler WeaviateWellknownReadinessHandler) *WeaviateWellknownReadiness {
	return &WeaviateWellknownReadiness{Context: ctx, Handler: handler}
}

/*WeaviateWellknownReadiness swagger:route GET /.well-known/ready weaviateWellknownReadiness

Determines whether the application is ready to receive traffic. Can be used for kubernetes readiness probe.

*/
type WeaviateWellknownReadiness struct {
	Context *middleware.Context
	Handler WeaviateWellknownReadinessHandler
}

func (o *WeaviateWellknownReadiness) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateWellknownReadinessParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
