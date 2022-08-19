// Code generated by go-swagger; DO NOT EDIT.

package classifications

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ClassificationsPostHandlerFunc turns a function with the right signature into a classifications post handler
type ClassificationsPostHandlerFunc func(ClassificationsPostParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ClassificationsPostHandlerFunc) Handle(params ClassificationsPostParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ClassificationsPostHandler interface for that can handle valid classifications post params
type ClassificationsPostHandler interface {
	Handle(ClassificationsPostParams, *models.Principal) middleware.Responder
}

// NewClassificationsPost creates a new http.Handler for the classifications post operation
func NewClassificationsPost(ctx *middleware.Context, handler ClassificationsPostHandler) *ClassificationsPost {
	return &ClassificationsPost{Context: ctx, Handler: handler}
}

/*ClassificationsPost swagger:route POST /classifications/ classifications classificationsPost

Starts a classification.

Trigger a classification based on the specified params. Classifications will run in the background, use GET /classifications/<id> to retrieve the status of your classification.

*/
type ClassificationsPost struct {
	Context *middleware.Context
	Handler ClassificationsPostHandler
}

func (o *ClassificationsPost) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewClassificationsPostParams()

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
