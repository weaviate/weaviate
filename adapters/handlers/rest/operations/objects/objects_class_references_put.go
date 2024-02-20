// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// ObjectsClassReferencesPutHandlerFunc turns a function with the right signature into a objects class references put handler
type ObjectsClassReferencesPutHandlerFunc func(ObjectsClassReferencesPutParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn ObjectsClassReferencesPutHandlerFunc) Handle(params ObjectsClassReferencesPutParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// ObjectsClassReferencesPutHandler interface for that can handle valid objects class references put params
type ObjectsClassReferencesPutHandler interface {
	Handle(ObjectsClassReferencesPutParams, *models.Principal) middleware.Responder
}

// NewObjectsClassReferencesPut creates a new http.Handler for the objects class references put operation
func NewObjectsClassReferencesPut(ctx *middleware.Context, handler ObjectsClassReferencesPutHandler) *ObjectsClassReferencesPut {
	return &ObjectsClassReferencesPut{Context: ctx, Handler: handler}
}

/*
	ObjectsClassReferencesPut swagger:route PUT /objects/{className}/{id}/references/{propertyName} objects objectsClassReferencesPut

Replace all references to a class-property.

Update all references of a property of a data object.
*/
type ObjectsClassReferencesPut struct {
	Context *middleware.Context
	Handler ObjectsClassReferencesPutHandler
}

func (o *ObjectsClassReferencesPut) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewObjectsClassReferencesPutParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
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
