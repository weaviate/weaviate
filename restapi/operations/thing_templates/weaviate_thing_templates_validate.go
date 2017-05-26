/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package thing_templates


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateThingTemplatesValidateHandlerFunc turns a function with the right signature into a weaviate thing templates validate handler
type WeaviateThingTemplatesValidateHandlerFunc func(WeaviateThingTemplatesValidateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesValidateHandlerFunc) Handle(params WeaviateThingTemplatesValidateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesValidateHandler interface for that can handle valid weaviate thing templates validate params
type WeaviateThingTemplatesValidateHandler interface {
	Handle(WeaviateThingTemplatesValidateParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesValidate creates a new http.Handler for the weaviate thing templates validate operation
func NewWeaviateThingTemplatesValidate(ctx *middleware.Context, handler WeaviateThingTemplatesValidateHandler) *WeaviateThingTemplatesValidate {
	return &WeaviateThingTemplatesValidate{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesValidate swagger:route POST /thingTemplates/validate thingTemplates weaviateThingTemplatesValidate

Validate if a thing template is correct.

Validates a template, will respond if it is a valid thing template.

*/
type WeaviateThingTemplatesValidate struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesValidateHandler
}

func (o *WeaviateThingTemplatesValidate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingTemplatesValidateParams()

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
