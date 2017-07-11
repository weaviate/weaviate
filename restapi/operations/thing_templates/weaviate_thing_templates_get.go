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

// WeaviateThingTemplatesGetHandlerFunc turns a function with the right signature into a weaviate thing templates get handler
type WeaviateThingTemplatesGetHandlerFunc func(WeaviateThingTemplatesGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesGetHandlerFunc) Handle(params WeaviateThingTemplatesGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesGetHandler interface for that can handle valid weaviate thing templates get params
type WeaviateThingTemplatesGetHandler interface {
	Handle(WeaviateThingTemplatesGetParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesGet creates a new http.Handler for the weaviate thing templates get operation
func NewWeaviateThingTemplatesGet(ctx *middleware.Context, handler WeaviateThingTemplatesGetHandler) *WeaviateThingTemplatesGet {
	return &WeaviateThingTemplatesGet{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesGet swagger:route GET /thingTemplates/{thingTemplateId} thingTemplates weaviateThingTemplatesGet

Get a thing template based on its uuid related to this key.

Returns a particular thing template.

*/
type WeaviateThingTemplatesGet struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesGetHandler
}

func (o *WeaviateThingTemplatesGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingTemplatesGetParams()

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
