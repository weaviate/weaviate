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

// WeaviateThingTemplatesUpdateHandlerFunc turns a function with the right signature into a weaviate thing templates update handler
type WeaviateThingTemplatesUpdateHandlerFunc func(WeaviateThingTemplatesUpdateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesUpdateHandlerFunc) Handle(params WeaviateThingTemplatesUpdateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesUpdateHandler interface for that can handle valid weaviate thing templates update params
type WeaviateThingTemplatesUpdateHandler interface {
	Handle(WeaviateThingTemplatesUpdateParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesUpdate creates a new http.Handler for the weaviate thing templates update operation
func NewWeaviateThingTemplatesUpdate(ctx *middleware.Context, handler WeaviateThingTemplatesUpdateHandler) *WeaviateThingTemplatesUpdate {
	return &WeaviateThingTemplatesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesUpdate swagger:route PUT /thingTemplates/{thingTemplateId} thingTemplates weaviateThingTemplatesUpdate

Update thing template based on its uuid related to this key.

Updates a particular thing template.

*/
type WeaviateThingTemplatesUpdate struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesUpdateHandler
}

func (o *WeaviateThingTemplatesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingTemplatesUpdateParams()

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
