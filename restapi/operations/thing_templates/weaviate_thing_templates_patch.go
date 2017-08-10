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

// WeaviateThingTemplatesPatchHandlerFunc turns a function with the right signature into a weaviate thing templates patch handler
type WeaviateThingTemplatesPatchHandlerFunc func(WeaviateThingTemplatesPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesPatchHandlerFunc) Handle(params WeaviateThingTemplatesPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesPatchHandler interface for that can handle valid weaviate thing templates patch params
type WeaviateThingTemplatesPatchHandler interface {
	Handle(WeaviateThingTemplatesPatchParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesPatch creates a new http.Handler for the weaviate thing templates patch operation
func NewWeaviateThingTemplatesPatch(ctx *middleware.Context, handler WeaviateThingTemplatesPatchHandler) *WeaviateThingTemplatesPatch {
	return &WeaviateThingTemplatesPatch{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesPatch swagger:route PATCH /thingTemplates/{thingTemplateId} thingTemplates weaviateThingTemplatesPatch

Update a thing template based on its uuid (using patch sematics) related to this key.

Updates a particular thing template.

*/
type WeaviateThingTemplatesPatch struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesPatchHandler
}

func (o *WeaviateThingTemplatesPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingTemplatesPatchParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
