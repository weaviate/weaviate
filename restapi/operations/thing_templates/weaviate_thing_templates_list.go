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

// WeaviateThingTemplatesListHandlerFunc turns a function with the right signature into a weaviate thing templates list handler
type WeaviateThingTemplatesListHandlerFunc func(WeaviateThingTemplatesListParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesListHandlerFunc) Handle(params WeaviateThingTemplatesListParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesListHandler interface for that can handle valid weaviate thing templates list params
type WeaviateThingTemplatesListHandler interface {
	Handle(WeaviateThingTemplatesListParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesList creates a new http.Handler for the weaviate thing templates list operation
func NewWeaviateThingTemplatesList(ctx *middleware.Context, handler WeaviateThingTemplatesListHandler) *WeaviateThingTemplatesList {
	return &WeaviateThingTemplatesList{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesList swagger:route GET /thingTemplates thingTemplates weaviateThingTemplatesList

Get a list of thing template related to this key.

Lists all model manifests.

*/
type WeaviateThingTemplatesList struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesListHandler
}

func (o *WeaviateThingTemplatesList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateThingTemplatesListParams()

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
