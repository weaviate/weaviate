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

// WeaviateThingTemplatesCreateHandlerFunc turns a function with the right signature into a weaviate thing templates create handler
type WeaviateThingTemplatesCreateHandlerFunc func(WeaviateThingTemplatesCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateThingTemplatesCreateHandlerFunc) Handle(params WeaviateThingTemplatesCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateThingTemplatesCreateHandler interface for that can handle valid weaviate thing templates create params
type WeaviateThingTemplatesCreateHandler interface {
	Handle(WeaviateThingTemplatesCreateParams, interface{}) middleware.Responder
}

// NewWeaviateThingTemplatesCreate creates a new http.Handler for the weaviate thing templates create operation
func NewWeaviateThingTemplatesCreate(ctx *middleware.Context, handler WeaviateThingTemplatesCreateHandler) *WeaviateThingTemplatesCreate {
	return &WeaviateThingTemplatesCreate{Context: ctx, Handler: handler}
}

/*WeaviateThingTemplatesCreate swagger:route POST /thingTemplates thingTemplates weaviateThingTemplatesCreate

Create a new thing template related to this key.

Creates a new thing template.

*/
type WeaviateThingTemplatesCreate struct {
	Context *middleware.Context
	Handler WeaviateThingTemplatesCreateHandler
}

func (o *WeaviateThingTemplatesCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateThingTemplatesCreateParams()

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
