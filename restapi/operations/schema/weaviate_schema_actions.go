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
   

package schema

 
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateSchemaActionsHandlerFunc turns a function with the right signature into a weaviate schema actions handler
type WeaviateSchemaActionsHandlerFunc func(WeaviateSchemaActionsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateSchemaActionsHandlerFunc) Handle(params WeaviateSchemaActionsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateSchemaActionsHandler interface for that can handle valid weaviate schema actions params
type WeaviateSchemaActionsHandler interface {
	Handle(WeaviateSchemaActionsParams, interface{}) middleware.Responder
}

// NewWeaviateSchemaActions creates a new http.Handler for the weaviate schema actions operation
func NewWeaviateSchemaActions(ctx *middleware.Context, handler WeaviateSchemaActionsHandler) *WeaviateSchemaActions {
	return &WeaviateSchemaActions{Context: ctx, Handler: handler}
}

/*WeaviateSchemaActions swagger:route GET /schema/actions schema weaviateSchemaActions

Download the schema file where all actions are based on.

Download the schema where all actions are based on.

*/
type WeaviateSchemaActions struct {
	Context *middleware.Context
	Handler WeaviateSchemaActionsHandler
}

func (o *WeaviateSchemaActions) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateSchemaActionsParams()

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
