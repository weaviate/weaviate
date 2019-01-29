/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	context "golang.org/x/net/context"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateSchemaActionsPropertiesDeleteHandlerFunc turns a function with the right signature into a weaviate schema actions properties delete handler
type WeaviateSchemaActionsPropertiesDeleteHandlerFunc func(context.Context, WeaviateSchemaActionsPropertiesDeleteParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateSchemaActionsPropertiesDeleteHandlerFunc) Handle(ctx context.Context, params WeaviateSchemaActionsPropertiesDeleteParams) middleware.Responder {
	return fn(ctx, params)
}

// WeaviateSchemaActionsPropertiesDeleteHandler interface for that can handle valid weaviate schema actions properties delete params
type WeaviateSchemaActionsPropertiesDeleteHandler interface {
	Handle(context.Context, WeaviateSchemaActionsPropertiesDeleteParams) middleware.Responder
}

// NewWeaviateSchemaActionsPropertiesDelete creates a new http.Handler for the weaviate schema actions properties delete operation
func NewWeaviateSchemaActionsPropertiesDelete(ctx *middleware.Context, handler WeaviateSchemaActionsPropertiesDeleteHandler) *WeaviateSchemaActionsPropertiesDelete {
	return &WeaviateSchemaActionsPropertiesDelete{Context: ctx, Handler: handler}
}

/*WeaviateSchemaActionsPropertiesDelete swagger:route DELETE /schema/actions/{className}/properties/{propertyName} schema weaviateSchemaActionsPropertiesDelete

Remove a property from an Action class.

*/
type WeaviateSchemaActionsPropertiesDelete struct {
	Context *middleware.Context
	Handler WeaviateSchemaActionsPropertiesDeleteHandler
}

func (o *WeaviateSchemaActionsPropertiesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateSchemaActionsPropertiesDeleteParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(r.Context(), Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
