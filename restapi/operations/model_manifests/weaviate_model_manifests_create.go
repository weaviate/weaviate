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
 package model_manifests


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateModelManifestsCreateHandlerFunc turns a function with the right signature into a weaviate model manifests create handler
type WeaviateModelManifestsCreateHandlerFunc func(WeaviateModelManifestsCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsCreateHandlerFunc) Handle(params WeaviateModelManifestsCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviateModelManifestsCreateHandler interface for that can handle valid weaviate model manifests create params
type WeaviateModelManifestsCreateHandler interface {
	Handle(WeaviateModelManifestsCreateParams, interface{}) middleware.Responder
}

// NewWeaviateModelManifestsCreate creates a new http.Handler for the weaviate model manifests create operation
func NewWeaviateModelManifestsCreate(ctx *middleware.Context, handler WeaviateModelManifestsCreateHandler) *WeaviateModelManifestsCreate {
	return &WeaviateModelManifestsCreate{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsCreate swagger:route POST /modelManifests modelManifests weaviateModelManifestsCreate

Inserts a model manifest.

*/
type WeaviateModelManifestsCreate struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsCreateHandler
}

func (o *WeaviateModelManifestsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsCreateParams()

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
