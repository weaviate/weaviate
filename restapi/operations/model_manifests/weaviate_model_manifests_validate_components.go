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

// WeaviateModelManifestsValidateComponentsHandlerFunc turns a function with the right signature into a weaviate model manifests validate components handler
type WeaviateModelManifestsValidateComponentsHandlerFunc func(WeaviateModelManifestsValidateComponentsParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsValidateComponentsHandlerFunc) Handle(params WeaviateModelManifestsValidateComponentsParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsValidateComponentsHandler interface for that can handle valid weaviate model manifests validate components params
type WeaviateModelManifestsValidateComponentsHandler interface {
	Handle(WeaviateModelManifestsValidateComponentsParams) middleware.Responder
}

// NewWeaviateModelManifestsValidateComponents creates a new http.Handler for the weaviate model manifests validate components operation
func NewWeaviateModelManifestsValidateComponents(ctx *middleware.Context, handler WeaviateModelManifestsValidateComponentsHandler) *WeaviateModelManifestsValidateComponents {
	return &WeaviateModelManifestsValidateComponents{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsValidateComponents swagger:route POST /modelManifests/validateComponents modelManifests weaviateModelManifestsValidateComponents

Validates given components definitions and returns errors.

*/
type WeaviateModelManifestsValidateComponents struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsValidateComponentsHandler
}

func (o *WeaviateModelManifestsValidateComponents) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsValidateComponentsParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
